"""
cancel_in_progress_jobs.py
==========================

Find every Fabric job (notebook, pipeline, Spark job definition, dataflow,
experiment, etc.) that is currently *In progress* (or *Not started*) and
cancel it. Mirrors the "In progress" filter visible in the Power BI /
Fabric Monitor hub.

Scope
-----
By default, scans *every workspace the signed-in user can see* (the same
list shown by `fab ls` and by the Monitor hub).  Optional flags let you:
    * limit to one or more named workspaces (`-w`)
    * use the tenant-wide Admin APIs to see workspaces you do not have
      explicit access to (`--admin`, requires Fabric Admin role).

Authentication uses the Azure SDK's `DefaultAzureCredential`, which picks
up your Azure CLI (`az login`) or interactive browser identity automatically.
If neither is available, the script falls back to shelling out to
`az account get-access-token --resource https://api.fabric.microsoft.com`.

Usage
-----
    # Cancel everything in-progress in a workspace
    python cancel_in_progress_jobs.py -w "crestshield-smartclaims-sachinsaraf"

    # Limit to specific items (by name OR GUID, repeatable)
    python cancel_in_progress_jobs.py -w ws -i "01_Bronze_Ingestion" -i "OfferProducerCatchup"

    # Limit to specific item types (repeatable)
    python cancel_in_progress_jobs.py -w ws --item-type Notebook --item-type DataPipeline

    # Exclude items by name/GUID (repeatable)
    python cancel_in_progress_jobs.py -w ws --exclude-item "Critical_Production_Pipeline"

    # Interactively pick which jobs to cancel
    python cancel_in_progress_jobs.py -w ws --interactive

    # Preview, poll, multi-workspace, tenant-wide
    python cancel_in_progress_jobs.py -w ws --dry-run
    python cancel_in_progress_jobs.py -w ws --poll 60
    python cancel_in_progress_jobs.py -w ws1 -w ws2
    python cancel_in_progress_jobs.py --admin

Flags
-----
    -w / --workspace     Workspace display name or GUID. Repeatable.
                         REQUIRED unless --admin.
    -i / --item          Restrict to items with this display name OR GUID.
                         Repeatable. Wildcards (*, ?) supported.
    --item-type          Restrict to these item types (Notebook, DataPipeline,
                         MLExperiment, Dataflow, SparkJobDefinition, ...).
                         Repeatable.
    --exclude-item       Skip items with this name or GUID. Repeatable.
                         Wildcards supported.
    --interactive        Show found jobs and ask which ones to cancel.
    --admin              Tenant-wide via Admin APIs (requires Fabric Admin).
    --dry-run            List matching jobs but do not cancel.
    --only-in-progress   Cancel only InProgress (skip NotStarted).
    --poll N             Poll up to N seconds for final status after cancel.
    --concurrency K      Parallel cancel/list requests (default: 16).
    --verbose            Print noisy per-item warnings.
    --json               Machine-readable summary.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import random
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Iterable

import requests

# Tunables -- adjustable via CLI flags --sleep-interval and --max-retries.
CONFIG = {
    "sleep_interval": 0.0,   # seconds slept after every request (gentle pacing)
    "max_retries":    6,     # how many times to retry on 429/5xx
    "backoff_base":   2.0,   # exponential backoff base (seconds)
    "backoff_max":    60.0,  # cap on backoff sleep
}

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
PBI_ADMIN_BASE = "https://api.powerbi.com/v1.0/myorg/admin"

# Power BI Activity Events that correspond to active Fabric jobs/sessions.
# Adapted from hfleitas/fabriciq commit 22ef278.
ACTIVITY_TYPES_NOTEBOOK = {
    "UpdateNotebook", "ExecuteNotebookJob", "StartNotebookSession", "RunNotebook",
}
ACTIVITY_TYPES_PIPELINE = {
    "ExecutePipeline", "RunDataPipeline", "StartPipeline", "DataPipelineRun",
}

# Status values that count as "active" / "in progress" in the Monitor hub.
# Normalised to lowercase for matching (case-insensitive).
ACTIVE_STATUSES = {"inprogress", "notstarted", "starting", "running", "unknown"}
TERMINAL_STATUSES = {"completed", "failed", "cancelled", "canceled", "deduped"}

# Item types known to never expose /jobs/instances -- skip them silently.
NO_JOBS_ITEM_TYPES = {
    "SQLEndpoint", "SQLAnalyticsEndpoint", "MountedWarehouse",
    "Dashboard", "PaginatedReport",
}


def normalize_status(value: str | None) -> str:
    if value is None:
        return "unknown"
    return str(value).replace(" ", "").replace("_", "").lower()


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_fabric_token() -> str:
    """Bearer token for Fabric API audience."""
    return _get_token("https://api.fabric.microsoft.com/.default",
                      cli_resource="https://api.fabric.microsoft.com")


def get_powerbi_token() -> str:
    """Bearer token for Power BI / analysis.windows.net audience (admin APIs)."""
    return _get_token("https://analysis.windows.net/powerbi/api/.default",
                      cli_resource="https://analysis.windows.net/powerbi/api")


def _get_token(scope: str, *, cli_resource: str) -> str:
    """Return a bearer token. Tries multiple sources in order:

    1. Fabric notebook (`notebookutils.mssparkutils.credentials.getToken`) -
       works automatically inside Microsoft Fabric notebooks.
    2. `azure.identity.DefaultAzureCredential` - picks up `az login`,
       VS Code, managed identity, env vars, etc.
    3. Shell out to `az account get-access-token`.
    """
    # 1) Fabric notebook identity (no extra setup needed inside Fabric)
    try:
        from notebookutils import mssparkutils  # type: ignore
        return mssparkutils.credentials.getToken(cli_resource)
    except Exception:  # noqa: BLE001 - missing module, not running in Fabric notebook
        pass

    # 2) azure-identity
    try:
        from azure.identity import DefaultAzureCredential  # type: ignore
        cred = DefaultAzureCredential(exclude_interactive_browser_credential=False)
        return cred.get_token(scope).token
    except Exception as exc:  # noqa: BLE001
        print(f"  (azure-identity unavailable: {exc}; falling back to 'az' CLI)")

    # 3) az CLI
    try:
        result = subprocess.run(
            ["az", "account", "get-access-token",
             "--resource", cli_resource,
             "--query", "accessToken", "-o", "tsv"],
            capture_output=True, text=True, check=True, shell=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        print("ERROR: could not obtain access token. Run 'az login' or install azure-identity.")
        if isinstance(exc, subprocess.CalledProcessError):
            print(exc.stderr or exc.stdout)
        sys.exit(2)

    token = (result.stdout or "").strip()
    if not token or "." not in token:
        print("ERROR: empty / unexpected token from 'az'.")
        sys.exit(2)
    return token


def auth_header(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


# ---------------------------------------------------------------------------
# HTTP with throttling-aware retry (Retry-After + exponential backoff + jitter)
# ---------------------------------------------------------------------------

def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    json_body: dict | None = None,
    timeout: int = 60,
) -> requests.Response:
    """Send an HTTP request, retrying on 429 and 5xx with backoff.

    - Honors the `Retry-After` header on 429 responses (seconds or HTTP-date).
    - Falls back to exponential backoff with jitter for 5xx/connection errors.
    - Sleeps `CONFIG['sleep_interval']` after every successful call to pace.
    """
    last_exc: Exception | None = None
    for attempt in range(1, CONFIG["max_retries"] + 1):
        try:
            resp = session.request(method, url, json=json_body, timeout=timeout)
        except requests.RequestException as exc:
            last_exc = exc
            wait = min(
                CONFIG["backoff_max"],
                CONFIG["backoff_base"] ** attempt + random.uniform(0, 1),
            )
            print(f"  ! network error ({exc.__class__.__name__}), retrying in {wait:.1f}s "
                  f"[{attempt}/{CONFIG['max_retries']}]")
            time.sleep(wait)
            continue

        # 429 -- Throttled. Wait the server-instructed amount then retry.
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "")
            wait = _parse_retry_after(retry_after) or min(
                CONFIG["backoff_max"],
                CONFIG["backoff_base"] ** attempt + random.uniform(0, 1),
            )
            print(f"  ! throttled (429), waiting {wait:.1f}s "
                  f"[{attempt}/{CONFIG['max_retries']}]  url={url}")
            time.sleep(wait)
            continue

        # 5xx -- transient, retry with backoff.
        if 500 <= resp.status_code < 600:
            wait = min(
                CONFIG["backoff_max"],
                CONFIG["backoff_base"] ** attempt + random.uniform(0, 1),
            )
            print(f"  ! server {resp.status_code}, retrying in {wait:.1f}s "
                  f"[{attempt}/{CONFIG['max_retries']}]")
            time.sleep(wait)
            continue

        # Gentle pacing after every non-retried response.
        if CONFIG["sleep_interval"] > 0:
            time.sleep(CONFIG["sleep_interval"])

        return resp

    # Exhausted retries.
    if last_exc is not None:
        raise last_exc
    return resp  # type: ignore[return-value]


def _parse_retry_after(value: str) -> float | None:
    """Retry-After may be seconds (int) or an HTTP-date. Return seconds or None."""
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(value)
        if dt is None:
            return None
        return max(0.0, (dt.timestamp() - time.time()))
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Fabric REST helpers
# ---------------------------------------------------------------------------

@dataclass
class ActiveJob:
    workspace_id: str
    workspace_name: str
    item_id: str
    item_name: str
    item_type: str
    instance_id: str
    job_type: str
    status: str
    start_time: str
    invoke_type: str


def get_paged(session: requests.Session, url: str) -> list[dict]:
    """GET with continuationToken/continuationUri pagination and 429 retry."""
    out: list[dict] = []
    next_url: str | None = url
    while next_url:
        r = request_with_retry(session, "GET", next_url)
        r.raise_for_status()
        body = r.json()
        out.extend(body.get("value", []))
        cont_uri = body.get("continuationUri")
        cont_token = body.get("continuationToken")
        if cont_uri:
            next_url = cont_uri
        elif cont_token:
            sep = "&" if "?" in url else "?"
            next_url = f"{url}{sep}continuationToken={cont_token}"
        else:
            next_url = None
    return out


def resolve_workspace_id(session: requests.Session, workspace: str) -> tuple[str, str]:
    """Return (workspaceId, displayName). Accepts a name or a GUID."""
    if len(workspace) == 36 and workspace.count("-") == 4:
        r = request_with_retry(session, "GET", f"{FABRIC_BASE}/workspaces/{workspace}")
        r.raise_for_status()
        body = r.json()
        return body["id"], body.get("displayName", workspace)

    workspaces = get_paged(session, f"{FABRIC_BASE}/workspaces")
    for w in workspaces:
        if w.get("displayName", "").lower() == workspace.lower():
            return w["id"], w["displayName"]
    raise SystemExit(f"ERROR: workspace '{workspace}' not found.")


def list_all_workspaces(session: requests.Session, admin: bool) -> list[dict]:
    """Return [{id, displayName}, ...] for every workspace in scope."""
    if admin:
        rows = get_paged(session, f"{FABRIC_BASE}/admin/workspaces")
        return [
            {"id": w["id"], "displayName": w.get("name") or w.get("displayName") or w["id"]}
            for w in rows
            if w.get("type", "Workspace") not in ("PersonalGroup", "AdminInsights")
        ]
    rows = get_paged(session, f"{FABRIC_BASE}/workspaces")
    return [{"id": w["id"], "displayName": w.get("displayName", w["id"])} for w in rows]


def list_items(session: requests.Session, ws_id: str) -> list[dict]:
    return get_paged(session, f"{FABRIC_BASE}/workspaces/{ws_id}/items")


def list_job_instances(
    session: requests.Session,
    ws_id: str,
    item_id: str,
    *,
    active_only: bool = False,
    active_statuses: set[str] | None = None,
) -> list[dict]:
    """List job instances for an item.

    `/jobs/instances` returns runs newest-first. When `active_only=True` we
    page through results but stop as soon as we see a page containing zero
    active statuses -- old terminal runs aren't worth fetching.
    """
    url = f"{FABRIC_BASE}/workspaces/{ws_id}/items/{item_id}/jobs/instances"
    if not active_only:
        return get_paged(session, url)

    out: list[dict] = []
    next_url: str | None = url
    while next_url:
        r = request_with_retry(session, "GET", next_url)
        r.raise_for_status()
        body = r.json()
        page = body.get("value", [])
        out.extend(page)
        page_has_active = any(
            normalize_status(inst.get("status")) in (active_statuses or ACTIVE_STATUSES)
            for inst in page
        )
        if not page_has_active:
            break
        cont_uri = body.get("continuationUri")
        cont_token = body.get("continuationToken")
        if cont_uri:
            next_url = cont_uri
        elif cont_token:
            sep = "&" if "?" in url else "?"
            next_url = f"{url}{sep}continuationToken={cont_token}"
        else:
            next_url = None
    return out


def cancel_job_instance(
    session: requests.Session, ws_id: str, item_id: str, instance_id: str
) -> tuple[int, str]:
    """Cancel a job instance. Tries `/jobs/instances/.../cancel` first,
    falls back to `/jobInstances/.../cancel` for older items.
    (Dual-URL pattern from hfleitas/fabriciq.)
    """
    urls = [
        f"{FABRIC_BASE}/workspaces/{ws_id}/items/{item_id}/jobs/instances/{instance_id}/cancel",
        f"{FABRIC_BASE}/workspaces/{ws_id}/items/{item_id}/jobInstances/{instance_id}/cancel",
    ]
    last_resp: requests.Response | None = None
    for url in urls:
        r = request_with_retry(session, "POST", url)
        if 200 <= r.status_code < 300:
            return r.status_code, (r.text or "").strip()
        last_resp = r
    return last_resp.status_code, (last_resp.text or "").strip()  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Power BI Admin Activity Events -- single tenant-wide call to discover
# running notebooks/pipelines without enumerating every item per workspace.
# Pattern from hfleitas/fabriciq commit 22ef278 ("fixes for activityevents api").
# Requires Fabric Administrator role.
# ---------------------------------------------------------------------------

def list_activity_events(
    pbi_session: requests.Session,
    start_time_utc: str,
    end_time_utc: str,
    activity_filter: str | None = None,
) -> list[dict]:
    """Page through /admin/activityevents for a given window.

    start_time_utc / end_time_utc must be ISO-8601 in single quotes per the
    Power BI API spec; window must be <= 24 hours.
    """
    quoted_start = f"'{start_time_utc}'"
    quoted_end = f"'{end_time_utc}'"
    base = (f"{PBI_ADMIN_BASE}/activityevents"
            f"?startDateTime={quoted_start}&endDateTime={quoted_end}")
    if activity_filter:
        base += f"&$filter={activity_filter}"

    events: list[dict] = []
    next_url: str | None = base
    while next_url:
        r = request_with_retry(pbi_session, "GET", next_url)
        if r.status_code == 401:
            raise SystemExit(
                "ERROR: /admin/activityevents returned 401. "
                "This API requires the Fabric Administrator role."
            )
        r.raise_for_status()
        body = r.json()
        events.extend(body.get("activityEventEntities", body.get("value", [])))
        next_url = body.get("continuationUri")
    return events


def activity_to_active_job(
    activity: dict, ws_lookup: dict[str, str]
) -> ActiveJob | None:
    """Map a Power BI activity event into our ActiveJob shape, if it represents
    an active notebook session, notebook job, or pipeline run."""
    status_norm = normalize_status(activity.get("Status"))
    if status_norm not in ACTIVE_STATUSES:
        return None

    a_type = activity.get("Activity") or activity.get("OperationName") or ""
    if a_type in ACTIVITY_TYPES_NOTEBOOK:
        item_kind = "Notebook"
    elif a_type in ACTIVITY_TYPES_PIPELINE:
        item_kind = "DataPipeline"
    else:
        return None

    ws_id = activity.get("WorkspaceId") or activity.get("WorkSpaceId") or ""
    return ActiveJob(
        workspace_id=ws_id,
        workspace_name=ws_lookup.get(ws_id, ws_id),
        item_id=activity.get("ItemId") or activity.get("ObjectId") or "",
        item_name=activity.get("ItemName") or activity.get("ObjectId") or "",
        item_type=item_kind,
        instance_id=activity.get("Id") or "",
        job_type=a_type,
        status=activity.get("Status", "InProgress"),
        start_time=activity.get("CreationTime") or activity.get("EventTime", ""),
        invoke_type="Activity",
    )


def discover_via_activity_events(
    pbi_session: requests.Session,
    fabric_session: requests.Session,
    lookback_minutes: int,
) -> list[ActiveJob]:
    """Tenant-wide active-job discovery via Power BI Activity Events.

    Returns ActiveJob entries for notebook + pipeline activities currently
    in an active state within the last `lookback_minutes`.
    """
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = (now - timedelta(minutes=lookback_minutes)).isoformat().replace("+00:00", "Z")
    end = now.isoformat().replace("+00:00", "Z")

    print(f"  Activity Events window: {start} -> {end}")
    events = list_activity_events(pbi_session, start, end)
    print(f"  Activity events returned: {len(events)}")

    # Build a workspace-id → display-name lookup (for prettier output).
    ws_lookup: dict[str, str] = {}
    try:
        groups = get_paged(pbi_session, f"{PBI_ADMIN_BASE}/groups?%24top=5000")
        ws_lookup = {
            g["id"]: (g.get("name") or g.get("displayName") or g["id"])
            for g in groups if g.get("id")
        }
    except requests.HTTPError:
        pass

    out: list[ActiveJob] = []
    seen_keys: set[tuple[str, str]] = set()
    for ev in events:
        job = activity_to_active_job(ev, ws_lookup)
        if job is None:
            continue
        key = (job.item_id, job.instance_id)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        out.append(job)
    return out


def get_job_instance(
    session: requests.Session, ws_id: str, item_id: str, instance_id: str
) -> dict:
    r = request_with_retry(
        session,
        "GET",
        f"{FABRIC_BASE}/workspaces/{ws_id}/items/{item_id}/jobs/instances/{instance_id}",
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Notebook Spark sessions (separate from job instances). A notebook job can
# leave a live Spark session that the Monitor hub also surfaces as active.
# Pattern adopted from hfleitas/fabriciq/Stop-All-Running-Fabric-Workloads.
# ---------------------------------------------------------------------------

def list_notebook_sessions(
    session: requests.Session, ws_id: str, notebook_id: str
) -> list[dict]:
    url = f"{FABRIC_BASE}/workspaces/{ws_id}/notebooks/{notebook_id}/sessions"
    try:
        r = request_with_retry(session, "GET", url)
        if r.status_code >= 400:
            return []
        return r.json().get("value", [])
    except requests.HTTPError:
        return []


def stop_notebook_session(
    session: requests.Session, ws_id: str, notebook_id: str, session_id: str
) -> tuple[int, str]:
    url = (
        f"{FABRIC_BASE}/workspaces/{ws_id}/notebooks/{notebook_id}"
        f"/sessions/{session_id}/stop"
    )
    r = request_with_retry(session, "POST", url)
    return r.status_code, (r.text or "").strip()


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def collect_active_jobs(
    session: requests.Session,
    ws_id: str,
    ws_name: str,
    items: list[dict],
    active_statuses: set[str],
    concurrency: int,
    verbose: bool,
) -> list[ActiveJob]:
    """Hit each item's jobs/instances endpoint in parallel and collect matches."""
    active: list[ActiveJob] = []

    def fetch(item: dict) -> list[ActiveJob]:
        if item.get("type") in NO_JOBS_ITEM_TYPES:
            return []
        try:
            instances = list_job_instances(
                session, ws_id, item["id"],
                active_only=True, active_statuses=active_statuses,
            )
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else 0
            if code in (400, 403, 404):
                if verbose:
                    print(f"  ! [{ws_name}] {item.get('displayName')} "
                          f"({item.get('type')}): HTTP {code}")
                return []
            print(
                f"  ! [{ws_name}] could not list jobs for {item.get('displayName')} "
                f"({item.get('type')}): {exc}"
            )
            return []
        rows: list[ActiveJob] = []
        for inst in instances:
            if normalize_status(inst.get("status")) in active_statuses:
                rows.append(
                    ActiveJob(
                        workspace_id=ws_id,
                        workspace_name=ws_name,
                        item_id=item["id"],
                        item_name=item.get("displayName", ""),
                        item_type=item.get("type", ""),
                        instance_id=inst["id"],
                        job_type=inst.get("jobType", ""),
                        status=inst.get("status", ""),
                        start_time=inst.get("startTimeUtc", ""),
                        invoke_type=inst.get("invokeType", ""),
                    )
                )
        return rows

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(fetch, it): it for it in items}
        for f in as_completed(futures):
            active.extend(f.result())
    return active


def cancel_all(
    session: requests.Session,
    jobs: Iterable[ActiveJob],
    concurrency: int,
) -> list[tuple[ActiveJob, int, str]]:
    results: list[tuple[ActiveJob, int, str]] = []

    def go(job: ActiveJob) -> tuple[ActiveJob, int, str]:
        code, body = cancel_job_instance(session, job.workspace_id, job.item_id, job.instance_id)
        return job, code, body

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(go, j) for j in jobs]
        for f in as_completed(futures):
            results.append(f.result())
    return results


def poll_until_terminal(
    session: requests.Session,
    jobs: list[ActiveJob],
    seconds: int,
) -> dict[str, str]:
    deadline = time.time() + seconds
    final: dict[str, str] = {}
    pending = {j.instance_id: j for j in jobs}
    while pending and time.time() < deadline:
        time.sleep(min(5, max(1, seconds // 6 or 1)))
        for inst_id in list(pending):
            job = pending[inst_id]
            try:
                body = get_job_instance(session, job.workspace_id, job.item_id, inst_id)
            except requests.HTTPError as exc:
                final[inst_id] = f"error:{exc}"
                pending.pop(inst_id, None)
                continue
            status = body.get("status", "Unknown")
            if normalize_status(status) in TERMINAL_STATUSES:
                final[inst_id] = status
                pending.pop(inst_id, None)
    for inst_id, job in pending.items():
        try:
            body = get_job_instance(session, job.workspace_id, job.item_id, inst_id)
            final[inst_id] = body.get("status", "Unknown") + " (still pending)"
        except requests.HTTPError as exc:
            final[inst_id] = f"error:{exc}"
    return final


# ---------------------------------------------------------------------------
# Stop notebook Spark sessions (live Livy/Spark sessions left by notebook runs)
# ---------------------------------------------------------------------------

def stop_active_notebook_sessions(
    session: requests.Session,
    workspaces: list[dict],
    items_by_workspace: dict[str, list[dict]],
    dry_run: bool,
    verbose: bool,
) -> tuple[int, int]:
    """Iterate notebooks in each workspace and stop any active Spark sessions.

    Returns (sessions_seen, sessions_stopped).
    """
    seen = stopped = 0
    for ws in workspaces:
        ws_id, ws_name = ws["id"], ws["displayName"]
        notebooks = [it for it in items_by_workspace.get(ws_id, [])
                     if it.get("type") == "Notebook"]
        for nb in notebooks:
            sessions = list_notebook_sessions(session, ws_id, nb["id"])
            for s in sessions:
                state = normalize_status(s.get("state"))
                seen += 1
                if state in ACTIVE_STATUSES:
                    sid = s.get("id")
                    label = f"{ws_name} / {nb.get('displayName')} (session {sid}, state={state})"
                    if dry_run:
                        print(f"  [DRY-RUN] would stop session: {label}")
                        continue
                    code, body = stop_notebook_session(session, ws_id, nb["id"], sid)
                    if 200 <= code < 300:
                        stopped += 1
                        print(f"  OK   [{code}]  stopped session: {label}")
                    else:
                        print(f"  FAIL [{code}]  session: {label} -> {body[:200]}")
                elif verbose:
                    print(f"  (skip session {s.get('id')} state={state})")
    return seen, stopped


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _item_matches(item: dict, patterns: list[str]) -> bool:
    """Case-insensitive match against item display name OR id (fnmatch wildcards)."""
    if not patterns:
        return False
    name = (item.get("displayName") or "").lower()
    iid = (item.get("id") or "").lower()
    for p in patterns:
        p_low = p.lower()
        if fnmatch.fnmatchcase(name, p_low) or fnmatch.fnmatchcase(iid, p_low):
            return True
        # Also allow plain substring match for convenience
        if p_low in name or p_low == iid:
            return True
    return False


def filter_items(
    items: list[dict],
    include_names: list[str],
    include_types: list[str],
    exclude_names: list[str],
) -> list[dict]:
    out = []
    type_set_lower = {t.lower() for t in include_types}
    for it in items:
        if include_types and (it.get("type", "").lower() not in type_set_lower):
            continue
        if include_names and not _item_matches(it, include_names):
            continue
        if exclude_names and _item_matches(it, exclude_names):
            continue
        out.append(it)
    return out


def interactive_pick(jobs: list[ActiveJob]) -> list[ActiveJob]:
    """Show a numbered list, ask user which to cancel (e.g. '1,3,5' or 'all')."""
    if not jobs:
        return []
    print("\nFound jobs:")
    for n, j in enumerate(jobs, 1):
        print(f"  [{n:>3}] {j.workspace_name} / {j.item_name}  "
              f"({j.item_type}, {j.job_type}, {j.status}, started {j.start_time[:19]})")
    print("\nEnter numbers to cancel (comma- or space-separated, ranges like 1-3 ok),")
    print("or type 'all' to cancel everything, or blank to cancel nothing:")
    try:
        raw = input("> ").strip()
    except EOFError:
        return []
    if not raw:
        return []
    if raw.lower() in ("all", "*"):
        return list(jobs)

    picked: set[int] = set()
    for token in raw.replace(",", " ").split():
        if "-" in token:
            try:
                a, b = token.split("-", 1)
                for i in range(int(a), int(b) + 1):
                    picked.add(i)
            except ValueError:
                print(f"  ! ignoring invalid range '{token}'")
        else:
            try:
                picked.add(int(token))
            except ValueError:
                print(f"  ! ignoring invalid token '{token}'")

    chosen = [jobs[i - 1] for i in sorted(picked) if 1 <= i <= len(jobs)]
    print(f"Selected {len(chosen)} of {len(jobs)} job(s).")
    return chosen


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("-w", "--workspace", action="append", default=[], required=False,
                   help="Workspace display name or GUID. Repeatable. "
                        "If omitted, scans every workspace the signed-in user "
                        "has access to (mirrors the Fabric Monitor hub view).")
    p.add_argument("-i", "--item", action="append", default=[],
                   help="Restrict to items with this display name or GUID. "
                        "Repeatable. Wildcards (*, ?) and substring match supported.")
    p.add_argument("--item-type", action="append", default=[], dest="item_types",
                   help="Restrict to these item types (Notebook, DataPipeline, "
                        "MLExperiment, Dataflow, SparkJobDefinition, ...). Repeatable.")
    p.add_argument("--exclude-item", action="append", default=[], dest="exclude_items",
                   help="Skip items with this name or GUID. Repeatable. Wildcards supported.")
    p.add_argument("--interactive", action="store_true",
                   help="Show found jobs and ask which ones to cancel.")
    p.add_argument("--max-default-workspaces", type=int, default=10,
                   dest="max_default_workspaces",
                   help="Safety cap on auto-discovered workspaces (default 10). "
                        "If the user has access to more, refuse to scan unless "
                        "--confirm-large-scan is set.")
    p.add_argument("--confirm-large-scan", action="store_true",
                   dest="confirm_large_scan",
                   help="Acknowledge that you want to scan more than "
                        "--max-default-workspaces workspaces (may hit 429s).")
    p.add_argument("--admin", action="store_true",
                   help="Enumerate workspaces tenant-wide via Admin APIs "
                        "(requires Fabric Administrator role).")
    p.add_argument("--use-activity-events", action=argparse.BooleanOptionalAction,
                   default=True,
                   help="FAST tenant-wide discovery via Power BI "
                        "/admin/activityevents (one call instead of "
                        "per-workspace scanning). DEFAULT for tenant admins. "
                        "Disable with --no-use-activity-events.")
    p.add_argument("--activity-lookback", type=int, default=60, metavar="MINUTES",
                   help="Look back this many minutes for activity events "
                        "(default: 60; max 1440 per API).")
    p.add_argument("--exclude-workspace", action="append", default=[],
                   dest="exclude_workspaces",
                   help="Skip workspaces with this display name (repeatable, "
                        "case-insensitive). E.g. --exclude-workspace Admin")
    p.add_argument("--dry-run", action="store_true",
                   help="List in-progress jobs but do not cancel them")
    p.add_argument("--include-not-started", dest="include_not_started",
                   action="store_true", default=True,
                   help="Also cancel NotStarted/Starting jobs (default: on)")
    p.add_argument("--only-in-progress", dest="include_not_started",
                   action="store_false",
                   help="Cancel ONLY InProgress/Running; skip NotStarted/Starting")
    p.add_argument("--poll", type=int, default=0,
                   help="Seconds to poll for final status after cancel (default: 0)")
    p.add_argument("--concurrency", type=int, default=16,
                   help="Parallel items-per-workspace requests (default: 16)")
    p.add_argument("--workspace-concurrency", type=int, default=8, dest="workspace_concurrency",
                   help="Parallel workspaces to scan at once (default: 8). "
                        "Raise for many workspaces; lower if you hit 429s.")
    p.add_argument("--verbose", action="store_true",
                   help="Print noisy per-item warnings (403/404 on items that "
                        "do not expose jobs/instances).")
    p.add_argument("--json", action="store_true", help="Print machine-readable JSON output")

    # ---- API throttling controls (adapted from fabriciq notebook) ----
    p.add_argument("--sleep-interval", type=float, default=0.0, metavar="SECONDS",
                   help="Sleep this long after every API call to pace requests "
                        "(default: 0 -- only sleep on 429). Use 0.05-0.25 to be gentle.")
    p.add_argument("--max-retries", type=int, default=6,
                   help="Max retries on HTTP 429 / 5xx (default: 6). Honors Retry-After.")

    # ---- Continuous monitoring loop (adapted from fabriciq notebook) ----
    p.add_argument("--loop", action="store_true",
                   help="Continuously rescan and cancel new in-progress jobs.")
    p.add_argument("--loop-duration", type=int, default=30, metavar="MINUTES",
                   help="Loop duration in minutes when --loop is set (default: 30).")
    p.add_argument("--poll-interval", type=int, default=30, metavar="SECONDS",
                   help="Seconds between loop iterations (default: 30).")

    # ---- Notebook Spark sessions ----
    p.add_argument("--stop-notebook-sessions", action="store_true",
                   help="Also stop any active notebook Spark sessions "
                        "(POST /notebooks/{id}/sessions/{sid}/stop).")
    return p.parse_args(argv)


def print_table(jobs: list[ActiveJob]) -> None:
    if not jobs:
        print("  (none)")
        return
    headers = ["Workspace", "Item Name", "Type", "Job Type", "Status", "Started (UTC)", "Instance ID"]
    rows = [
        [j.workspace_name[:30], j.item_name[:40], j.item_type, j.job_type,
         j.status, j.start_time[:19], j.instance_id]
        for j in jobs
    ]
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("  " + "  ".join("-" * w for w in widths))
    for r in rows:
        print(fmt.format(*r))


def _build_status_set(include_not_started: bool) -> set[str]:
    s = {"inprogress", "running"}
    if include_not_started:
        s.update({"notstarted", "starting", "unknown"})
    return s


def _scan_workspace(
    session: requests.Session,
    ws: dict,
    statuses: set[str],
    args: argparse.Namespace,
) -> tuple[dict, list[ActiveJob], list[dict]]:
    """Scan one workspace and return (info, active jobs, items)."""
    ws_id, ws_name = ws["id"], ws["displayName"]
    info: dict = {"ws_id": ws_id, "ws_name": ws_name, "items": 0, "filtered": 0,
                  "active": 0, "skipped": False, "error": ""}
    try:
        items = list_items(session, ws_id)
    except requests.HTTPError as exc:
        info["skipped"] = True
        info["error"] = f"HTTP {exc.response.status_code if exc.response is not None else '?'}"
        return info, [], []
    info["items"] = len(items)
    filtered = filter_items(items, args.item, args.item_types, args.exclude_items)
    info["filtered"] = len(filtered)
    active = collect_active_jobs(
        session, ws_id, ws_name, filtered, statuses, args.concurrency, args.verbose,
    )
    info["active"] = len(active)
    return info, active, items


def run_once(
    session: requests.Session,
    workspaces: list[dict],
    args: argparse.Namespace,
    pbi_session: requests.Session | None = None,
) -> dict:
    """One full scan + cancel pass. Returns a summary dict."""
    statuses = _build_status_set(args.include_not_started)
    all_active: list[ActiveJob] = []
    items_by_workspace: dict[str, list[dict]] = {}

    t0 = time.time()

    if args.use_activity_events and pbi_session is not None:
        # FAST PATH: one tenant-wide call to Activity Events.
        print(f"Discovering active jobs via /admin/activityevents "
              f"(lookback {args.activity_lookback} min)...")
        try:
            all_active = discover_via_activity_events(
                pbi_session, session, args.activity_lookback,
            )
        except SystemExit:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"  ! Activity Events discovery failed: {exc}")
            print("  Falling back to per-workspace scan...")
            args.use_activity_events = False

        if args.exclude_workspaces:
            excl = {e.lower() for e in args.exclude_workspaces}
            before = len(all_active)
            all_active = [j for j in all_active if j.workspace_name.lower() not in excl]
            print(f"  Excluded {before - len(all_active)} job(s) from excluded workspaces.")

    if not args.use_activity_events:
        # ORIGINAL PATH: per-workspace, per-item parallel scan.
        print(f"Scanning {len(workspaces)} workspace(s) with workspace-parallelism="
              f"{args.workspace_concurrency}, item-parallelism={args.concurrency}...")

        with ThreadPoolExecutor(max_workers=args.workspace_concurrency) as pool:
            futures = {pool.submit(_scan_workspace, session, ws, statuses, args): ws
                       for ws in workspaces}
            for idx, fut in enumerate(as_completed(futures), 1):
                info, active, items = fut.result()
                items_by_workspace[info["ws_id"]] = items
                if info["skipped"]:
                    print(f"  [{idx}/{len(workspaces)}] {info['ws_name']:<40}  "
                          f"skipped ({info['error']})")
                    continue
                if args.item or args.item_types or args.exclude_items:
                    scope = f"items={info['filtered']}/{info['items']}"
                else:
                    scope = f"items={info['items']}"
                marker = f"  ACTIVE: {info['active']}" if info["active"] else ""
                print(f"  [{idx}/{len(workspaces)}] {info['ws_name']:<40}  "
                      f"{scope:<14}{marker}")
                all_active.extend(active)

    elapsed = time.time() - t0
    print(f"\nScan finished in {elapsed:.1f}s. Found {len(all_active)} active job instance(s):")
    print_table(all_active)

    selected = all_active
    if args.interactive and selected:
        selected = interactive_pick(selected)
        if not selected:
            print("Nothing selected -- exiting iteration.")
            return {"found": len(all_active), "cancelled": 0, "scan_seconds": elapsed}

    cancel_succeeded: list[ActiveJob] = []
    cancel_failed: list[tuple[ActiveJob, int, str]] = []

    if selected and not args.dry_run:
        print(f"\nCancelling {len(selected)} job(s)...")
        for job, code, body in cancel_all(session, selected, args.concurrency):
            if 200 <= code < 300:
                cancel_succeeded.append(job)
                print(f"  OK   [{code}]  {job.workspace_name} / {job.item_name}  ({job.instance_id})")
            else:
                cancel_failed.append((job, code, body))
                print(f"  FAIL [{code}]  {job.workspace_name} / {job.item_name}  "
                      f"({job.instance_id}) -> {body[:200]}")
    elif selected and args.dry_run:
        print("\n--dry-run set: no cancellations issued.")

    session_seen = session_stopped = 0
    if args.stop_notebook_sessions:
        print("\nScanning notebook Spark sessions...")
        session_seen, session_stopped = stop_active_notebook_sessions(
            session, workspaces, items_by_workspace, args.dry_run, args.verbose,
        )
        print(f"  Notebook sessions seen={session_seen}, stopped={session_stopped}")

    if args.poll > 0 and cancel_succeeded:
        print(f"\nPolling for terminal status (up to {args.poll}s)...")
        final = poll_until_terminal(session, cancel_succeeded, args.poll)
        for job in cancel_succeeded:
            print(f"  {job.workspace_name} / {job.item_name}  "
                  f"{job.instance_id}  ->  {final.get(job.instance_id, 'unknown')}")

    return {
        "found": len(all_active),
        "cancel_requested": len(cancel_succeeded) + len(cancel_failed),
        "cancel_succeeded": len(cancel_succeeded),
        "cancel_failed": len(cancel_failed),
        "notebook_sessions_seen": session_seen,
        "notebook_sessions_stopped": session_stopped,
        "scan_seconds": elapsed,
    }


def cancel_in_progress_jobs(
    workspace: str | list[str] | None = None,
    *,
    item: list[str] | None = None,
    item_type: list[str] | None = None,
    exclude_item: list[str] | None = None,
    exclude_workspace: list[str] | None = None,
    admin: bool = False,
    use_activity_events: bool = True,
    activity_lookback: int = 60,
    max_default_workspaces: int = 10,
    confirm_large_scan: bool = False,
    dry_run: bool = False,
    only_in_progress: bool = False,
    poll: int = 0,
    loop: bool = False,
    loop_duration: int = 30,
    poll_interval: int = 30,
    stop_notebook_sessions: bool = False,
    sleep_interval: float = 0.0,
    max_retries: int = 6,
    concurrency: int = 16,
    workspace_concurrency: int = 8,
    interactive: bool = False,
    verbose: bool = False,
    json_output: bool = False,
) -> dict:
    """Cancel in-progress Microsoft Fabric jobs (notebook + pipeline runs).

    Default behavior (Fabric Admin recipient):
        cancel_in_progress_jobs()                # tenant-wide via /admin/activityevents
        cancel_in_progress_jobs(dry_run=True)    # preview only

    Targeted (non-admin, or specific workspaces):
        cancel_in_progress_jobs(workspace="myws", poll=60)
        cancel_in_progress_jobs(workspace=["ws1","ws2"], poll=60)

    Per-workspace mode (no admin role available):
        cancel_in_progress_jobs(use_activity_events=False, workspace="myws")

    Continuous monitoring:
        cancel_in_progress_jobs(
            loop=True, loop_duration=30, poll_interval=30,
            exclude_workspace=["Admin"], poll=45,
        )

    Returns the aggregated summary dict.
    """
    if isinstance(workspace, str):
        workspaces_arg = [workspace]
    elif workspace is None:
        workspaces_arg = []
    else:
        workspaces_arg = list(workspace)

    ns = argparse.Namespace(
        workspace=workspaces_arg,
        item=list(item or []),
        item_types=list(item_type or []),
        exclude_items=list(exclude_item or []),
        exclude_workspaces=list(exclude_workspace or []),
        admin=admin,
        use_activity_events=use_activity_events,
        activity_lookback=activity_lookback,
        max_default_workspaces=max_default_workspaces,
        confirm_large_scan=confirm_large_scan,
        dry_run=dry_run,
        include_not_started=not only_in_progress,
        poll=poll,
        loop=loop,
        loop_duration=loop_duration,
        poll_interval=poll_interval,
        stop_notebook_sessions=stop_notebook_sessions,
        sleep_interval=sleep_interval,
        max_retries=max_retries,
        concurrency=concurrency,
        workspace_concurrency=workspace_concurrency,
        interactive=interactive,
        verbose=verbose,
        json=json_output,
    )
    return _run_with_args(ns)


def _run_with_args(args: argparse.Namespace) -> dict:
    """Shared core used by main() (CLI) and cancel_in_progress_jobs() (notebook)."""
    CONFIG["sleep_interval"] = max(0.0, args.sleep_interval)
    CONFIG["max_retries"] = max(1, args.max_retries)

    print("Acquiring Fabric token...")
    token = get_fabric_token()
    session = requests.Session()
    session.headers.update(auth_header(token))

    pbi_session: requests.Session | None = None
    # Only fetch PBI token if we'll actually use Activity Events.
    if args.use_activity_events and not args.workspace:
        print("Acquiring Power BI admin token (for /admin/activityevents)...")
        pbi_session = requests.Session()
        pbi_session.headers.update(auth_header(get_powerbi_token()))

    # Dispatch: explicit workspace arg always wins over activity-events default.
    if args.workspace:
        workspaces: list[dict] = []
        for w in args.workspace:
            ws_id, ws_name = resolve_workspace_id(session, w)
            workspaces.append({"id": ws_id, "displayName": ws_name})
        # If the caller didn't explicitly opt into activity events, ensure we
        # take the per-workspace path so pbi_session isn't required.
        if args.use_activity_events:
            print("(workspace= explicitly given, ignoring use_activity_events default)")
            args.use_activity_events = False
    elif args.use_activity_events:
        print("Using Activity Events for tenant-wide discovery "
              "(single /admin/activityevents call, Fabric Admin required).")
        workspaces = []
    elif args.admin:
        print("Enumerating ALL tenant workspaces via Admin API (requires Fabric Admin)...")
        workspaces = list_all_workspaces(session, admin=True)
    else:
        # Default: scan every workspace the signed-in user has access to.
        print("Enumerating workspaces the signed-in user can see "
              "(Monitor hub equivalent)...")
        workspaces = list_all_workspaces(session, admin=False)

        if (len(workspaces) > args.max_default_workspaces
                and not args.confirm_large_scan):
            ws_names = ", ".join(w["displayName"] for w in workspaces[:5])
            print()
            print(f"!!! Found {len(workspaces)} accessible workspaces "
                  f"(first 5: {ws_names}, ...).")
            print(f"!!! Scanning that many will fan out thousands of API calls "
                  f"and likely hit 429 throttling for many minutes.")
            print(f"!!! Refusing to proceed. Options:")
            print(f"!!!   1) Pass workspace=... (or -w on the CLI) to target "
                  f"specific workspaces.")
            print(f"!!!   2) Use use_activity_events=True (one tenant-wide call, "
                  f"Fabric Admin only).")
            print(f"!!!   3) Pass confirm_large_scan=True to proceed anyway "
                  f"(slow + throttled).")
            print(f"!!!   4) Raise max_default_workspaces=N (current default: "
                  f"{args.max_default_workspaces}).")
            return {"error": "too_many_workspaces",
                    "workspace_count": len(workspaces)}

    if args.exclude_workspaces:
        excl = {e.lower() for e in args.exclude_workspaces}
        before = len(workspaces)
        workspaces = [w for w in workspaces if w["displayName"].lower() not in excl]
        print(f"Excluded {before - len(workspaces)} workspace(s) by name "
              f"(matched {args.exclude_workspaces}).")

    print(f"Workspaces in scope:        {len(workspaces)}")
    statuses = _build_status_set(args.include_not_started)
    print(f"Active statuses:            {sorted(statuses)}")
    if args.item:
        print(f"Filtering by item name/GUID: {args.item}")
    if args.item_types:
        print(f"Filtering by item type:      {args.item_types}")
    if args.exclude_items:
        print(f"Excluding items:             {args.exclude_items}")
    print(f"Throttle: sleep_interval={CONFIG['sleep_interval']}s, "
          f"max_retries={CONFIG['max_retries']}")

    aggregated: dict = {
        "iterations": 0,
        "found_total": 0,
        "cancel_succeeded_total": 0,
        "cancel_failed_total": 0,
        "notebook_sessions_stopped_total": 0,
    }

    if not args.loop:
        summary = run_once(session, workspaces, args, pbi_session)
        aggregated["iterations"] = 1
        aggregated["found_total"] = summary.get("found", 0)
        aggregated["cancel_succeeded_total"] = summary.get("cancel_succeeded", 0)
        aggregated["cancel_failed_total"] = summary.get("cancel_failed", 0)
        aggregated["notebook_sessions_stopped_total"] = summary.get(
            "notebook_sessions_stopped", 0
        )
    else:
        deadline = time.time() + args.loop_duration * 60
        print(f"\n[LOOP] running for up to {args.loop_duration} min, "
              f"rescan every {args.poll_interval}s")
        while time.time() < deadline:
            aggregated["iterations"] += 1
            print("\n" + "=" * 100)
            remaining = int(deadline - time.time())
            print(f"[LOOP] iteration {aggregated['iterations']}  (remaining: {remaining}s)")
            summary = run_once(session, workspaces, args, pbi_session)
            aggregated["found_total"] += summary.get("found", 0)
            aggregated["cancel_succeeded_total"] += summary.get("cancel_succeeded", 0)
            aggregated["cancel_failed_total"] += summary.get("cancel_failed", 0)
            aggregated["notebook_sessions_stopped_total"] += summary.get(
                "notebook_sessions_stopped", 0
            )
            if time.time() >= deadline:
                break
            sleep_s = min(args.poll_interval, max(0, int(deadline - time.time())))
            if sleep_s > 0:
                print(f"[LOOP] sleeping {sleep_s}s before next iteration...")
                time.sleep(sleep_s)
        print("\n[LOOP] window complete.")

    print(f"\nSummary: {aggregated}")
    if args.json:
        print(json.dumps(aggregated, default=str))
    return aggregated


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = _run_with_args(args)
    if result.get("error"):
        return 2
    return 0 if result.get("cancel_failed_total", 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
