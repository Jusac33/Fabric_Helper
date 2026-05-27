# Fabric_Helper

Operator scripts for Microsoft Fabric admins and power users.

## Files in this repo

| File | Use it when... |
| --- | --- |
| [`Cancel-In-Progress-Jobs.ipynb`](./Cancel-In-Progress-Jobs.ipynb) | **You want to run it from a notebook.** Self-contained: the full library is embedded as a single "paste-once" cell at the top, followed by recipe cells. Open in Fabric, Run All. No extra file upload needed. |
| [`cancel_in_progress_jobs.py`](./cancel_in_progress_jobs.py) | **You want to run it from the terminal** (CLI), import it as a module in your own Python project, or schedule it (cron / Fabric pipeline / Azure DevOps). |
| [`requirements.txt`](./requirements.txt) | `pip install -r requirements.txt` for CLI / local use. The notebook installs `requests` itself in its first cell. |

## `cancel_in_progress_jobs.py` (library / CLI)

Find every Fabric job (Notebook, DataPipeline, MLExperiment, Dataflow,
SparkJobDefinition, Lakehouse table maintenance, ...) that is currently
**InProgress / NotStarted / Starting / Running** — the same set the
Microsoft Fabric Monitor hub shows under the *In progress* filter — and
cancel it.

#### Highlights

- **All item types** - iterates `/v1/workspaces/{ws}/items` and calls
  `/jobs/instances` per item. Not notebook-specific.
- **Targeted by default** - `-w` is required (repeatable). Use `--admin`
  for tenant-wide via Admin APIs, or `--use-activity-events` for a single
  tenant-wide call via Power BI `/admin/activityevents`.
- **Fast tenant-wide path** - `--use-activity-events` calls Power BI's
  `/admin/activityevents` once (instead of fanning out per workspace).
  Requires Fabric Administrator role. Pattern from
  [hfleitas/fabriciq commit 22ef278](https://github.com/hfleitas/fabriciq/commit/22ef278871c84b7ac42a5de266157bfd8cb2e407).
- **Workspace-level parallelism** - `--workspace-concurrency` (default 8)
  scans many workspaces in parallel; per-workspace items parallelised via
  `--concurrency` (default 16).
- **Item filters** - `-i name|guid` (wildcards), `--item-type`,
  `--exclude-item`, `--exclude-workspace`, and an `--interactive`
  numbered picker.
- **API throttling** - retries on HTTP `429` honoring `Retry-After`,
  exponential backoff with jitter on `5xx`, optional `--sleep-interval`
  between calls.
- **Continuous loop** - `--loop --loop-duration MIN --poll-interval SEC`
  keeps cancelling new in-progress jobs that appear during the window.
- **Notebook Spark sessions** - `--stop-notebook-sessions` also stops
  live Spark sessions left by notebook runs.
- **Dual-URL cancel** - tries both `/jobs/instances/{id}/cancel` and the
  legacy `/jobInstances/{id}/cancel` for older items.
- **Verification** - `--poll N` waits and reports the terminal status
  (`Cancelled` / `Completed` / `Failed`) of each cancelled job.

#### Auth

Uses `azure-identity` (`DefaultAzureCredential`) and falls back to
`az account get-access-token --resource https://api.fabric.microsoft.com`.
**Inside a Fabric notebook**, it auto-uses `notebookutils.mssparkutils.credentials.getToken(...)`
(no `az login` needed).

```bash
# one-time (terminal use)
az login
pip install -r requirements.txt
```

#### Recommended for Fabric Admins (DEFAULT mode)

If the person running this is a Fabric Administrator, just call the
function with no arguments - `use_activity_events=True` is the default,
so a single tenant-wide API call (`GET /v1.0/myorg/admin/activityevents`)
discovers every running notebook + pipeline run across the entire tenant
in one request. No per-workspace fanout, no throttling.

```python
from cancel_in_progress_jobs import cancel_in_progress_jobs

# Preview first
cancel_in_progress_jobs(dry_run=True)

# Cancel everything in-progress tenant-wide and verify
cancel_in_progress_jobs(
    activity_lookback=60,                          # minutes to look back
    exclude_workspace=["Admin", "Fabric Admin"],   # safety: never touch
    poll=60,
)

# Explicit time window (computed dynamically)
from datetime import datetime, timedelta, timezone
now   = datetime.now(timezone.utc).replace(microsecond=0)
start = (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
end   = now.isoformat().replace("+00:00", "Z")
cancel_in_progress_jobs(activity_start=start, activity_end=end, poll=60)

# Continuous monitoring for the next 30 minutes
cancel_in_progress_jobs(
    loop=True, loop_duration=30, poll_interval=30,
    exclude_workspace=["Admin", "Fabric Admin"],
    sleep_interval=0.1,
)
```



Upload `cancel_in_progress_jobs.py` to your notebook's working directory
(or use the self-contained [`Cancel-In-Progress-Jobs.ipynb`](./Cancel-In-Progress-Jobs.ipynb))
and call the Python API. For Fabric Admins the headline call needs no
workspace at all (see admin section above). For users **without** the
Fabric Administrator role, target workspaces explicitly:

```python
from cancel_in_progress_jobs import cancel_in_progress_jobs

# Preview
cancel_in_progress_jobs(workspace="<your-workspace-name>", dry_run=True)

# Cancel and verify
cancel_in_progress_jobs(workspace="<your-workspace-name>", poll=60)

# Multi-workspace
cancel_in_progress_jobs(workspace=["<workspace-1>", "<workspace-2>"], poll=60)

# Continuous loop on a specific workspace
cancel_in_progress_jobs(
    workspace="<your-workspace-name>",
    loop=True, loop_duration=30, poll_interval=30,
    sleep_interval=0.1, poll=45,
)
```

See [`Cancel-In-Progress-Jobs.ipynb`](./Cancel-In-Progress-Jobs.ipynb) for
the ready-to-import admin notebook (no workspace argument needed).

#### Usage

```bash
# Cancel everything in-progress in a workspace, verify with poll
python cancel_in_progress_jobs.py -w "<your-workspace-name>" --poll 60

# Multi-workspace
python cancel_in_progress_jobs.py -w ws1 -w ws2 --poll 60

# Tenant-wide via Admin API (per-workspace fanout, parallel)
python cancel_in_progress_jobs.py --admin --workspace-concurrency 16 --poll 60

# FAST tenant-wide via Activity Events (one API call, requires Fabric Admin)
python cancel_in_progress_jobs.py --use-activity-events --activity-lookback 60 --poll 60

# Skip specific workspaces (admin/critical)
python cancel_in_progress_jobs.py --admin \
  --exclude-workspace "Admin" --exclude-workspace "Fabric Admin"

# Preview only
python cancel_in_progress_jobs.py -w ws --dry-run

# Item filters
python cancel_in_progress_jobs.py -w ws -i "01_Bronze*" -i "*Silver*"
python cancel_in_progress_jobs.py -w ws --item-type Notebook --item-type DataPipeline
python cancel_in_progress_jobs.py -w ws --exclude-item "*Production*"

# Interactive picker
python cancel_in_progress_jobs.py -w ws --interactive --poll 60

# Continuous loop for 30 minutes, gentle pacing
python cancel_in_progress_jobs.py -w ws --loop --loop-duration 30 --poll-interval 30 \
  --sleep-interval 0.1

# Throttling controls
python cancel_in_progress_jobs.py -w ws --sleep-interval 0.25 --max-retries 10

# Also stop live notebook Spark sessions
python cancel_in_progress_jobs.py -w ws --stop-notebook-sessions
```

#### Flags

| Flag | Purpose |
| --- | --- |
| `-w / --workspace` (repeat) | Workspace name or GUID. Required unless `--admin` / `--use-activity-events`. |
| `-i / --item` (repeat) | Restrict to items by name or GUID (wildcards / substring). |
| `--item-type` (repeat) | Restrict to item type (Notebook, DataPipeline, ...). |
| `--exclude-item` (repeat) | Skip items by name or GUID. |
| `--exclude-workspace` (repeat) | Skip workspaces by display name (case-insensitive). |
| `--interactive` | Numbered picker for the discovered jobs. |
| `--admin` | Tenant-wide via Admin APIs (per-workspace scan, parallel). |
| `--use-activity-events` | FAST: tenant-wide via Power BI `/admin/activityevents` (one call). |
| `--activity-lookback MIN` | Lookback window for activity events (default 60). |
| `--dry-run` | Detect only; do not cancel. |
| `--only-in-progress` | Skip NotStarted / Starting jobs. |
| `--poll N` | After cancel, poll up to N seconds for terminal status. |
| `--loop` | Continuously rescan and cancel new in-progress jobs. |
| `--loop-duration MIN` | Loop duration (default 30 min). |
| `--poll-interval SEC` | Seconds between loop iterations (default 30). |
| `--stop-notebook-sessions` | Also stop live notebook Spark sessions. |
| `--sleep-interval SEC` | Pace between API calls. |
| `--max-retries N` | Max retries on 429/5xx (default 6). |
| `--concurrency K` | Parallel items-per-workspace requests (default 16). |
| `--workspace-concurrency K` | Parallel workspaces scanned at once (default 8). |
| `--json` | Machine-readable summary. |
| `--verbose` | Print noisy per-item warnings. |

#### Required APIs

Per-workspace (default):
- `GET  /v1/workspaces` - list workspaces
- `GET  /v1/admin/workspaces` - tenant-wide (admin only)
- `GET  /v1/workspaces/{ws}/items` - enumerate items
- `GET  /v1/workspaces/{ws}/items/{id}/jobs/instances` - list job instances
- `POST /v1/workspaces/{ws}/items/{id}/jobs/instances/{inst}/cancel` - cancel
- `POST /v1/workspaces/{ws}/items/{id}/jobInstances/{inst}/cancel` - legacy fallback
- `GET  /v1/workspaces/{ws}/notebooks/{nb}/sessions` - list Spark sessions
- `POST /v1/workspaces/{ws}/notebooks/{nb}/sessions/{sid}/stop` - stop session

Fast tenant-wide mode (`--use-activity-events`, Fabric Admin required):
- `GET  https://api.powerbi.com/v1.0/myorg/admin/activityevents` - one tenant-wide call
- `GET  https://api.powerbi.com/v1.0/myorg/admin/groups` - workspace name lookup

#### Credits

Polling, retry, and continuous-loop patterns adapted from
[hfleitas/fabriciq](https://github.com/hfleitas/fabriciq/blob/main/Stop-All-Running-Fabric-Workloads.ipynb).

## License

MIT
