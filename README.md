# Fabric_Helper

Operator scripts for Microsoft Fabric admins and power users.

## Scripts

### `cancel_in_progress_jobs.py`

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

#### Recommended for Fabric Admins (tenant-wide, one API call)

If the person running this is a Fabric Administrator, the fastest and
cleanest path is the Activity Events API — one tenant-wide call instead
of fanning out per workspace:

```python
from cancel_in_progress_jobs import cancel_in_progress_jobs

# Preview first
cancel_in_progress_jobs(use_activity_events=True, dry_run=True)

# Cancel and verify
cancel_in_progress_jobs(
    use_activity_events=True,
    activity_lookback=60,                       # minutes
    exclude_workspace=["Admin", "Fabric Admin"], # safety: never touch
    poll=60,
)

# Continuous monitoring for the next 30 minutes
cancel_in_progress_jobs(
    use_activity_events=True,
    loop=True, loop_duration=30, poll_interval=30,
    exclude_workspace=["Admin", "Fabric Admin"],
    sleep_interval=0.1,
)
```

This uses `GET https://api.powerbi.com/v1.0/myorg/admin/activityevents` —
the same data source the Power BI Activity Events report uses — and returns
notebook + pipeline runs in `InProgress`/`Running`/`Starting` state across
the entire tenant in a single call.



Upload `cancel_in_progress_jobs.py` to your notebook's working directory
(or `%pip install` the helper) and call the Python API:

```python
from cancel_in_progress_jobs import cancel_in_progress_jobs

# Preview
cancel_in_progress_jobs(workspace="crestshield-smartclaims-sachinsaraf", dry_run=True)

# Cancel and verify
cancel_in_progress_jobs(workspace="crestshield-smartclaims-sachinsaraf", poll=60)

# Multi-workspace
cancel_in_progress_jobs(workspace=["ws1", "ws2"], poll=60)

# Continuous loop (mirrors hfleitas/fabriciq pattern)
cancel_in_progress_jobs(
    workspace="crestshield-smartclaims-sachinsaraf",
    loop=True, loop_duration=30, poll_interval=30,
    sleep_interval=0.1, poll=45,
)

# Tenant-wide fast path (Fabric Admin required)
cancel_in_progress_jobs(use_activity_events=True, exclude_workspace=["Admin"])
```

See [`Cancel-In-Progress-Jobs.ipynb`](./Cancel-In-Progress-Jobs.ipynb) for a
ready-to-import notebook with all recipes.

#### Usage

```bash
# Cancel everything in-progress in a workspace, verify with poll
python cancel_in_progress_jobs.py -w "crestshield-smartclaims-sachinsaraf" --poll 60

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
