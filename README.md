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

- **All item types** — iterates `/v1/workspaces/{ws}/items` and calls
  `/jobs/instances` per item. Not notebook-specific.
- **Targeted by default** — `-w` is required (repeatable). Use `--admin`
  for explicit tenant-wide scope via the Admin APIs.
- **Item filters** — `-i name|guid` (wildcards), `--item-type`,
  `--exclude-item`, and an `--interactive` numbered picker.
- **API throttling** — retries on HTTP `429` honoring `Retry-After`,
  exponential backoff with jitter on `5xx`, optional `--sleep-interval`
  between calls.
- **Continuous loop** — `--loop --loop-duration MIN --poll-interval SEC`
  keeps cancelling new in-progress jobs that appear during the window.
- **Notebook Spark sessions** — `--stop-notebook-sessions` also stops
  live Spark sessions left by notebook runs.
- **Verification** — `--poll N` waits and reports the terminal status
  (`Cancelled` / `Completed` / `Failed`) of each cancelled job.

#### Auth

Uses `azure-identity` (`DefaultAzureCredential`) and falls back to
`az account get-access-token --resource https://api.fabric.microsoft.com`.

```bash
# one-time
az login
pip install -r requirements.txt
```

#### Usage

```bash
# Cancel everything in-progress in a workspace, verify with poll
python cancel_in_progress_jobs.py -w "crestshield-smartclaims-sachinsaraf" --poll 60

# Multi-workspace
python cancel_in_progress_jobs.py -w ws1 -w ws2 --poll 60

# Tenant-wide (Fabric Admin only)
python cancel_in_progress_jobs.py --admin --poll 60

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
| `-w / --workspace` (repeat) | Workspace name or GUID. Required unless `--admin`. |
| `-i / --item` (repeat) | Restrict to items by name or GUID (wildcards / substring). |
| `--item-type` (repeat) | Restrict to item type (Notebook, DataPipeline, ...). |
| `--exclude-item` (repeat) | Skip items by name or GUID. |
| `--interactive` | Numbered picker for the discovered jobs. |
| `--admin` | Tenant-wide via Admin APIs. |
| `--dry-run` | Detect only; do not cancel. |
| `--only-in-progress` | Skip NotStarted / Starting jobs. |
| `--poll N` | After cancel, poll up to N seconds for terminal status. |
| `--loop` | Continuously rescan and cancel new in-progress jobs. |
| `--loop-duration MIN` | Loop duration (default 30 min). |
| `--poll-interval SEC` | Seconds between loop iterations (default 30). |
| `--stop-notebook-sessions` | Also stop live notebook Spark sessions. |
| `--sleep-interval SEC` | Pace between API calls. |
| `--max-retries N` | Max retries on 429/5xx (default 6). |
| `--concurrency K` | Parallel cancel/list requests (default 16). |
| `--json` | Machine-readable summary. |
| `--verbose` | Print noisy per-item warnings. |

#### Required APIs

- `GET  /v1/workspaces` — list workspaces
- `GET  /v1/admin/workspaces` — tenant-wide (admin only)
- `GET  /v1/workspaces/{ws}/items` — enumerate items
- `GET  /v1/workspaces/{ws}/items/{id}/jobs/instances` — list job instances
- `POST /v1/workspaces/{ws}/items/{id}/jobs/instances/{inst}/cancel` — cancel
- `GET  /v1/workspaces/{ws}/notebooks/{nb}/sessions` — list Spark sessions
- `POST /v1/workspaces/{ws}/notebooks/{nb}/sessions/{sid}/stop` — stop session

#### Credits

Polling, retry, and continuous-loop patterns adapted from
[hfleitas/fabriciq](https://github.com/hfleitas/fabriciq/blob/main/Stop-All-Running-Fabric-Workloads.ipynb).

## License

MIT
