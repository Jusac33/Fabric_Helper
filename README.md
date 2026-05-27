# Fabric_Helper

Operator notebooks for Microsoft Fabric admins and power users.

## `Cancel-In-Progress-Jobs.ipynb`

Self-contained Fabric notebook that detects every `InProgress` notebook /
pipeline / dataflow run across the workspaces the signed-in user can
access, then cancels them.

### How it works

```
List workspaces  ->  list items per workspace  ->  list job instances per item
                                                          |
                                                          v
                                          filter status == "InProgress"
                                                          |
                                                          v
                                  POST /jobs/instances/{id}/cancel
                                                          |
                                                          v
                                  poll until terminal status reached
```

Uses Microsoft's [`sempy.fabric.FabricRestClient`](https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric.fabricrestclient)
under the notebook's own identity - no `az login`, no token handling.

### How to use

1. In Microsoft Fabric, create or open the workspace where you want to run this.
2. Upload `Cancel-In-Progress-Jobs.ipynb` (or open it from this repo and copy cells).
3. Run cells **1 -> 3 -> 5** for a one-shot cancel pass, or also **7** for
   continuous monitoring (30 min default).

### Cell map (7 cells)

| # | Type | What it does |
|---|---|---|
| 1 | markdown | Intro |
| 2 | markdown | "1. Discover every in-progress job" |
| 3 | code | Sets up `FabricRestClient`, lists workspaces + items + job instances, populates `df` with one row per `InProgress` job. Self-protects by excluding the notebook running this code. |
| 4 | markdown | "2. Cancel every in-progress job" |
| 5 | code | Cancels each job in `df`, polls each cancellation for terminal status (`Cancelled` / `Completed` / `Failed`). |
| 6 | markdown | "3. Continuous monitoring (optional)" |
| 7 | code | Re-scans + cancels every 30 s for 30 min. Use to drain a runaway capacity. |

### Targeting specific workspaces

Top of cell 3:

```python
workspace_filter: list[str] = []   # empty = every workspace the user can see
```

Set it to a list of workspace display names to restrict the scan:

```python
workspace_filter = [
    "my-prod-workspace",
    "another-workspace",
]
```

### Safety features

- **Self-protection.** The notebook reads its own `currentNotebookId` and
  `activityId` from `notebookutils.mssparkutils.runtime.context` and
  skips itself, so the cancel pass never kills the Spark session that
  is running it.
- **No `%pip` magic.** `sempy.fabric`, `pandas`, and `notebookutils` are
  all pre-installed in the Fabric runtime.
- **Throttling.** `get_all_pages()` honours the `Retry-After` header on
  HTTP 429 responses; `cancel_job()` retries up to 6 times with the
  server-instructed wait.

### Required permissions

The user running the notebook must have at least **Member / Contributor**
access to the workspaces being scanned. The notebook does *not* require
Fabric Administrator role - it uses the standard `GET /v1/workspaces`
endpoint, which only returns workspaces the caller can already see.

### API references

- [List Workspaces](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspaces)
- [List Items](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items)
- [List Item Job Instances](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances)
- [Cancel Item Job Instance](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/cancel-item-job-instance)

## License

MIT
