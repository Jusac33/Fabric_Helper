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

### Scaling: capacity_filter, workspace_filter, and the safety gate

Top of cell 3 has three knobs:

```python
workspace_filter: list[str] = []   # display names of specific workspaces
capacity_filter:  list[str] = []   # capacity display names OR GUIDs
WORKSPACE_PARALLELISM = 8          # workspaces scanned in parallel
MAX_AUTO_WORKSPACES   = 50         # safety: refuse unfiltered scan above N
```

| Tenant size | Recommended setup |
|---|---|
| **Small** (< 50 workspaces) | Leave both filters empty - scans everything in parallel. |
| **Large** (1000+ workspaces) | **Set `capacity_filter`** to the names or GUIDs of the capacity (or capacities) you want to drain. Most operational use case: a single overloaded capacity needs everything cancelled. |
| **Surgical** | Use `workspace_filter` with explicit workspace names. |

If both filters are empty and the user has access to more than
`MAX_AUTO_WORKSPACES` workspaces, the cell **aborts** with a message
telling you to add a filter or raise the cap. This prevents an accidental
1000-workspace serial scan that would take hours and hit rate limits.

### Targeting specific workspaces

```python
workspace_filter = ["my-prod-workspace", "another-workspace"]
```

### Targeting a Fabric capacity (recommended for large tenants)

```python
capacity_filter = ["my-overloaded-capacity"]
# or by GUID:
capacity_filter = ["12345678-1234-1234-1234-123456789012"]
```

### Cell map (7 cells)

| # | Type | What it does |
|---|---|---|
| 1 | markdown | Intro |
| 2 | markdown | "1. Discover every in-progress job" |
| 3 | code | Sets up `FabricRestClient`. Lists workspaces, filters by `workspace_filter` / `capacity_filter`, runs parallel `scan_one_workspace()` futures, populates `df` with one row per `InProgress` job. Self-protects. |
| 4 | markdown | "2. Cancel every in-progress job" |
| 5 | code | Parallel cancel (8-wide) for every row in `df`, then polls each cancelled job for terminal status (60 s deadline). |
| 6 | markdown | "3. Continuous monitoring (optional)" |
| 7 | code | Re-scans + cancels every 30 s for 30 min, re-using the workspaces / filters from cell 3. |

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
