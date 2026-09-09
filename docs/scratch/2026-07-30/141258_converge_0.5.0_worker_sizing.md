# Converge engine 0.5.0 — surgical worker sizing

## What shipped

Engine **`0.5.0`**: `T4.workers-capacity` no longer only caps replicas via
`schedulable_nodes − 1`. It applies full package worker wire surgically to a
live `DaskCluster/cybersec-dask`.

### Detect
- Pending workers (oversubscription)
- CR / Deployment count vs capacity-capped target
- Explicit sizing drift: `replicas`, `nthreads`, `cpu`, `memory` (+ optional request)

### Remediate (no zarf re-push)
1. Fold aliases `DASK_WORKER_MEM_LIMIT` → `MEMORY`, `MEM_REQUEST` → requests
2. Target = `min(desired, floor((total_alloc_Gi − 8) / worker_Gi))`, Pending shrink
3. Merge-patch CR worker replicas + container args/limits
4. Recycle worker pods when template fields change
5. Reap excess worker Deployments
6. Stamp effective values into `ctx.s3` so later deploys preserve sizing

### Canonical vars
| Var | Role |
|-----|------|
| `DASK_WORKER_REPLICAS` | count (default 4 via converge-node / deploy path) |
| `DASK_WORKER_NTHREADS` | `--nthreads` |
| `DASK_WORKER_CPU` | limits.cpu |
| `DASK_WORKER_MEMORY` | limits.memory + `--memory-limit` |

Unset sizing fields are **not** forced onto a manually sized CR (avoids thrash).
Unset replicas only **caps** oversubscription — never scales up without explicit env.

## Tests
`uv run pytest tests/test_converge_worker_sizing.py` — 19 passed.

## Files
- `zarf/converge/catalog.py`, `__init__.py`, `manual.py`
- `zarf/scripts/converge-node.sh`
- `zarf/AIRGAP-CONVERGE-RUNBOOK.md`, `AIRGAP-REMEDIATION-COMMANDS.md`
- `tests/test_converge_worker_sizing.py`
