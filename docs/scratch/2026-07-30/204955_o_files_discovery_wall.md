# O(files) object-store discovery wall (notebook field learning)

## Where the silent grind happens (client-side; dashboard empty)

All of this can complete **before** the first Dask task renders:

1. **Recursive listing** of every partition directory against the appliance  
   (sequential LISTs, high per-request latency).
2. **Graph construction and optimization** over one task per file.
3. **Shipping** a graph with thousands of tiny tasks to the scheduler.

At a few dozen files this is invisible. At real-world scale — months × many
files per day — you can sit for **minutes** before workers start. When they do,
overhead-dominated small-file tasks dominate runtime.

`filters=` on `dd.read_parquet("s3://…/spans/", …)` only prunes **reads after**
full discovery. It does **not** prune the LIST.

## Near-term cures (escalating)

| Cure | Effect |
|------|--------|
| **Prune before discovery** | `read_parquet` / `find` under `…/spans/date=2026-07-01/` lists one day, not twenty. |
| **List once, explicitly** | `fs.find(prefix)` (single paginated recursive LIST) → hand file list to `read_parquet`. Same pattern as otel-navigator enumerate-window-read. |
| **Fatten tasks** | `aggregate_files=True` (or `blocksize`) merges small files into fewer partitions. |
| **`ddf.persist()`** | Pay discovery + load once when iterating aggregates. |

`cluster_env.load_active_spans_ddf(date=…, under=…, aggregate_files=True, persist=…)`
implements the near-term pattern.

## Strategic point (roadmap #42–45)

Object-store listing as a query planner is fundamentally **O(files)**. Iceberg /
the metadata plane replaces it with **manifest-based planning**: the catalog
already knows every file, partition values, and column stats, so “plan the query”
is a few metadata reads with real predicate pruning **before** a data byte moves.

The notebook grind on ~20 days of OTEL spans is a **small-scale preview** of what
petabytes of HDF5-derived data would do — which is why the
pointer-table / kerchunk / Iceberg plane exists in the roadmap.

| Horizon | Fix |
|---------|-----|
| Near-term | Explicit list + prune root + fatten partitions (`cluster_env` / notebooks) |
| Durable | Catalog / Iceberg metadata plane |
