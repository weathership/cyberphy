# Sample Notebooks

**Source of truth:** `zarf/notebooks/` (+ `snippets/cluster_env.py`, `zarf/scripts/generate_hdf5.py`).  
Rebuild ConfigMap: `python3 zarf/scripts/verify-sample-notebooks.py` before `zarf package create`.

## In situ (JupyterHub)

ConfigMap `sample-notebooks` mounts RO at `/root/sample-notebooks/`. On singleuser
**start**, JH copies to writable `/root/`:

- all `*.ipynb` (OTEL, Dask, **HDF5_***)
- `generate_hdf5.py` (CPHY notebook import)
- `cluster_env.py`

Converge **T5.sample-notebooks** fails if any of those are missing. After package
deploy: **Stop My Server → Start My Server** so seeds refresh.

Open `/root/HDF5_CPHY_Acquisition_Generator.ipynb` (not the RO mount).

## Notebooks

| Notebook | Role |
|----------|------|
| `OTEL_Data_Generator.ipynb` | Spans → `…/spans/date=*/batch_*.parquet` |
| `Dask_S3_Validation.ipynb` | Explicit LIST + worker parquet |
| `Dask_S3_Workers_OneCell.ipynb` | Minimal hand-carry (s3fs+dask only) |
| `HDF5_CPHY_Acquisition_Generator.ipynb` | CPHY HDF5 + Dask (idempotent) |
| `HDF5_Iceberg_Metadata_Provider.ipynb` | hdf5_iceberg metadata plane |

## Env (from converge / Zarf)

- `DASK_SCHEDULER_ADDRESS` (not `DASK_SCHEDULER=tcp://…` — breaks dask planning)
- `S3_ENDPOINT`, `S3_BUCKET`, AWS keys (lab RustFS: `admin`/`admin`)
- `OTEL_DATA_PATH` / `OTEL_PREFIX`, `HDF5_PROFILE=lab`, `USE_DASK=1`, `BOKEH_RESOURCES=inline`

## HDF5 notes

Idempotent by default (`ensure_parts`). Force rewrite: `HDF5_FORCE_REGENERATE=1`.
