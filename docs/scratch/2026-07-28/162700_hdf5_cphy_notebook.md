# HDF5 CPHY acquisition notebook

## Delivered
- `zarf/scripts/generate_hdf5.py` — CPHY/OTel structural analog, profiles lab / airgap_2tb,
  hive keys under `datasets/hdf5/otelcphy/`, filename `otelcphy_<ISO>_<part>Z.h5`,
  time_unit ns|us, group_style underscore|brackets, pointer-table inventory.
- `zarf/notebooks/HDF5_CPHY_Acquisition_Generator.ipynb` — generate, audit, Dask stack,
  Holoviews/datashader, inventory parquet.
- ConfigMap embed includes the new notebook (~85 KiB total).

## Lab smoke
4 parts on s3://cyberphy/datasets/hdf5/otelcphy/... with admin/admin RustFS.
