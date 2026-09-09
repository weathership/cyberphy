# Air-gap notebook deps baked into image

## Image
`localhost/cybersec-dask:2025.2.0-notebook` — build verifies holoviews, datashader, h5py, generate_hdf5.

## Sources
- zarf/images/requirements-airgap.txt (+ kerchunk, h5py pin, ipykernel)
- Dockerfile.cybersec-dask: PYTHONPATH=/app, generate_hdf5 at /app/
- jupyterhub-values PYTHONPATH includes /app
- dask-cluster PYTHONPATH=/app
- ConfigMap still embeds notebooks + generate_hdf5.py

## Deploy path (air-gap)
1. Build image on connected build host
2. Load into zarf package / registry
3. Deploy — no runtime pip
