# Air-gap image pin: `2025.2.0-notebook` (h5py baked)

## Problem
Helm/live drifts used base tags (`2025.2.0`, `…-111e854736`, `…-fbc70781a3`) that
lacked notebook deps. Jupyter then failed:

```text
SystemExit: h5py is required
```

Air-gap must never `pip install` at runtime.

## Canonical package image
| Item | Value |
|------|--------|
| Tag | `localhost:5555/cybersec-dask:2025.2.0-notebook` |
| Dockerfile | `zarf/images/Dockerfile.cybersec-dask` |
| Deps | `zarf/images/requirements-airgap.txt` (includes `h5py`, holoviews, …) |
| Build gate | Dockerfile `RUN python -c "… import h5py …"` fails the build if missing |

Referenced from:
- `zarf/zarf.yaml` (`cybersec-images`, `jupyterhub` images)
- `zarf/manifests/dask-cluster.yaml`, `jupyterhub-values.yaml`, `panel-viz.yaml`, `engine.yaml`
- `zarf/artifacts.manifest.json`

## Build (connected host)
```bash
podman build -t localhost:5555/cybersec-dask:2025.2.0-notebook \
  -f zarf/images/Dockerfile.cybersec-dask .
# optional: also tag for local pull tests
podman tag localhost:5555/cybersec-dask:2025.2.0-notebook \
  localhost/cybersec-dask:2025.2.0-notebook
```

## Package create
```bash
cd zarf && zarf package create . --confirm
# create fails if the image tag is not present locally
```

## Deploy smoke
`dask-cluster` onDeploy runs `import h5py, holoviews, …` in the scheduler pod.
Missing deps fail the action with a rebuild hint.

## Jupyter / Dask
Same image for singleuser, workers, scheduler, panel — one bundle, one push.
