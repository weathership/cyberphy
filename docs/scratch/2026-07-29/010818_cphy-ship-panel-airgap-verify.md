# CPHY sample ship + Panel air-gap verify

## CPHY notebook (one shipping copy)

- Source of truth: `zarf/notebooks/HDF5_CPHY_Acquisition_Generator.ipynb`
- Embed list: `zarf/scripts/embed-notebooks.py` → `INCLUDE_NOTEBOOKS` includes **one** CPHY entry
- ConfigMap: `zarf/manifests/sample-notebooks-configmap.yaml` keys:
  - `OTEL_Data_Generator.ipynb`
  - `Dask_S3_Validation.ipynb`
  - `HDF5_CPHY_Acquisition_Generator.ipynb` ← single CPHY
  - `HDF5_Iceberg_Metadata_Provider.ipynb`
- Stripped source matches ConfigMap JSON (byte-identical after embed strip)
- Image: Dockerfile does **not** bake full notebooks under `/app/sample-notebooks` (README pointer only); JupyterHub seeds writable `/root/*.ipynb` from ConfigMap

## Panel apps air-gap

| Concern | Status |
|---------|--------|
| `cdn.bokeh.org` in app runtime | None (comments only) |
| `fonts.googleapis.com` | Neutralized in both apps (Fast theme `_resources["font"]={}`, `FONT_URL=""`, template `font_url=""` + system font stack) |
| ghostty-web | Baked in image; served `/ghostty/*` via `--static-dirs` |
| BOKEH_RESOURCES | **Added** `server` on panel-viz Deployment + `setdefault` in `otel-navigator.py` / `data-view.py` before bokeh/panel import |
| Jupyter notebooks | Already `BOKEH_RESOURCES=inline` via jupyterhub-values + notebook cells |

Build-time only: Dockerfile still downloads ghostty-web from npm at **image build** (air-gap runtime uses baked assets).

## OTEL Navigator WebSocket (package-baked)

Already in commits (e.g. `f1ad1eed` terminal WS + later air-gap fixes):

| Path | Target |
|------|--------|
| Page `:5006` (local panel serve) | `ws://host:8765` |
| Page `:30506` (NodePort) | `ws://host:30765` |
| Ingress / :80/:443 | same-origin `/ws` |
| Override | `PTY_PROXY_WS` zarf var → Deployment env |

- Sidecar: `pty-proxy` on 8765; Service NodePorts 30506 (http) + 30765 (ws)
- Ingress `panel-viz`: path `/ws` → service port 8765; `/` → 5006; proxy timeouts 3600s

## Follow-up for full package refresh

Rebuild/push `cybersec-dask:2025.2.0-notebook` (or content-tagged) so image includes updated navigator/data-view sources, then re-package if shipping a new Zarf artifact.
