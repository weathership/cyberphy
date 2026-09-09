# Paste into Dask_S3_Validation notebook when zoom does not hit Dask workers.
# Requires: ddf (dask dataframe), client connected, holoviews, datashader, panel.
import os
os.environ.setdefault("BOKEH_RESOURCES", "inline")

import datashader as ds
import holoviews as hv
from holoviews.operation.datashader import rasterize, dynspread
import panel as pn

hv.extension("bokeh", inline=True)
pn.extension()

DASK_VIZ_FRAC = float(os.getenv("DASK_VIZ_FRAC", "0.1"))
ddf_pts = ddf.assign(
    timestamp_numeric=ddf["start_time_unix_nano"] / 1e9,
    duration_ms=ddf["duration_ns"] / 1e6,
)[["timestamp_numeric", "duration_ms"]]
if 0 < DASK_VIZ_FRAC < 1.0:
    ddf_pts = ddf_pts.sample(frac=DASK_VIZ_FRAC, random_state=0)
ddf_pts = ddf_pts.persist()
try:
    from dask.distributed import wait
    wait(ddf_pts)
except Exception:
    pass
n_est = int(ddf_pts.shape[0].compute())
print(f"persisted ~{n_est} rows on workers")

points = hv.Points(ddf_pts, kdims=["timestamp_numeric", "duration_ms"])
shaded = rasterize(
    points, aggregator=ds.count(), width=900, height=400, dynamic=True, precompute=True,
).opts(
    cmap="fire", width=900, height=400, colorbar=True,
    tools=["hover", "wheel_zoom", "box_zoom", "pan", "reset"],
    active_tools=["wheel_zoom"],
    title=f"Dask datashader (~{n_est}) — zoom should show tasks on dashboard",
)
pn.panel(dynspread(shaded, threshold=0.5, max_px=5), width=920, height=440)
