# Paste into notebook when DynamicMap raises Image bounds ValueError.
# Requires: ddf, client, holoviews, datashader, panel, numpy
import os
os.environ.setdefault("BOKEH_RESOURCES", "inline")
import numpy as np
import dask.dataframe as dd
import datashader as ds
import holoviews as hv
from holoviews.streams import RangeXY, PlotSize
import panel as pn

hv.config.image_rtol = 1.0
hv.extension("bokeh", inline=True)
pn.extension()

def image_from_agg(agg, kdims, vdim="count"):
    import xarray as xr
    if hasattr(agg, "compute"):
        agg = agg.compute()
    da = list(agg.data_vars.values())[0] if isinstance(agg, xr.Dataset) else agg
    ydim, xdim = da.dims[0], da.dims[1]
    ys = np.asarray(da.coords[ydim].values, dtype=np.float64)
    xs = np.asarray(da.coords[xdim].values, dtype=np.float64)
    arr = np.nan_to_num(np.asarray(da.values, dtype=np.float64), nan=0.0)
    if arr.shape == (xs.size, ys.size):
        arr = arr.T
    return hv.Image((xs, ys, arr), kdims=list(kdims), vdims=[vdim], rtol=1.0)

frac = float(os.getenv("DASK_VIZ_FRAC", "0.1"))
ddf_pts = ddf.assign(
    timestamp_numeric=ddf["start_time_unix_nano"] / 1e9,
    duration_ms=ddf["duration_ns"] / 1e6,
)[["timestamp_numeric", "duration_ms"]]
if 0 < frac < 1:
    ddf_pts = ddf_pts.sample(frac=frac, random_state=0)
ddf_pts = ddf_pts.persist()
x0, x1, y0, y1 = dd.compute(
    ddf_pts.timestamp_numeric.min(), ddf_pts.timestamp_numeric.max(),
    ddf_pts.duration_ms.min(), ddf_pts.duration_ms.max(),
)
dx, dy = (float(x1 - x0) * 0.01 or 1.0), (float(y1 - y0) * 0.01 or 1.0)
x0, x1, y0, y1 = float(x0 - dx), float(x1 + dx), float(y0 - dy), float(y1 + dy)
print("rows", int(ddf_pts.shape[0].compute()), "extents", (x0, x1), (y0, y1))

def density(x_range, y_range, width=900, height=400, scale=1.0):
    w, h = max(int(width or 900), 2), max(int(height or 400), 2)
    xr = x_range if x_range and x_range[0] is not None else (x0, x1)
    yr = y_range if y_range and y_range[0] is not None else (y0, y1)
    xa, xb = float(min(xr)), float(max(xr))
    ya, yb = float(min(yr)), float(max(yr))
    if xb <= xa: xb = xa + 1
    if yb <= ya: yb = ya + 1
    agg = ds.Canvas(w, h, x_range=(xa, xb), y_range=(ya, yb)).points(
        ddf_pts, "timestamp_numeric", "duration_ms", ds.count()
    )
    return image_from_agg(agg, ["timestamp_numeric", "duration_ms"]).opts(
        cmap="fire", colorbar=True, width=w, height=h, framewise=True,
        default_tools=["pan", "wheel_zoom", "box_zoom", "reset", "hover"],
        active_tools=["wheel_zoom"],
        title="Dask density (safe Image) — zoom should hit workers",
    )

rng = RangeXY(x_range=(x0, x1), y_range=(y0, y1))
sz = PlotSize(width=900, height=400)
dmap = hv.DynamicMap(density, streams=[rng, sz])
rng.source = dmap
pn.panel(dmap, width=920, height=440)
