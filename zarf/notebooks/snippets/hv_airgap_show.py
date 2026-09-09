"""Paste into a notebook cell when Holoviews plots are blank in air-gap.

Requires: holoviews, bokeh, pillow, and either `heatmap` + `sub` already defined
or `viz_np` from the CPHY notebook.
"""
import os, io, base64
os.environ["BOKEH_RESOURCES"] = "inline"

import numpy as np
import holoviews as hv
from bokeh.resources import INLINE
from bokeh.embed import file_html
from IPython.display import display, HTML, Image as IPImage
from PIL import Image as PILImage

hv.extension("bokeh", logo=False)

def _array_to_png_bytes(a, width=900, height=420):
    a = np.asarray(a, dtype=np.float64)
    finite = np.isfinite(a)
    if finite.any():
        lo, hi = float(a[finite].min()), float(a[finite].max())
        if hi <= lo: hi = lo + 1.0
        a = (a - lo) / (hi - lo)
    else:
        a = np.zeros_like(a)
    a = np.clip(a, 0, 1)
    rgb = (np.stack([
        np.clip(1.5 * a, 0, 1),
        np.clip(1.5 * a - 0.5, 0, 1),
        np.clip(1.5 * a - 1.0, 0, 1),
    ], axis=-1) * 255).astype(np.uint8)
    im = PILImage.fromarray(rgb, mode="RGB").resize((width, height), PILImage.NEAREST)
    buf = io.BytesIO(); im.save(buf, format="PNG"); return buf.getvalue()

def show_hv(obj, *, width=900, height=420, fallback_array=None):
    renderer = hv.Store.renderers.get("bokeh") or hv.renderer("bokeh")
    try:
        plot = renderer.get_plot(obj).state
        html_doc = file_html(plot, INLINE, "holoviews")
        assert "cdn.bokeh.org" not in html_doc
        b64 = base64.b64encode(html_doc.encode("utf-8")).decode("ascii")
        display(HTML(
            f'<iframe src="data:text/html;base64,{b64}" width="{width+40}" '
            f'height="{height+80}" style="border:0;background:#111;"></iframe>'
        ))
        print("Displayed via Bokeh INLINE (air-gap safe)."); return
    except Exception as e:
        print(f"INLINE failed ({e}); PNG fallback")
    display(IPImage(data=_array_to_png_bytes(fallback_array, width, height)))

# Re-show existing objects from prior cells, if present
if "heatmap" in dir() or "heatmap" in globals():
    show_hv(heatmap, fallback_array=sub if "sub" in globals() else viz_np)
elif "viz_np" in globals():
    n_s, n_t = viz_np.shape
    sub = viz_np[::max(1, n_s//256), ::max(1, n_t//1000)]
    ss, tt = sub.shape
    hm = hv.Image(
        (np.arange(tt, dtype=float), np.arange(ss, dtype=float), sub),
        kdims=["time_sample", "series"], vdims=["value"],
    ).opts(cmap="fire", width=900, height=420, colorbar=True,
           title="CPHY acquisition (re-render)")
    show_hv(hm, fallback_array=sub)
else:
    print("Define viz_np or heatmap first (run the compute cell).")
