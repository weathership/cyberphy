"""Data-View — ELK Discover–style live explorer for streaming VPC flow logs.

Sibling app to OTEL Navigator. Continuously rebinds to new partitions as the
vpc-flow generator appends (and expires) data under a fixed disk envelope.

Routes (panel serve multi-app):
  /data-view

Environment:
  S3_BUCKET, S3_ENDPOINT, AWS_*, VPC_FLOW_PREFIX (default vpc-flow)
  DATA_VIEW_WINDOW_MIN (default 15)
  DATA_VIEW_DISCOVERY_S (default 2)
  DATA_VIEW_FACET_S (default 5)
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

# Air-gap: Bokeh assets from panel serve (pod-local), never cdn.bokeh.org
os.environ.setdefault("BOKEH_RESOURCES", "server")

import numpy as np
import pandas as pd
import panel as pn
import param

try:
    import holoviews as hv
    import datashader as dsh
    hv.extension("bokeh")
    _HAS_HV = True
    _HAS_DS = True
except Exception:  # pragma: no cover
    _HAS_HV = False
    _HAS_DS = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("data-view")

# Air-gap: neutralize Panel Fast hard-coded Google Fonts (Open Sans).
_AIRGAP_UI_FONT = "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"
try:
    import panel.theme.fast as _fast_theme
    _fast_theme.FONT_URL = ""
    _res = getattr(_fast_theme.Fast, "_resources", None)
    if isinstance(_res, dict):
        _res = dict(_res)
        _res["font"] = {}
        _fast_theme.Fast._resources = _res
    if hasattr(_fast_theme, "FastStyle"):
        _fast_theme.FastStyle.param.font_url.default = ""
        _fast_theme.FastStyle.param.font.default = _AIRGAP_UI_FONT
except Exception as _e:  # pragma: no cover
    logger.warning("airgap font patch failed: %s", _e)

pn.extension("tabulator", loading_spinner="dots", loading_color="#0072B5")

# -------------------------------------------------------------------------
# Config
# -------------------------------------------------------------------------

# Process-stable I/O (survives panel serve session re-exec / missing __builtins__).
# Must import by path so `panel serve /app/data-view.py` resolves it from /app.
import sys as _sys

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
if _APP_DIR not in _sys.path:
    _sys.path.insert(0, _APP_DIR)

import data_view_lib as _dvl  # noqa: E402

S3_BUCKET = _dvl.S3_BUCKET
S3_ENDPOINT = _dvl.S3_ENDPOINT
VPC_FLOW_PREFIX = _dvl.VPC_FLOW_PREFIX
WINDOW_MIN = _dvl.WINDOW_MIN
DISCOVERY_S = float(os.environ.get("DATA_VIEW_DISCOVERY_S", "2"))
FACET_S = float(os.environ.get("DATA_VIEW_FACET_S", "5"))
TABLE_ROWS = int(os.environ.get("DATA_VIEW_TABLE_ROWS", "80"))
MAX_FILES = _dvl.MAX_FILES
CACHE_TTL_S = _dvl.CACHE_TTL_S

FACET_FIELDS = [
    "action",
    "protocol",
    "flow_direction",
    "log_status",
    "dstport",
    "vpc_id",
]

# Short labels — page title is separate (FastListTemplate); nav is Metrics | Data-View.
NAV_HTML = """
<div style="display:flex;gap:16px;align-items:center;font-family:-apple-system,BlinkMacSystemFont,sans-serif;font-size:13px;">
  <a href="/otel-navigator" style="color:#c9d1d9;text-decoration:none;">Metrics</a>
  <a href="/data-view" style="color:#fff;text-decoration:none;font-weight:700;border-bottom:2px solid #58a6ff;padding-bottom:2px;">Data-View</a>
</div>
"""

apply_filters = _dvl.apply_filters


# -------------------------------------------------------------------------
# DataView app
# -------------------------------------------------------------------------

class DataView(param.Parameterized):
    live = param.Boolean(default=True)
    time_window = param.Selector(default=f"{WINDOW_MIN}m", objects=["5m", "15m", "1h", "3h"])
    stream_epoch = param.Integer(default=0)
    catalog_id = param.String(default="")
    filters = param.List(default=[])
    hist_token = param.Integer(default=0)
    facet_token = param.Integer(default=0)
    table_token = param.Integer(default=0)
    rows_in_window = param.Integer(default=0)
    files_in_window = param.Integer(default=0)
    load_ms = param.Number(default=0.0)
    last_event_ts = param.String(default="—")
    status_line = param.String(default="Starting…")
    rps_est = param.Number(default=0.0)
    error = param.String(default="")
    refreshing = param.Boolean(default=False)
    from_cache = param.Boolean(default=False)

    def __init__(self, **params):
        super().__init__(**params)
        self._df_raw = pd.DataFrame()
        self._df = pd.DataFrame()
        self._lock = threading.Lock()
        self._last_facet = 0.0
        self._last_rows = 0
        self._last_rows_t = time.time()
        self._busy = False
        self._cb = None  # UI tick (applies pending + schedules bg)
        self._bg_kicked = False  # first discover thread launched
        self._pending: dict | None = None  # set by bg thread, applied on event loop
        self._last_bg = 0.0
        # Persistent histogram state — must NOT be recreated on every stream_epoch
        # or the user's pan/zoom is wiped by the refresh loop.
        self._hist_view = pn.Column(
            pn.pane.Markdown(
                "### Time histogram\n_Waiting for data…_",
                sizing_mode="stretch_width",
            ),
            sizing_mode="stretch_width",
            height=200,
        )
        self._hist_stream = None  # hv.streams.RangeX — persistent viewport
        self._hist_pipe = None  # hv.streams.Pipe of plot-ready frame
        self._hist_t0 = None  # pd.Timestamp origin for t_sec
        self._hist_t_max = 1.0
        # Absolute user viewport (UTC timestamps). None = follow full window.
        # Source of truth across live refresh (t0 shifts every load).
        self._hist_view_abs: tuple | None = None
        # True while we programmatically push Pipe/RangeX — callback must NOT
        # treat stream.x_range (stale relative secs) as a user gesture.
        self._hist_applying = False
        self._hist_built = False
        # Stable facet host — rebuilt in place so we don't depend on stream_epoch
        # (live refresh was recreating buttons every poll and eating clicks).
        self._facets_col = pn.Column(
            pn.pane.Markdown("_No facets yet_", sizing_mode="stretch_width"),
            sizing_mode="stretch_width",
            scroll=True,
        )
        self._filter_input = pn.widgets.TextInput(
            name="",  # no label — keeps the control bar on one baseline
            placeholder='field:value  e.g. action:REJECT  dstport:443',
            sizing_mode="stretch_width",
        )
        self._filter_input.param.watch(self._on_filter_submit, "value")
        self._add_btn = pn.widgets.Button(name="Add filter", button_type="primary", width=110)
        self._add_btn.on_click(self._on_add_click)
        self._clear_btn = pn.widgets.Button(name="Clear", width=70)
        self._clear_btn.on_click(self._on_clear)
        self._pause = pn.widgets.Toggle(name="Live", value=True, width=70)
        self._pause.param.watch(self._on_live_toggle, "value")
        self._window = pn.widgets.Select.from_param(self.param.time_window, name="", width=100)
        self.param.watch(self._on_window_change, "time_window")

    # -- filter UI --
    def _on_live_toggle(self, event):
        self.live = bool(event.new)

    def _on_window_change(self, event):
        # Force catalog rebind for the new sliding window; clear user viewport
        # so we re-anchor to the full new window.
        self.catalog_id = ""
        self._hist_view_abs = None
        self.status_line = f"Loading window {event.new}…"
        self.refreshing = True
        self._schedule_bg(force=True)

    def _on_clear(self, *_):
        if not self.filters:
            return
        self.filters = []
        self.error = ""
        self._bump_all()

    def _on_add_click(self, *_):
        self._parse_and_add(self._filter_input.value)
        self._filter_input.value = ""

    def _on_filter_submit(self, event):
        # Enter in some browsers fires value change with same text; only on explicit add for safety
        pass

    def _filter_exists(self, field: str, value: str, op: str = "==") -> bool:
        sv = str(value)
        for f in self.filters:
            if (
                f.get("field") == field
                and f.get("op", "==") == op
                and str(f.get("value")) == sv
            ):
                return True
        return False

    def _parse_and_add(self, text: str):
        text = (text or "").strip()
        if not text:
            return
        if ":" in text:
            field, _, value = text.partition(":")
            field, value = field.strip(), value.strip()
        elif "==" in text:
            field, _, value = text.partition("==")
            field, value = field.strip(), value.strip().strip("\"'")
        else:
            self.error = f"Bad filter: {text!r} (use field:value)"
            return
        if not field or value == "":
            self.error = f"Bad filter: {text!r} (use field:value)"
            return
        self.add_facet_filter(field, value)

    def add_facet_filter(self, field: str, value: str):
        field = (field or "").strip()
        value = str(value).strip()
        if not field or value == "":
            return
        if self._filter_exists(field, value):
            # Already applied — still refresh chips/status so the UI feels responsive.
            self.error = ""
            self._refresh_filter_bar_only()
            return
        print("[DATA-VIEW] add filter %s=%s" % (field, value), flush=True)
        self.filters = list(self.filters) + [{"field": field, "op": "==", "value": value}]
        self.error = ""
        self._bump_all()

    def remove_filter(self, idx: int):
        fl = list(self.filters)
        if 0 <= idx < len(fl):
            removed = fl.pop(idx)
            print("[DATA-VIEW] remove filter %s" % removed, flush=True)
            self.filters = fl
            self._bump_all()

    def _refresh_filter_bar_only(self):
        """Force filter chip row to repaint without recomputing data."""
        self.param.trigger("filters")

    def _bump_all(self):
        # Recompute FIRST so reactive panes that fire on token bumps see filtered _df.
        self._recompute_filtered()
        self.table_token += 1
        self.facet_token += 1
        self._update_histogram_data()
        self._rebuild_facets()
        # Ensure filter chips / status see new rows count.
        try:
            self.param.trigger("filters")
        except Exception:
            pass

    def _window_minutes(self) -> int:
        m = {"5m": 5, "15m": 15, "1h": 60, "3h": 180}
        return m.get(self.time_window, WINDOW_MIN)

    def _recompute_filtered(self):
        import data_view_lib as dvl

        with self._lock:
            raw = self._df_raw if self._df_raw is not None else pd.DataFrame()
            fl = list(self.filters)
            self._df = dvl.apply_filters(raw, fl)
            self.rows_in_window = int(len(self._df))
            if not self._df.empty and "start" in self._df.columns:
                mx = self._df["start"].max()
                self.last_event_ts = str(mx) if pd.notna(mx) else "—"
            elif self._df.empty:
                self.last_event_ts = "—"
        print(
            "[DATA-VIEW] filters=%s rows=%s/%s"
            % (len(fl), self.rows_in_window, len(raw) if raw is not None else 0),
            flush=True,
        )

    # -- discovery: cache-first paint + background refresh --
    # Measured cold path ~3.4s (glob 1.5s + sequential reads 1.75s). That must
    # NOT run on the Bokeh event loop or first paint freezes. Pattern:
    #   1) apply process cache immediately (if any)
    #   2) periodic tick on event loop only applies `_pending` results
    #   3) worker thread lists/loads and sets `_pending` when catalog moves
    def start(self):
        """Idempotent boot: kick bg I/O anytime; attach UI tick when Document exists.

        Safe to call at module import *and* from ``pn.state.onload``. Discovery
        does not need a Document; ``add_periodic_callback`` does.
        """
        print("[DATA-VIEW] start() bg_kicked=%s cb=%s" % (self._bg_kicked, self._cb is not None), flush=True)
        # Instant paint from process cache (other sessions / prior poll).
        try:
            self._try_apply_process_cache(instant=True)
        except Exception as e:
            print("[DATA-VIEW] cache apply failed: %s" % e, flush=True)
        if self._df_raw is None or getattr(self._df_raw, "empty", True):
            try:
                self.status_line = "epoch 0 · window %s · waiting for data…" % self.time_window
            except Exception:
                pass
        if not self._bg_kicked:
            self._bg_kicked = True
            try:
                self.refreshing = True
            except Exception:
                pass
            self._schedule_bg(force=True)
        # UI tick — only works once a Document is available (onload).
        if self._cb is None:
            try:
                self._cb = pn.state.add_periodic_callback(self._tick, period=400)
                print("[DATA-VIEW] periodic callback attached", flush=True)
            except Exception as e:
                print("[DATA-VIEW] periodic_callback deferred: %s" % e, flush=True)
        logger.info(
            "Data-View start cache_hit=%s cb=%s bg=%s",
            self.from_cache, self._cb is not None, self._bg_kicked,
        )

    def _try_apply_process_cache(self, instant: bool = False) -> bool:
        import data_view_lib as dvl

        hit = dvl.cache_get()
        if not hit:
            return False
        self._publish(
            df=hit["df"],
            sig=hit["sig"],
            paths=hit["paths"],
            cursor=hit["cursor"],
            load_ms=hit["load_ms"],
            from_cache=True,
            refreshing=True,
        )
        return True

    def _schedule_bg(self, force: bool = False):
        if not self.live and not force:
            return
        if self._busy:
            return
        now = time.time()
        if not force and (now - self._last_bg) < DISCOVERY_S:
            return
        self._busy = True
        self._last_bg = now
        self.refreshing = True
        threading.Thread(target=self._bg_discover, daemon=True, name="data-view-bg").start()

    def _tick(self):
        """Event-loop only: apply bg results + schedule next discover."""
        pending = None
        with self._lock:
            if self._pending is not None:
                pending = self._pending
                self._pending = None
        if pending is not None:
            if pending.get("error"):
                self.error = str(pending["error"])
                self.refreshing = False
            elif pending.get("unchanged"):
                # Catalog quiet — heartbeat only, keep current panes / stable wording.
                self.refreshing = False
                self.from_cache = False
                npaths = len(pending.get("paths") or [])
                if npaths:
                    self.files_in_window = npaths
                if pending.get("load_ms") is not None:
                    self.load_ms = float(pending["load_ms"])
                self.status_line = (
                    f"epoch {self.stream_epoch} · window {self.time_window}"
                )
            else:
                self._publish(
                    df=pending["df"],
                    sig=pending["sig"],
                    paths=pending["paths"],
                    cursor=pending.get("cursor") or {},
                    load_ms=pending.get("load_ms", 0),
                    from_cache=False,
                    refreshing=False,
                )
        if self.live:
            self._schedule_bg(force=False)

    def _bg_discover(self):
        """Worker thread: S3 list + load via process-stable data_view_lib.

        Never touches param from here. Local-import the lib so we never depend
        on the panel session module globals (which can lose ``__builtins__`` /
        names mid-flight and turn into blank histograms).
        """
        import logging as _logging
        import time as _time
        import data_view_lib as dvl

        log = _logging.getLogger("data-view")
        try:
            # Read session-owned attrs once; don't re-resolve module globals later.
            window_min = self._window_minutes()
            catalog_id = self.catalog_id
            has_data = self._df_raw is not None and not getattr(self._df_raw, "empty", True)
            skip = catalog_id if has_data else None
            result = dvl.discover(window_min, skip_if_sig=skip)
            if result.get("error"):
                with self._lock:
                    self._pending = {"error": result["error"]}
                return
            if result.get("unchanged"):
                with self._lock:
                    self._pending = {
                        "df": self._df_raw,
                        "sig": result["sig"],
                        "paths": result.get("paths") or [],
                        "cursor": result.get("cursor") or {},
                        "load_ms": result.get("load_ms", 0),
                        "unchanged": True,
                    }
                return
            with self._lock:
                self._pending = {
                    "df": result["df"],
                    "sig": result["sig"],
                    "paths": result.get("paths") or [],
                    "cursor": result.get("cursor") or {},
                    "load_ms": result.get("load_ms", 0),
                    "unchanged": False,
                }
            print(
                "[DATA-VIEW] bg pending rows=%s files=%s" % (
                    0 if result.get("df") is None else len(result["df"]),
                    len(result.get("paths") or []),
                ),
                flush=True,
            )
            # Nudge the event loop in case the periodic callback is delayed.
            try:
                pn.state.schedule_callback(self._tick, 50)
            except Exception:
                pass
        except Exception as e:
            log.exception("bg discover: %s", e)
            print("[DATA-VIEW] bg discover ERROR: %s" % e, flush=True)
            try:
                with self._lock:
                    self._pending = {"error": str(e)}
            except Exception:
                pass
        finally:
            self._busy = False

    def _publish(
        self,
        *,
        df: pd.DataFrame,
        sig: str,
        paths: list,
        cursor: dict,
        load_ms: float,
        from_cache: bool,
        refreshing: bool,
    ):
        """Apply a frame on the event loop — bumps tokens so panes repaint."""
        unchanged = sig == self.catalog_id and not from_cache
        with self._lock:
            self._df_raw = df if df is not None else pd.DataFrame()
        self.catalog_id = sig
        self._recompute_filtered()
        now = time.time()
        dt = max(now - self._last_rows_t, 0.001)
        delta = max(len(self._df_raw) - self._last_rows, 0)
        if not unchanged and not from_cache:
            self.rps_est = round(delta / dt, 1) if delta > 0 else max(self.rps_est * 0.5, 0.0)
        self._last_rows = len(self._df_raw)
        self._last_rows_t = now
        self.from_cache = from_cache
        self.refreshing = refreshing
        self.error = ""

        # Bump tokens on real data changes so table repaints.
        # Histogram is updated in-place via Pipe (preserves pan/zoom).
        # Facets rebuild on a throttle (not every stream_epoch) so clicks survive.
        if (not unchanged) or (from_cache and self.stream_epoch == 0):
            self.stream_epoch += 1
            self.table_token += 1
            self._update_histogram_data()
            if now - self._last_facet >= FACET_S or self.facet_token == 0:
                self.facet_token += 1
                self._last_facet = now
                self._rebuild_facets()

        win = self.time_window
        self.files_in_window = len(paths or [])
        self.load_ms = float(load_ms or 0)
        nfilt = len(self.filters)
        filt_bit = (" · %d filter%s" % (nfilt, "s" if nfilt != 1 else "")) if nfilt else ""
        # Status text holds epoch/window only — files/rows/load live in fixed slots.
        if self._df_raw is None or self._df_raw.empty:
            self.status_line = "epoch %s · window %s · waiting for data…" % (
                self.stream_epoch, win,
            )
        else:
            self.status_line = "epoch %s · window %s%s" % (
                self.stream_epoch, win, filt_bit,
            )

    # -- panes --
    @param.depends("filters", "error")
    def filter_bar(self):
        chips = []
        for i, f in enumerate(self.filters):
            label = "%s:%s" % (f.get("field"), f.get("value"))
            btn = pn.widgets.Button(
                name="× %s" % label,
                button_type="light",
                width=max(120, 10 * len(label) + 24),
            )
            def _rm(event, idx=i):
                self.remove_filter(idx)
            btn.on_click(_rm)
            chips.append(btn)
        err = (
            pn.pane.Alert(self.error, alert_type="warning")
            if self.error
            else pn.Spacer(height=0)
        )
        return pn.Column(
            pn.Row(
                self._filter_input,
                self._add_btn,
                self._clear_btn,
                self._window,
                self._pause,
                sizing_mode="stretch_width",
            ),
            pn.Row(*chips, sizing_mode="stretch_width") if chips else pn.Spacer(height=0),
            err,
            sizing_mode="stretch_width",
        )

    def histogram_pane(self):
        """Stable container — plot is built once and fed via Pipe so pan/zoom survives refresh."""
        return self._hist_view

    def _prepare_hist_frame(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Timestamp, float, str, bool]:
        """Return (plot_df with t_sec/metric/y, t0, t_max, ylabel, use_sum)."""
        plot_df = df.dropna(subset=["start"]).copy()
        plot_df["start"] = pd.to_datetime(plot_df["start"], utc=True, errors="coerce")
        plot_df = plot_df.dropna(subset=["start"])
        if plot_df.empty:
            return plot_df, pd.Timestamp.now(tz="UTC"), 1.0, "value", False
        if "bytes" in plot_df.columns:
            # Bytes are non-negative; clamp so Σ never paints below zero.
            plot_df["metric"] = (
                pd.to_numeric(plot_df["bytes"], errors="coerce").fillna(0.0).clip(lower=0.0)
            )
            ylabel, use_sum = "bytes (Σ)", True
        else:
            plot_df["metric"] = 1.0
            ylabel, use_sum = "flows", False
        t0 = plot_df["start"].min()
        plot_df = plot_df.assign(
            t_sec=(plot_df["start"] - t0).dt.total_seconds().astype("float64"),
            y=0.0,
        )
        # Drop any rows that somehow land before the origin (clock skew / tz).
        plot_df = plot_df[plot_df["t_sec"] >= 0.0]
        if plot_df.empty:
            return plot_df, t0, 1.0, ylabel, use_sum
        t_max = float(plot_df["t_sec"].max()) if len(plot_df) else 1.0
        if t_max <= 0:
            t_max = 1.0
        return plot_df, t0, t_max, ylabel, use_sum

    def _clamp_xr(self, lo: float, hi: float, t_max: float) -> tuple[float, float]:
        """Hard-clamp viewport to [0, t_max] — never show negative time."""
        t_max = max(float(t_max), 1e-3)
        try:
            lo = float(lo)
        except (TypeError, ValueError):
            lo = 0.0
        try:
            hi = float(hi)
        except (TypeError, ValueError):
            hi = t_max
        if not np.isfinite(lo):
            lo = 0.0
        if not np.isfinite(hi):
            hi = t_max
        if hi < lo:
            lo, hi = hi, lo
        span = max(hi - lo, 1e-3)
        if span >= t_max - 1e-9:
            return (0.0, t_max)
        # Hit walls while preserving span when possible.
        if lo < 0.0:
            lo, hi = 0.0, min(t_max, span)
        if hi > t_max:
            hi, lo = t_max, max(0.0, t_max - span)
        if lo < 0.0:
            lo = 0.0
        if hi - lo < 1e-6:
            hi = min(t_max, lo + 1.0)
            lo = max(0.0, hi - 1.0)
        return (float(lo), float(hi))

    def _as_sec(self, v, t0: pd.Timestamp) -> float | None:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        if isinstance(v, np.datetime64):
            v = pd.Timestamp(v)
        if isinstance(v, datetime) and not isinstance(v, pd.Timestamp):
            v = pd.Timestamp(v)
        if isinstance(v, pd.Timestamp):
            t0s = pd.Timestamp(t0)
            if t0s.tzinfo is not None and v.tzinfo is None:
                v = v.tz_localize("UTC")
            elif t0s.tzinfo is None and v.tzinfo is not None:
                v = v.tz_convert("UTC").tz_localize(None)
            return float((v - t0s).total_seconds())
        try:
            fv = float(v)
        except (TypeError, ValueError):
            return None
        if abs(fv) > 1e12:
            try:
                unit = "ns" if abs(fv) > 1e15 else "ms"
                ts = pd.Timestamp(fv, unit=unit, tz="UTC")
                t0s = pd.Timestamp(t0)
                if t0s.tzinfo is None:
                    t0s = t0s.tz_localize("UTC")
                return float((ts - t0s).total_seconds())
            except Exception:
                return None
        return fv

    def _range_from_abs(self, t0: pd.Timestamp, t_max: float) -> tuple[float, float]:
        """Convert stored absolute viewport → seconds relative to current t0."""
        if self._hist_view_abs is None or t0 is None:
            return (0.0, float(t_max))
        a0, a1 = self._hist_view_abs
        lo = self._as_sec(a0, t0)
        hi = self._as_sec(a1, t0)
        if lo is None or hi is None:
            return (0.0, float(t_max))
        return self._clamp_xr(lo, hi, t_max)

    def _range_from_stream(self, x_range, t0: pd.Timestamp, t_max: float) -> tuple[float, float]:
        """Interpret RangeX payload (user gesture) as seconds in [0, t_max]."""
        if x_range is not None and x_range[0] is not None and x_range[1] is not None:
            lo, hi = self._as_sec(x_range[0], t0), self._as_sec(x_range[1], t0)
            if lo is not None and hi is not None:
                return self._clamp_xr(lo, hi, t_max)
        return self._range_from_abs(t0, t_max)

    def _store_abs_viewport(self, xr: tuple[float, float], t0: pd.Timestamp, t_max: float) -> None:
        """Persist user viewport in absolute time. Full window → None (follow live)."""
        lo, hi = float(xr[0]), float(xr[1])
        # Near-full range means user is following the live window (or hit reset).
        if lo <= 0.05 and hi >= float(t_max) - 0.05:
            self._hist_view_abs = None
            return
        try:
            t0s = pd.Timestamp(t0)
            self._hist_view_abs = (
                t0s + pd.to_timedelta(lo, unit="s"),
                t0s + pd.to_timedelta(hi, unit="s"),
            )
        except Exception:
            pass

    def _hist_n_bins(self, span: float, t_max: float) -> int:
        """Choose bin count so time windows stay visually distinct.

        Target ~2s per bin on a full window; more bins when zoomed in.
        """
        span = max(float(span), 1.0)
        t_max = max(float(t_max), span)
        # Zoomed view: denser bins (down to ~0.5s); full window: ~2s bins.
        target_w = 0.5 if span < t_max * 0.35 else 2.0
        n = int(round(span / target_w))
        return int(np.clip(n, 48, 200))

    def _hist_empty_plot(self, title: str = ""):
        opts = dict(
            height=160,
            responsive=True,
            xlim=(0, 1),
            ylim=(0, 1),
            bgcolor="#0d1117",
            color="#58a6ff",
            line_color="#0d1117",
        )
        if title:
            opts["title"] = title
        # Rectangles: (x0, y0, x1, y1) — single empty bin with gap styling.
        return hv.Rectangles([(0.05, 0.0, 0.95, 0.0)], ["x0", "y0", "x1", "y1"]).opts(**opts)

    def _hist_callback(self, data, x_range):
        """DynamicMap callback: (plot_df, meta) + RangeX → gapped time bars."""
        try:
            return self._hist_callback_inner(data, x_range)
        except Exception as e:
            # DynamicMap swallows errors as a blank pane — log and return a stub.
            try:
                logger.exception("hist callback: %s", e)
            except Exception:
                pass
            return self._hist_empty_plot(title="histogram error (see logs)")

    def _hist_callback_inner(self, data, x_range):
        if not data or data.get("df") is None or data["df"].empty:
            return self._hist_empty_plot()

        plot_df = data["df"]
        t0 = data["t0"]
        t_max = float(data["t_max"])
        ylabel = data["ylabel"]
        use_sum = data["use_sum"]

        # Critical split:
        #  - Programmatic Pipe/RangeX push (live refresh): absolute viewport wins.
        #    Stream holds *relative* secs from the previous t0 and must be ignored
        #    or every discover poll rewrites the user's zoom to garbage / full range.
        #  - User pan/zoom: RangeX is authoritative; persist as absolute timestamps.
        if self._hist_applying:
            xr = self._range_from_abs(t0, t_max)
        else:
            xr = self._range_from_stream(x_range, t0, t_max)
            self._store_abs_viewport(xr, t0, t_max)
        xr = self._clamp_xr(xr[0], xr[1], t_max)
        span = max(xr[1] - xr[0], 1e-3)
        n_bins = self._hist_n_bins(span, t_max)

        sub = plot_df[(plot_df["t_sec"] >= xr[0]) & (plot_df["t_sec"] <= xr[1])]
        if sub.empty:
            edges = np.linspace(xr[0], xr[1], n_bins + 1)
            vals = np.zeros(n_bins, dtype="float64")
        elif _HAS_DS:
            cvs = dsh.Canvas(
                plot_width=n_bins,
                plot_height=1,
                x_range=xr,
                y_range=(-0.5, 0.5),
            )
            agg = (
                cvs.points(sub, "t_sec", "y", agg=dsh.sum("metric"))
                if use_sum
                else cvs.points(sub, "t_sec", "y", agg=dsh.count())
            )
            vals = np.nan_to_num(np.asarray(agg, dtype="float64").ravel(), nan=0.0)
            # Force edge grid to match requested n_bins (agg width can differ by 1).
            if len(vals) != n_bins:
                n_bins = max(len(vals), 1)
            edges = np.linspace(xr[0], xr[1], num=n_bins + 1)
            if len(vals) != n_bins:
                vals = np.resize(vals, n_bins)
        else:
            vals, edges = np.histogram(
                sub["t_sec"].to_numpy(),
                bins=n_bins,
                range=xr,
                weights=sub["metric"].to_numpy() if use_sum else None,
            )
            vals = np.asarray(vals, dtype="float64")

        # Σ bytes / counts are non-negative — never let float noise paint below 0.
        vals = np.maximum(np.asarray(vals, dtype="float64"), 0.0)
        edges = np.asarray(edges, dtype="float64")
        if len(edges) < 2 or not np.all(np.diff(edges) > 0):
            edges = np.linspace(xr[0], max(xr[1], xr[0] + 1.0), num=n_bins + 1)
            vals = np.zeros(len(edges) - 1, dtype="float64")
        if len(vals) != len(edges) - 1:
            vals = np.resize(vals, len(edges) - 1)

        # Gapped rectangles so adjacent time bins stay visually distinct.
        # hv.Histogram/Quad paints flush edges → solid wall when rates are even.
        bin_w = np.diff(edges)
        gap = np.minimum(bin_w * 0.18, np.maximum(bin_w * 0.05, 0.05))
        x0s = edges[:-1] + gap * 0.5
        x1s = edges[1:] - gap * 0.5
        # Keep a minimum bar width when bins are very narrow.
        too_thin = (x1s - x0s) < (bin_w * 0.4)
        if np.any(too_thin):
            mid = (edges[:-1] + edges[1:]) * 0.5
            half = bin_w * 0.4
            x0s = np.where(too_thin, mid - half, x0s)
            x1s = np.where(too_thin, mid + half, x1s)
        y0s = np.zeros_like(vals)
        y1s = vals
        y_hi = max(float(np.max(vals)) * 1.12, 1.0) if len(vals) else 1.0
        t0s = pd.Timestamp(t0)
        origin = t0s.strftime("%H:%M:%S")

        def _axis_hook(plot, element):
            """Force non-negative axes: time ∈ [0, t_max], value ∈ [0, ∞).

            Bokeh DataRange1d defaults to range_padding≈0.1, which pads *below*
            the data min — so a min of 0 becomes a visible negative axis. That
            is wrong for both elapsed-seconds (x) and Σ bytes (y).
            """
            try:
                from bokeh.models import CustomJS

                x_rng = plot.handles.get("x_range")
                y_rng = plot.handles.get("y_range")
                tmax = float(self._hist_t_max)

                if x_rng is not None:
                    try:
                        if hasattr(x_rng, "range_padding"):
                            x_rng.range_padding = 0
                        if hasattr(x_rng, "follow"):
                            x_rng.follow = None
                    except Exception:
                        pass
                    x_rng.bounds = (0.0, tmax)
                    x_rng.min_interval = 0.5
                    x_rng.max_interval = max(tmax, 0.5)
                    try:
                        x_rng.reset_start = 0.0
                        x_rng.reset_end = tmax
                    except Exception:
                        pass
                    if not getattr(x_rng, "_data_view_js_clamp", False):
                        cb = CustomJS(
                            args=dict(xr=x_rng),
                            code="""
                            const tmin = 0.0;
                            const b = xr.bounds;
                            const tmax = (b && b.length === 2) ? b[1] : xr.end;
                            let s = xr.start, e = xr.end;
                            if (e < s) { const t = s; s = e; e = t; }
                            let span = e - s;
                            if (!(span > 0)) span = 1.0;
                            if (span >= tmax) { s = tmin; e = tmax; }
                            else {
                              if (s < tmin) { s = tmin; e = Math.min(tmax, s + span); }
                              if (e > tmax) { e = tmax; s = Math.max(tmin, e - span); }
                              if (s < tmin) s = tmin;
                            }
                            if (s !== xr.start || e !== xr.end) {
                              xr.setv({start: s, end: e});
                            }
                            """,
                        )
                        x_rng.js_on_change("start", cb)
                        x_rng.js_on_change("end", cb)
                        x_rng._data_view_js_clamp = True
                    if not getattr(x_rng, "_data_view_py_clamp", False):
                        def _snap_x(attr, old, new):
                            if self._hist_applying:
                                return
                            try:
                                s, e = float(x_rng.start), float(x_rng.end)
                                ns, ne = self._clamp_xr(s, e, float(self._hist_t_max))
                                if abs(ns - s) > 1e-6 or abs(ne - e) > 1e-6:
                                    x_rng.update(start=ns, end=ne)
                                if self._hist_t0 is not None:
                                    self._store_abs_viewport(
                                        (ns, ne), self._hist_t0, float(self._hist_t_max)
                                    )
                            except Exception:
                                pass

                        x_rng.on_change("start", _snap_x)
                        x_rng.on_change("end", _snap_x)
                        x_rng._data_view_py_clamp = True
                    s, e = float(x_rng.start), float(x_rng.end)
                    ns, ne = self._clamp_xr(s, e, tmax)
                    if abs(ns - s) > 1e-6 or abs(ne - e) > 1e-6:
                        x_rng.update(start=ns, end=ne)

                if y_rng is not None:
                    try:
                        if hasattr(y_rng, "range_padding"):
                            y_rng.range_padding = 0
                    except Exception:
                        pass
                    try:
                        y_rng.bounds = (0.0, None)
                    except Exception:
                        pass
                    try:
                        if float(y_rng.start) < 0.0:
                            y_rng.start = 0.0
                    except Exception:
                        pass
                    if not getattr(y_rng, "_data_view_y_clamp", False):
                        def _snap_y(attr, old, new):
                            try:
                                if float(y_rng.start) < 0.0:
                                    y_rng.start = 0.0
                            except Exception:
                                pass

                        y_rng.on_change("start", _snap_y)
                        y_rng._data_view_y_clamp = True
                        y_cb = CustomJS(
                            args=dict(yr=y_rng),
                            code="if (yr.start < 0) { yr.start = 0; }",
                        )
                        y_rng.js_on_change("start", y_cb)
            except Exception:
                pass

        # Explicit (x0,y0,x1,y1) bars with gutters — readable time resolution.
        x_lo = max(0.0, float(xr[0]))
        x_hi = max(x_lo + 1e-3, float(xr[1]))
        rects = hv.Rectangles(
            (x0s, y0s, x1s, y1s),
            kdims=["x0", "y0", "x1", "y1"],
        ).opts(
            height=160,
            responsive=True,
            color="#58a6ff",
            line_color="#1f6feb",
            line_width=1,
            alpha=0.92,
            tools=["hover", "xpan", "xwheel_zoom", "reset"],
            active_tools=["xwheel_zoom"],
            xlabel="seconds from %s UTC" % origin,
            ylabel=ylabel,
            bgcolor="#0d1117",
            shared_axes=False,
            default_tools=["xpan", "xwheel_zoom", "box_zoom", "reset", "save"],
            fontsize={"labels": 10, "xticks": 9, "yticks": 9},
            xlim=(x_lo, x_hi),
            ylim=(0.0, y_hi),
            hooks=[_axis_hook],
        )
        # Soft domain on x0/x1 so reset stays in [0, t_max].
        return rects.redim.range(x0=(0.0, float(t_max)), x1=(0.0, float(t_max)))

    def _ensure_histogram_widget(self):
        """Build the DynamicMap once (Pipe for data, RangeX for viewport)."""
        if self._hist_built or not _HAS_HV:
            return
        self._hist_pipe = hv.streams.Pipe(data={
            "df": pd.DataFrame(),
            "t0": pd.Timestamp.now(tz="UTC"),
            "t_max": 1.0,
            "ylabel": "value",
            "use_sum": False,
        })
        self._hist_stream = hv.streams.RangeX(x_range=(0.0, 1.0))
        dmap = hv.DynamicMap(self._hist_callback, streams=[self._hist_pipe, self._hist_stream])
        hint = pn.pane.HTML(
            "<div style='font-size:11px;color:#8b949e;"
            "font-family:-apple-system,sans-serif;margin:0 0 4px 0;'>"
            "Time histogram · gapped bins · Σ bytes · "
            "wheel-zoom / pan rebins · viewport preserved across live refresh"
            "</div>",
        )
        self._hist_view.objects = [
            hint,
            pn.pane.HoloViews(dmap, sizing_mode="stretch_width", height=180),
        ]
        self._hist_built = True

    def _update_histogram_data(self):
        """Push new frame into the Pipe; restore absolute user viewport if any.

        Live refresh must NOT clobber the user's pan/zoom. t0 (window origin)
        shifts every load, so relative RangeX seconds are meaningless across
        refreshes — only ``_hist_view_abs`` (UTC) is stable.
        """
        df = self._df
        if df is None or df.empty or "start" not in getattr(df, "columns", []):
            # Keep abs viewport; only tear down the plot chrome so next paint
            # can restore the same target range once data returns.
            self._hist_view.objects = [
                pn.pane.Markdown(
                    "### Time histogram\n_No data in window — start vpc-flow-generator stream._",
                    sizing_mode="stretch_width",
                )
            ]
            self._hist_built = False
            self._hist_pipe = None
            self._hist_stream = None
            return

        if not _HAS_HV:
            self._hist_view.objects = [
                pn.pane.Markdown(
                    f"**{len(df):,}** flows (histogram backend unavailable)",
                    sizing_mode="stretch_width",
                )
            ]
            return

        try:
            plot_df, t0, t_max, ylabel, use_sum = self._prepare_hist_frame(df)
            if plot_df.empty:
                return
            self._hist_t0 = t0
            self._hist_t_max = t_max
            self._ensure_histogram_widget()
            payload = {
                "df": plot_df,
                "t0": t0,
                "t_max": t_max,
                "ylabel": ylabel,
                "use_sum": use_sum,
            }
            # Absolute → relative under the NEW t0. Never trust stream.x_range
            # here (it is relative to the previous origin).
            xr = self._range_from_abs(t0, t_max)
            self._hist_applying = True
            try:
                # Set range first so a subsequent pipe-triggered callback that
                # somehow sees the stream still gets a consistent pair; the
                # applying flag forces abs either way.
                if self._hist_stream is not None:
                    self._hist_stream.event(x_range=xr)
                self._hist_pipe.send(payload)
            finally:
                self._hist_applying = False
        except Exception as e:
            logger.exception("hist update: %s", e)
            self._hist_applying = False
            self._hist_view.objects = [
                pn.pane.Alert(f"Histogram error: {e}", alert_type="warning")
            ]
            self._hist_built = False

    def facets_pane(self):
        """Stable container — contents replaced by ``_rebuild_facets``."""
        return self._facets_col

    def _rebuild_facets(self):
        """Rebuild left-nav field chips from the *filtered* frame.

        Uses a stable Column host so live stream_epoch bumps do not tear down
        buttons mid-click. Counts reflect the current filter set (ELK-style).
        """
        df = self._df
        if df is None or df.empty:
            self._facets_col.objects = [
                pn.pane.Markdown("_No facets yet_", sizing_mode="stretch_width"),
            ]
            return

        sections = [
            pn.pane.Markdown("### Fields", margin=(0, 0, 4, 0)),
            pn.pane.Markdown(
                "_Click a value to filter · counts are for the current result set_",
                styles={"font-size": "11px", "color": "#8b949e"},
                margin=(0, 0, 8, 0),
            ),
        ]
        work = df
        if len(work) > 50_000:
            work = work.sample(n=50_000, random_state=0)

        active = {(str(f.get("field")), str(f.get("value"))) for f in self.filters}

        for field in FACET_FIELDS:
            if field not in work.columns:
                continue
            vc = work[field].astype(str).value_counts().head(8)
            total = max(int(vc.sum()), 1)
            rows = [pn.pane.Markdown("**%s**" % field, margin=(8, 0, 2, 0))]
            for val, cnt in vc.items():
                pct = 100.0 * cnt / total
                vstr = str(val)
                on = (field, vstr) in active
                label = "%s%s  %s (%.0f%%)" % (
                    "● " if on else "",
                    vstr,
                    f"{int(cnt):,}",
                    pct,
                )
                b = pn.widgets.Button(
                    name=label,
                    button_type="primary" if on else "light",
                    sizing_mode="stretch_width",
                    styles={"text-align": "left", "font-size": "12px"},
                )
                # Bind with defaults so the closure captures this iteration.
                def _click(event, f=field, v=vstr, already=on):
                    if already:
                        # Toggle off: remove matching filter(s).
                        fl = [
                            x for x in self.filters
                            if not (
                                x.get("field") == f
                                and str(x.get("value")) == v
                            )
                        ]
                        if len(fl) != len(self.filters):
                            self.filters = fl
                            self._bump_all()
                    else:
                        self.add_facet_filter(f, v)
                b.on_click(_click)
                rows.append(b)
            sections.extend(rows)

        self._facets_col.objects = sections

    @param.depends("table_token")
    def table_pane(self):
        # Only table_token — not stream_epoch. Live publish already bumps
        # table_token on data change; filter bumps it via _bump_all.
        df = self._df
        cols = [
            c for c in [
                "start", "srcaddr", "dstaddr", "srcport", "dstport",
                "protocol", "action", "bytes", "packets", "flow_direction", "vpc_id",
            ] if df is not None and c in df.columns
        ]
        if df is None or df.empty or not cols:
            return pn.pane.Markdown("_No documents in window_", sizing_mode="stretch_both")
        show = (
            df.sort_values("start", ascending=False).head(TABLE_ROWS)
            if "start" in df.columns
            else df.head(TABLE_ROWS)
        )
        show = show[cols].copy()
        if "start" in show.columns:
            show["start"] = show["start"].astype(str)
        return pn.widgets.Tabulator(
            show,
            pagination="remote",
            page_size=20,
            sizing_mode="stretch_both",
            theme="midnight",
            layout="fit_data_stretch",
        )

    @param.depends(
        "status_line", "rps_est", "rows_in_window", "files_in_window",
        "load_ms", "stream_epoch", "live", "last_event_ts", "filters",
    )
    def status_strip(self):
        # Keep badge text stable (LIVE / PAUSED only) so the strip doesn't
        # jump when background refresh flips loading/cache states.
        # Fixed slots for files/rows/load so stats never vanish between polls.
        if self.live:
            live_col, live_txt = "#3fb950", "● LIVE"
        else:
            live_col, live_txt = "#d29922", "○ PAUSED"
        nfilt = len(self.filters)
        filt_html = (
            "<span>filters: <b>%d</b></span>" % nfilt if nfilt else ""
        )
        return pn.pane.HTML(
            "<div style='display:flex;gap:18px;align-items:center;font-size:12px;"
            "color:#c9d1d9;font-family:-apple-system,BlinkMacSystemFont,sans-serif;'>"
            "<span style='color:%s;font-weight:700;'>%s</span>"
            "<span>%s</span>"
            "<span>files: <b>%s</b></span>"
            "<span>rows: <b>%s</b></span>"
            "%s"
            "<span>load: <b>%.0fms</b></span>"
            "<span>≈ %.0f Δrows/s</span>"
            "<span>last event: %s</span>"
            "</div>" % (
                live_col,
                live_txt,
                self.status_line,
                f"{self.files_in_window:,}",
                f"{self.rows_in_window:,}",
                filt_html,
                self.load_ms,
                self.rps_est,
                self.last_event_ts,
            ),
            sizing_mode="stretch_width",
        )

    def layout(self):
        # pn.panel() wraps @param.depends methods so token bumps repaint the DOM.
        main = pn.Column(
            pn.pane.HTML(NAV_HTML),
            pn.panel(self.status_strip),
            pn.panel(self.filter_bar),
            pn.layout.Divider(),
            self.histogram_pane(),
            pn.layout.Divider(),
            pn.pane.Markdown("### Documents", margin=(0, 0, 4, 0)),
            pn.panel(self.table_pane),
            sizing_mode="stretch_both",
            min_height=700,
        )
        side = pn.Column(
            pn.pane.Markdown("# Data-View", margin=(0, 0, 6, 0)),
            pn.pane.Markdown(
                "**Prefix** `%s/`  \n"
                "**Bucket** `%s`  \n"
                "**Window** sliding (see control)"
                % (VPC_FLOW_PREFIX, S3_BUCKET or "—"),
                styles={"font-size": "11px", "color": "#8b949e"},
            ),
            pn.layout.Divider(),
            self.facets_pane(),
            width=280,
            sizing_mode="fixed",
            scroll=True,
        )
        return pn.template.FastListTemplate(
            title="Data-View · VPC Flow",
            sidebar=[side],
            main=[main],
            accent_base_color="#0072B5",
            header_background="#161b22",
            sidebar_width=300,
            theme="dark",
            font=_AIRGAP_UI_FONT,
            font_url="",
        )


# -------------------------------------------------------------------------
# Serve — one DataView per browser session (script re-executes per session)
# -------------------------------------------------------------------------

view = DataView()
tmpl = view.layout()
# Module-level: kick bg discover immediately (no Document required).
# onload: attach periodic UI tick once the Bokeh Document exists so pending
# frames paint into the histogram.
view.start()
pn.state.onload(view.start)
tmpl.servable()
print("[DATA-VIEW] App ready", flush=True)
