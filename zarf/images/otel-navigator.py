"""OTEL Span Explorer - Auto-loading with live Dask status.

Production Panel application for visualizing OTEL trace data with HoloViews + Datashader.

Key architecture:
- Auto-loads data on page render (no button needed)
- Live status shows Dask connection, loading, and task activity
- Zoom/pan triggers background Dask work with visible progress
- Fixed-size heatmap that doesn't collapse
- Smart windowing: caps partitions at MAX_PARTITIONS to keep Dask graph small

Environment variables:
- DASK_SCHEDULER: Dask scheduler address (required)
- S3_BUCKET: S3 bucket name — REQUIRED, provided at deploy time (never hardcoded)
- OTEL_DATA_PATH: fallback base only; the active dataset is discovered from
  s3://{S3_BUCKET}/_active_dataset.json (the dataset name is never baked in)
- S3_ENDPOINT: S3 endpoint for local RustFS / S3-compatible gateway (optional)
- AWS_ACCESS_KEY_ID: S3 access key
- AWS_SECRET_ACCESS_KEY: S3 secret key
- AWS_REGION: AWS region (default: us-east-1)
- PTY_PROXY_WS: optional explicit terminal WebSocket URL (empty = auto-detect)
- BOKEH_RESOURCES: set to "server" for air-gap panel serve (never cdn.bokeh.org)
"""
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone

# Air-gap: force Bokeh assets from the Panel server before any bokeh/panel import.
# "server" = pod-local static paths; never https://cdn.bokeh.org/
os.environ.setdefault("BOKEH_RESOURCES", "server")

import colorcet as cc
import dask.dataframe as dd
import holoviews as hv
import hvplot.dask  # noqa: F401 - required for hvplot extension
import panel as pn
import param
from dask.distributed import Client
from holoviews.operation.datashader import rasterize, dynspread

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# Air-gap: Panel Fast design hard-codes Open Sans via fonts.googleapis.com in
# Fast._resources['font']. Template font_url="" alone does NOT remove that link.
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

hv.extension('bokeh')
pn.extension(loading_spinner='dots', loading_color='#0072B5',
             js_files={'ghostty-loader': '/ghostty/loader.js'})

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------

DASK_SCHEDULER = os.environ.get('DASK_SCHEDULER', '')
# The S3 bucket is PROVIDED AT DEPLOY TIME (the air-gap operator supplies it) — it is
# never hardcoded. Empty here means "not provided"; the app surfaces a clear error
# rather than silently reading a wrong bucket.
S3_BUCKET = os.environ.get('S3_BUCKET', '')
# Fallback base only. The active dataset is DISCOVERED from <bucket>/_active_dataset.json
# (see get_active_dataset) — the dataset name is never baked in here.
OTEL_DATA_PATH = os.environ.get('OTEL_DATA_PATH', f's3://{S3_BUCKET}/' if S3_BUCKET else '')
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', '')
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
AWS_SESSION_TOKEN = os.environ.get('AWS_SESSION_TOKEN', '')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Fixed canvas size to prevent collapse
CANVAS_WIDTH = 800
CANVAS_HEIGHT = 500
MIN_MAIN_HEIGHT = 600

# Terminal / PTY proxy
TERMINAL_WS_URL = os.environ.get('PTY_PROXY_WS', '')
TERMINAL_HEIGHT = 280

# Top-bar multi-app nav (panel serve multi-route). Labels are short so they
# don't echo the FastListTemplate title ("OTEL Navigator").
NAV_HTML = """
<div style="display:flex;gap:16px;align-items:center;font-family:-apple-system,BlinkMacSystemFont,sans-serif;font-size:13px;">
  <a href="/otel-navigator" style="color:#fff;text-decoration:none;font-weight:700;border-bottom:2px solid #58a6ff;padding-bottom:2px;">Metrics</a>
  <a href="/data-view" style="color:#c9d1d9;text-decoration:none;">Data-View</a>
</div>
"""

# -------------------------------------------------------------------------
# Dask Client (lazy singleton)
# -------------------------------------------------------------------------

_dask_client = None


def get_dask_client() -> Client:
    global _dask_client
    if _dask_client is not None:
        return _dask_client
    if not DASK_SCHEDULER:
        raise RuntimeError("DASK_SCHEDULER not set")
    _dask_client = Client(DASK_SCHEDULER, timeout='30s')
    return _dask_client


def get_dask_stats() -> dict:
    """Get current Dask cluster stats."""
    try:
        client = get_dask_client()
        info = client.scheduler_info()
        workers = len(info.get('workers', {}))
        processing = sum(
            len(w.get('processing', {}))
            for w in info.get('workers', {}).values()
        )
        return {'workers': workers, 'processing': processing, 'connected': True}
    except Exception:
        return {'workers': 0, 'processing': 0, 'connected': False}


# -------------------------------------------------------------------------
# S3 Storage
# -------------------------------------------------------------------------

def get_storage_options() -> dict:
    # When explicit credentials are provided (e.g. RustFS / local S3), use them.
    # When AWS_SESSION_TOKEN is set (IAM role / STS), include it.
    # When no credentials are set, omit key/secret so botocore uses its
    # default credential chain (instance role, env vars, config files).
    opts = {}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        opts['key'] = AWS_ACCESS_KEY_ID
        opts['secret'] = AWS_SECRET_ACCESS_KEY
        if AWS_SESSION_TOKEN:
            opts['token'] = AWS_SESSION_TOKEN
    if S3_ENDPOINT:
        opts['client_kwargs'] = {'endpoint_url': S3_ENDPOINT}
        # On-prem S3-compatible gateways (RustFS et al.) reached by IP/hostname
        # reject AWS virtual-hosted addressing (bucket.endpoint); force
        # path-style (endpoint/bucket) + SigV4. No-op for real AWS S3, which
        # never sets S3_ENDPOINT. This storage_options dict is also serialized
        # to the Dask workers via dd.read_parquet, so the workers inherit it.
        opts['config_kwargs'] = {'s3': {'addressing_style': 'path'},
                                 'signature_version': 's3v4'}
    if AWS_REGION:
        opts.setdefault('client_kwargs', {})
        opts['client_kwargs']['region_name'] = AWS_REGION
    return {k: v for k, v in opts.items() if v}


# -------------------------------------------------------------------------
# Dataset Detection (two-phase: minimal -> large)
# -------------------------------------------------------------------------

def get_active_dataset() -> dict:
    """Check which dataset is active/available."""
    import json
    import s3fs

    bucket = S3_BUCKET

    try:
        fs = s3fs.S3FileSystem(**get_storage_options())
        marker_path = f"{bucket}/_active_dataset.json"
        with fs.open(marker_path, 'r') as f:
            marker = json.load(f)
        dataset = marker.get('dataset')
        if not dataset:
            raise ValueError("_active_dataset.json present but has no 'dataset' key")
        return {
            'dataset': dataset,
            'phase': marker.get('phase', 'unknown'),
            'path': f"s3://{bucket}/{dataset}/",
            'total_spans': marker.get('total_spans'),
        }
    except Exception as e:
        # Do NOT silently fall back to a hardcoded dataset — that masks the real cause,
        # which is almost always a wrong or unset S3_BUCKET (given at deploy time).
        logger.error(
            "Cannot resolve active dataset from s3://%s/_active_dataset.json: %s. "
            "Verify S3_BUCKET — it is provided at deploy time and never hardcoded.",
            bucket or "<unset>", e)

    return {
        'dataset': '(unresolved)',
        'phase': 'error',
        'path': OTEL_DATA_PATH,
        'total_spans': None,
    }


# -------------------------------------------------------------------------
# Data Loader (smart windowing — explicit paths, capped partitions)
# -------------------------------------------------------------------------

_file_list_cache = {}

# Target partition count — balances graph size vs visual fidelity.
# Working version (c42866b1) used ~132 partitions.
# Each partition ~ 2M rows x 16 bytes (2 cols) ~ 32 MB in memory.
# 200 partitions -> ~400 KB graph, ~6.4 GiB total (distributed across workers).
MAX_PARTITIONS = 200


def load_span_data(start_time: datetime, end_time: datetime, data_path: str = None, on_progress=None) -> dd.DataFrame:
    """Load span data with smart windowing: glob, sort by recency, cap partitions.

    Uses explicit file paths (no Hive discovery) to avoid KeyError from mixed
    partition layouts (otel-1t: shard=/date=/ vs otel-minimal: date=/hour=/).
    Caps at MAX_PARTITIONS files via even sampling to keep Dask graph small.
    """
    import s3fs

    s3_base = (data_path or OTEL_DATA_PATH).rstrip('/')
    if not s3_base.endswith('/spans'):
        s3_base = f"{s3_base}/spans"

    if start_time is not None:
        start_date, end_date = start_time.date(), end_time.date()
        date_strings = []
        current = start_date
        while current <= end_date:
            date_strings.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
    else:
        date_strings = None  # All Data mode: skip date filtering

    if on_progress:
        if date_strings is not None:
            on_progress(f"Scanning {len(date_strings)} days...")
        else:
            on_progress("Loading all available data...")

    # --- File discovery (cached) ---
    cache_key = s3_base
    if cache_key not in _file_list_cache:
        fs = s3fs.S3FileSystem(**get_storage_options())
        s3_path = s3_base.replace('s3://', '')

        if on_progress:
            on_progress("Building file index (first load, will be cached)...")

        # Try layouts in order of expected size (largest first)
        files = fs.glob(f"{s3_path}/shard=*/date=*/batch_*.parquet")      # otel-1t
        if not files:
            files = fs.glob(f"{s3_path}/shard=*/date=*/hour=*/*.parquet")  # otel-1t old
        if not files:
            files = fs.glob(f"{s3_path}/date=*/hour=*/*.parquet")          # otel-minimal/large
        if not files:
            files = fs.glob(f"{s3_path}/date=*/*.parquet")                 # flat

        all_files = [f"s3://{f}" for f in files]
        _file_list_cache[cache_key] = all_files
        if on_progress:
            on_progress(f"Indexed {len(all_files)} files")
    else:
        all_files = _file_list_cache[cache_key]

    # --- Anchor relative window to the dataset's latest date ---
    # Relative presets ("Last 24 Hours") are computed against wall-clock now(),
    # but OTEL data is often historical / batch-loaded (on-prem S3 gateways
    # especially). When the requested window ends after the most recent data,
    # slide it back so it ends at the latest data date, preserving the span --
    # so "Last 24 Hours" always returns the 24h ending at the newest records.
    # No-op for live/current data; preferred over the all-data fallback below.
    if date_strings is not None and all_files:
        import re as _re
        _data_dates = sorted({
            _m.group(1) for _f in all_files
            for _m in [_re.search(r'date=(\d{4}-\d{2}-\d{2})', _f)] if _m
        })
        if _data_dates:
            _max_date = datetime.strptime(_data_dates[-1], '%Y-%m-%d').date()
            if end_date > _max_date:
                _span = end_date - start_date
                end_date = _max_date
                start_date = _max_date - _span
                date_strings = []
                _cur = start_date
                while _cur <= end_date:
                    date_strings.append(_cur.strftime('%Y-%m-%d'))
                    _cur += timedelta(days=1)
                if on_progress:
                    on_progress(f"Anchored to latest data: {start_date}..{end_date}")

    # --- Date filtering (with fallback to all data) ---
    if date_strings is not None:
        parquet_files = [
            f for f in all_files
            if any(f"date={d}" in f for d in date_strings)
        ]
        if not parquet_files and all_files:
            # Fallback: no files match the requested date range, but data exists.
            # This happens when data is older than the time preset (e.g., data from
            # Feb 14-15 but preset is "Last 24 Hours" on Feb 17).
            logger.warning(
                f"No files match dates {date_strings}, falling back to all "
                f"{len(all_files)} files"
            )
            if on_progress:
                on_progress(f"No recent data, loading all {len(all_files)} files...")
            parquet_files = all_files
    else:
        parquet_files = all_files

    if not parquet_files:
        import pandas as pd
        return dd.from_pandas(pd.DataFrame({
            'timestamp_s': pd.Series(dtype='float64'),
            'duration_ms': pd.Series(dtype='float64'),
        }), npartitions=1)

    # --- Smart windowing: cap partitions, prefer recent data ---
    total_files = len(parquet_files)
    if total_files > MAX_PARTITIONS:
        # Sort with most recent dates first
        parquet_files.sort(reverse=True)
        # Even sampling: take every Nth file to cover the full time range
        step = total_files // MAX_PARTITIONS
        parquet_files = parquet_files[::step][:MAX_PARTITIONS]
        if on_progress:
            on_progress(f"Windowed: {MAX_PARTITIONS}/{total_files} files (step={step})")
    else:
        if on_progress:
            on_progress(f"Loading all {total_files} files")

    # --- Read with explicit paths (no Hive discovery = no mixed-layout errors) ---
    ddf = dd.read_parquet(
        parquet_files,
        storage_options=get_storage_options(),
        columns=['start_time_unix_nano', 'duration_ns'],
        engine='pyarrow',
        split_row_groups=False,
    )

    ddf['timestamp_s'] = ddf['start_time_unix_nano'] / 1_000_000_000
    ddf['duration_ms'] = ddf['duration_ns'] / 1_000_000

    if on_progress:
        on_progress(f"Ready: {ddf.npartitions} partitions (from {total_files} files)")

    return ddf


# -------------------------------------------------------------------------
# Terminal Pane (ghostty-web — PTY proxy WebSocket bridge to gRPC engine)
# -------------------------------------------------------------------------

class GhosttyTerminal(pn.reactive.ReactiveHTML):
    """Embedded terminal connecting to NavigatorEngine via PTY proxy WebSocket.

    Architecture: ghostty-web (browser WASM) → WebSocket → PTY proxy (restricted REPL)
                  → gRPC → NavigatorEngine.
    The engine emits ParamUpdate events that flow back through this bridge
    to drive Panel param changes (cmap, time_preset, spread_enabled, etc).
    """

    ws_url = param.String(default=TERMINAL_WS_URL)
    engine_update = param.String(default='')

    _template = """\
<div id="terminal_wrapper" style="width:100%;height:100%;min-height:200px;border-radius:6px;border:1px solid #30363d;background:#0d1117;display:flex;flex-direction:column;position:relative;">
  <div style="flex:0 0 auto;display:flex;align-items:center;justify-content:space-between;padding:6px 12px;background:#161b22;border-bottom:1px solid #30363d;">
    <span style="color:#c9d1d9;font-size:12px;font-weight:600;font-family:-apple-system,BlinkMacSystemFont,sans-serif;">Navigator Engine</span>
    <span id="ws_status" style="font-size:11px;font-family:-apple-system,BlinkMacSystemFont,sans-serif;color:#8b949e;">...</span>
  </div>
  <div id="terminal_container" style="position:absolute;top:30px;left:0;right:0;bottom:0;overflow:hidden;"></div>
</div>"""

    __javascript__ = []
    __css__ = []

    _scripts = {
        'render': """
            function initTerminal() {
                var wsUrl = data.ws_url;
                if (!wsUrl) {
                    var pagePort = parseInt(window.location.port) || 0;
                    var host = window.location.hostname;
                    var proto = (window.location.protocol === 'https:') ? 'wss://' : 'ws://';
                    if (pagePort === 5006) {
                        // Local dev: panel serve on 5006, pty-proxy sidecar on 8765.
                        wsUrl = 'ws://' + host + ':8765';
                    } else if (pagePort === 30506) {
                        // Zarf NodePort: panel 30506 / pty-proxy 30765 (no ingress /ws).
                        wsUrl = 'ws://' + host + ':30765';
                    } else if (pagePort === 0 || pagePort === 80 || pagePort === 443) {
                        // Ingress / tunnel: same-origin /ws → pty-proxy.
                        wsUrl = proto + window.location.host + '/ws';
                    } else {
                        // Other direct ports: prefer explicit PTY_PROXY_WS; fall back /ws.
                        wsUrl = proto + window.location.host + '/ws';
                    }
                }

                function setStatus(text, color) {
                    ws_status.textContent = text;
                    ws_status.style.color = color;
                }

                function onGhosttyReady() {
                    if (window.__ghosttyError) {
                        fetch('/ghostty/ghostty-web.js').then(function(r) {
                            terminal_container.innerHTML = '<div style="padding:20px;color:#ff7b72;font-family:sans-serif;">' +
                                'ghostty-web init failed: ' + window.__ghosttyError.message +
                                '<br><small>/ghostty/ghostty-web.js returned HTTP ' + r.status +
                                ' (' + r.headers.get('content-type') + ')</small></div>';
                        }).catch(function() {
                            terminal_container.innerHTML = '<div style="padding:20px;color:#ff7b72;font-family:sans-serif;">' +
                                'ghostty-web init failed: ' + window.__ghosttyError.message + '</div>';
                        });
                        return;
                    }

                    var g = window.__ghostty;
                    var term = new g.Terminal({
                        ghostty: g.instance,
                        cursorBlink: true,
                        fontFamily: 'Menlo, Monaco, "Courier New", monospace',
                        fontSize: 13,
                        theme: {
                            background: '#0d1117',
                            foreground: '#c9d1d9',
                            cursor: '#58a6ff',
                            selectionBackground: 'rgba(56,139,253,0.4)',
                        },
                        scrollback: 5000,
                    });

                    var fitAddon = new g.FitAddon();
                    term.loadAddon(fitAddon);

                    state._term = term;
                    term.open(terminal_container);
                    fitAddon.fit();

                    var ws = null;
                    var reconnectDelay = 1000;

                    function connect() {
                        try {
                            ws = new WebSocket(wsUrl);
                        } catch (e) {
                            setStatus('error', '#ff7b72');
                            setTimeout(connect, Math.min(reconnectDelay *= 2, 30000));
                            return;
                        }
                        state._ws = ws;

                        ws.onopen = function() {
                            setStatus('connected ' + term.cols + 'x' + term.rows, '#3fb950');
                            reconnectDelay = 1000;
                        };

                        ws.onmessage = function(evt) {
                            try {
                                var msg = JSON.parse(evt.data);
                                if (msg.type === 'text') {
                                    term.write(msg.data);
                                } else if (msg.type === 'param_update') {
                                    data.engine_update = JSON.stringify({
                                        param: msg.param, value: msg.value,
                                        source: msg.source, ts: Date.now(),
                                    });
                                }
                            } catch (e) {
                                term.write(String(evt.data));
                            }
                        };

                        ws.onclose = function() {
                            setStatus('reconnecting...', '#d29922');
                            setTimeout(function() {
                                if (term.reset) term.reset();
                                reconnectDelay = Math.min(reconnectDelay * 2, 30000);
                                connect();
                            }, reconnectDelay);
                        };

                        ws.onerror = function() {};
                    }

                    term.onData(function(input) {
                        if (ws && ws.readyState === WebSocket.OPEN) {
                            ws.send(JSON.stringify({type: 'input', data: input}));
                        }
                    });

                    // Auto-fit on container resize (Panel layout settling, window resize)
                    if (typeof ResizeObserver !== 'undefined') {
                        var resizeTimer = null;
                        var ro = new ResizeObserver(function() {
                            clearTimeout(resizeTimer);
                            resizeTimer = setTimeout(function() { fitAddon.fit(); }, 50);
                        });
                        ro.observe(terminal_container);
                        state._resizeObserver = ro;
                    }

                    setStatus('connecting...', '#d29922');
                    connect();
                }

                // loader.js (via __javascript__) may have already completed,
                // or it may still be running. Handle both cases.
                if (window.__ghostty || window.__ghosttyError) {
                    onGhosttyReady();
                } else {
                    setStatus('loading terminal...', '#8b949e');
                    var timeout = setTimeout(function() {
                        fetch('/ghostty/ghostty-web.js').then(function(r) {
                            terminal_container.innerHTML = '<div style="padding:20px;color:#ff7b72;font-family:sans-serif;">' +
                                'ghostty-web init timeout (10s).' +
                                '<br><small>/ghostty/ghostty-web.js returned HTTP ' + r.status +
                                ' (' + r.headers.get('content-type') + ')</small>' +
                                '<br><small>Check browser console for module errors.</small></div>';
                        }).catch(function() {
                            terminal_container.innerHTML = '<div style="padding:20px;color:#ff7b72;font-family:sans-serif;">' +
                                'ghostty-web assets not found. Image may need rebuild.</div>';
                        });
                    }, 10000);
                    window.addEventListener('ghostty-ready', function() {
                        clearTimeout(timeout);
                        onGhosttyReady();
                    }, { once: true });
                }
            }

            initTerminal();
        """,
        'remove': r"""
            if (state._resizeObserver) { try { state._resizeObserver.disconnect(); } catch(e) {} }
            if (state._ws) { try { state._ws.close(); } catch(e) {} }
            if (state._term) { try { state._term.dispose(); } catch(e) {} }
        """,
    }


# -------------------------------------------------------------------------
# Main App
# -------------------------------------------------------------------------

def _wrap_canvas(content):
    """Wrap viz content in a fixed-height column so the canvas never collapses."""
    return pn.Column(
        content,
        min_height=CANVAS_HEIGHT,
        height=CANVAS_HEIGHT,
        sizing_mode='stretch_width',
        styles={
            'min-height': f'{CANVAS_HEIGHT}px',
            'height': f'{CANVAS_HEIGHT}px',
        },
    )


class SpanExplorer(param.Parameterized):
    """Auto-loading span explorer with live Dask status and dataset auto-swap."""

    # Controls
    time_preset = param.Selector(
        default='All Data',
        objects=['All Data', 'Last Hour', 'Last 6 Hours', 'Last 24 Hours', 'Last 7 Days'],
    )
    cmap = param.Selector(default='fire', objects=['fire', 'viridis', 'plasma', 'inferno', 'blues'])
    spread_enabled = param.Boolean(default=True)
    # Which viz component is shown. 'explorer' = the datashaded density heatmap
    # (the original view). 'tap' = a datashaded heatmap + a tap-linked latency-
    # spectrum cross-section (Holoviews Tap stream). Driven from the engine via
    # the `switch` command (a ParamUpdate), so a human — or, later, the agent
    # running in the engine — can dynamically swap the visualization.
    active_view = param.Selector(default='explorer', objects=['explorer', 'tap'])
    # Bumped when the tap view's density grid finishes computing in a background
    # thread; viz_view depends on it so the spinner swaps for the linked curve.
    tap_grid_token = param.Integer(default=0)

    # State
    phase = param.String(default='Initializing...')
    workers = param.Integer(default=0)
    processing = param.Integer(default=0)
    partitions = param.Integer(default=0)
    ready = param.Boolean(default=False)
    error = param.String(default='')

    # Dataset state
    current_dataset = param.String(default='')
    dataset_phase = param.String(default='')

    def __init__(self, **params):
        super().__init__(**params)
        self._ddf = None
        # Cached datashader density grid (time-bin × duration-bin counts) for the
        # tap view's instant cross-sections. Keyed by (dataset, ddf identity) so a
        # dataset reload invalidates it; see _ensure_grid() (background compute).
        self._grid_cache = None
        self._grid_cache_key = None
        self._grid_thread = None
        self._poll_thread = None
        self._dataset_thread = None
        self._stop_polling = False
        self._current_data_path = None

        # Bind module-level functions for use in daemon threads.
        # Panel/Bokeh re-executes scripts per-session in isolated globals;
        # daemon threads can outlive that globals dict, causing NameError.
        self._get_active_dataset = get_active_dataset
        self._load_span_data = load_span_data
        self._get_dask_client = get_dask_client
        self._get_dask_stats = get_dask_stats

        ds_info = self._get_active_dataset()
        self.current_dataset = ds_info['dataset']
        self.dataset_phase = ds_info['phase']
        self._current_data_path = ds_info['path']

        # Terminal pane (WebSocket → PTY proxy → gRPC engine)
        self._terminal_pane = GhosttyTerminal(
            ws_url=TERMINAL_WS_URL,
            sizing_mode='stretch_both',
            min_height=250,
        )
        self._terminal_pane.param.watch(self._on_engine_update, ['engine_update'])

    def _on_engine_update(self, event):
        """Apply param changes from engine (via terminal WebSocket bridge).

        Uses pn.state.execute() to schedule on the Bokeh event loop,
        matching the _set_params() pattern in load_data().
        """
        if not event.new:
            return
        try:
            update = json.loads(event.new)
        except (json.JSONDecodeError, TypeError):
            return

        param_name = update.get('param', '')
        value = update.get('value', '')
        source = update.get('source', '')

        if source == 'user':
            return  # Don't echo user-initiated widget changes

        if not param_name or param_name not in self.param:
            return

        # active_view: the engine sends the sentinel "toggle" (no concrete value)
        # for a bare `switch`, so the flip resolves HERE — per browser session —
        # keeping the engine stateless when several browsers share one engine.
        # An explicit `switch tap`/`switch explorer` sends a concrete view name.
        if param_name == 'active_view':
            objs = list(self.param.active_view.objects)
            if value not in objs:
                cur = self.active_view
                value = objs[(objs.index(cur) + 1) % len(objs)] if cur in objs else objs[0]

        # Type coercion
        p = self.param[param_name]
        if isinstance(p, param.Boolean):
            value = str(value).lower() in ('true', '1', 'yes')

        def _apply():
            setattr(self, param_name, value)

        try:
            pn.state.execute(_apply)
        except Exception:
            setattr(self, param_name, value)

    def _get_time_range(self):
        if self.time_preset == 'All Data':
            return (None, None)
        now = datetime.now(timezone.utc)
        deltas = {
            'Last Hour': timedelta(hours=1),
            'Last 6 Hours': timedelta(hours=6),
            'Last 24 Hours': timedelta(hours=24),
            'Last 7 Days': timedelta(days=7),
        }
        return (now - deltas[self.time_preset], now)

    def _poll_dask_status(self):
        """Background thread to poll Dask task activity."""
        import time as _time
        while not self._stop_polling:
            try:
                stats = self._get_dask_stats()
                self.workers = stats['workers']
                self.processing = stats['processing']
            except Exception:
                pass
            _time.sleep(1)

    def start_polling(self):
        """Start background Dask status polling."""
        if self._poll_thread is None:
            self._stop_polling = False
            self._poll_thread = threading.Thread(target=self._poll_dask_status, daemon=True)
            self._poll_thread.start()

    def _poll_dataset_changes(self):
        """Background thread to poll for dataset changes."""
        import time as _time
        import logging as _logging
        _logger = _logging.getLogger(__name__)

        _time.sleep(10)
        while not self._stop_polling:
            try:
                ds_info = self._get_active_dataset()
                new_dataset = ds_info['dataset']
                if new_dataset != self.current_dataset:
                    _logger.info(f"Dataset changed: {self.current_dataset} -> {new_dataset}")
                    self.current_dataset = new_dataset
                    self.dataset_phase = ds_info['phase']
                    self._current_data_path = ds_info['path']
                    # Clear file list cache so new dataset is re-globbed
                    _file_list_cache.clear()
                    # Clear cached data to force full reload
                    self._ddf = None
                    self._grid_cache = None  # tap view's density grid is now stale
                    self.ready = False
                    self.phase = f"Switched to {new_dataset}, reloading..."
                    self.load_data()
            except Exception as e:
                _logger.warning(f"Dataset poll error: {e}")
            _time.sleep(60)

    def start_dataset_watcher(self):
        """Start background dataset change polling."""
        if self._dataset_thread is None:
            self._dataset_thread = threading.Thread(target=self._poll_dataset_changes, daemon=True)
            self._dataset_thread.start()

    def load_data(self):
        """Load data in a background thread to avoid blocking the Bokeh event loop.

        The S3 glob + persist can take minutes for large datasets. Running it
        synchronously in onload() blocks WebSocket message delivery, causing the
        browser to time out before the UI ever updates past 'Initializing...'.

        Param watchers triggered by attribute changes execute rendering code
        (pn.pane.Alert, etc.) in the calling thread. Since Panel/Bokeh objects
        are not thread-safe, we batch all param updates via pn.state.execute()
        so they run on the event loop.
        """
        print(f"[OTEL-NAV] load_data called, dataset={self.current_dataset}", flush=True)
        import logging as _logging_bg
        import panel as _pn_bg

        def _set_params(**kwargs):
            """Schedule param updates on the Bokeh event loop."""
            def _apply():
                for k, v in kwargs.items():
                    setattr(self, k, v)
            try:
                _pn_bg.state.execute(_apply)
            except Exception:
                # Fallback: direct set (works when no active session)
                for k, v in kwargs.items():
                    setattr(self, k, v)

        def _bg_load():
            _log = _logging_bg.getLogger("panel.load")
            try:
                _set_params(phase=f"Connecting to Dask ({self.current_dataset})...")
                _log.info(f"Connecting to Dask ({self.current_dataset})...")
                client = self._get_dask_client()
                stats = self._get_dask_stats()
                _set_params(workers=stats['workers'])

                def on_progress(msg):
                    _set_params(phase=msg)
                    _log.info(msg)

                start, end = self._get_time_range()
                self._ddf = self._load_span_data(
                    start, end,
                    data_path=self._current_data_path,
                    on_progress=on_progress,
                )
                nparts = self._ddf.npartitions
                msg = f"Ready ({self.current_dataset}: {nparts} partitions)"
                _log.info(msg)
                _set_params(partitions=nparts, phase=msg, ready=True)
                self.start_polling()
                self.start_dataset_watcher()

            except Exception as e:
                _log.exception("Load failed")
                _set_params(error=str(e), phase=f"Error: {e}")

        threading.Thread(target=_bg_load, daemon=True).start()

    # Include `ready` — without it the activity line sticks on "Loading..." after
    # load_data sets ready=True (phase/partitions update, but this pane never re-runs).
    @param.depends(
        'phase', 'workers', 'processing', 'partitions', 'ready',
        'error', 'current_dataset', 'dataset_phase',
    )
    def status_panel(self):
        """Live status panel showing Dask activity and dataset info."""
        if self.error:
            return pn.pane.Alert(f"**Error**: {self.error}", alert_type='danger')

        if self.processing > 0:
            activity = f"**{self.processing}** tasks running"
            activity_style = "color: #28a745; font-weight: bold;"
        elif self.ready:
            activity = "Ready"
            activity_style = "color: #28a745; font-weight: bold;"
        else:
            activity = "Loading..."
            activity_style = "color: #007bff;"

        phase_emoji = {"minimal": "🔵", "large": "🟢", "default": "⚪"}.get(self.dataset_phase, "⚪")

        return pn.Column(
            pn.pane.Markdown("### Status", margin=(0, 0, 5, 0)),
            pn.pane.HTML(f"<div style='{activity_style}'>{activity}</div>"),
            pn.pane.Markdown(f"""
**Phase**: {self.phase}
**Workers**: {self.workers}
**Partitions**: {self.partitions}
**Dataset**: {phase_emoji} {self.current_dataset} ({self.dataset_phase})
            """, margin=(5, 0, 0, 0)),
            sizing_mode='stretch_width',
        )

    @param.depends('active_view', 'ready', 'cmap', 'spread_enabled', 'time_preset', 'tap_grid_token')
    def viz_view(self):
        """Active visualization — dispatches on active_view, which the engine's
        `switch` command drives via a ParamUpdate. The shared error/loading guards
        live here so both components can assume a ready DataFrame."""
        if self.error:
            return _wrap_canvas(pn.pane.Alert(f"Error: {self.error}", alert_type='danger'))

        if not self.ready or self._ddf is None:
            return _wrap_canvas(
                pn.Column(
                    pn.indicators.LoadingSpinner(value=True, size=50, color='primary'),
                    pn.pane.Markdown(f"**{self.phase}**", align='center'),
                    align='center',
                    styles={'display': 'flex', 'justify-content': 'center', 'align-items': 'center', 'height': '100%'},
                )
            )

        if self.active_view == 'tap':
            return self._tap_component()
        return self._explorer_component()

    def _cmap(self):
        """Resolve the selected colormap name to a colorcet / named map."""
        return {
            'fire': cc.fire, 'viridis': 'viridis',
            'plasma': 'plasma', 'inferno': 'inferno', 'blues': cc.blues,
        }.get(self.cmap, cc.fire)

    def _explorer_component(self):
        """The original datashaded density heatmap: rasterize(dynamic=True) for
        native viewport-driven re-rasterization (no manual DynamicMap / RangeXY —
        datashader renders only what's visible in the current viewport)."""
        try:
            points = hv.Points(self._ddf, kdims=['timestamp_s', 'duration_ms']).opts(
                width=CANVAS_WIDTH, height=CANVAS_HEIGHT,
            )

            rasterized = rasterize(points, aggregator='count', dynamic=True).opts(
                cmap=self._cmap(),
                cnorm='eq_hist',
                colorbar=True,
                xlabel='Time (Unix seconds)',
                ylabel='Duration (ms)',
                title=f'Span Latency - {self.time_preset}',
                tools=['hover', 'box_zoom', 'wheel_zoom', 'pan', 'reset'],
                active_tools=['box_zoom'],
                responsive=True,
            )

            result = dynspread(rasterized, max_px=3) if self.spread_enabled else rasterized

            return _wrap_canvas(
                pn.pane.HoloViews(
                    result,
                    sizing_mode='stretch_width',
                    min_height=CANVAS_HEIGHT,
                    height=CANVAS_HEIGHT,
                )
            )

        except Exception as e:
            logger.exception("Render failed")
            return _wrap_canvas(pn.pane.Alert(f"Render error: {e}", alert_type='warning'))

    def _ensure_grid(self):
        """Compute the datashader density grid (time-bins × duration-bins of span
        counts) in a BACKGROUND thread — never on the Bokeh event loop, where a
        full-dataset aggregation would freeze the UI (the symptom that made `switch`
        appear to hang on the old heatmap). One reduction per (dataset, ddf), cached;
        bumps tap_grid_token on completion so viz_view swaps the spinner for the
        linked curve. The heatmap and every tapped slice then come from the SAME
        datashader aggregate, and taps are instant (no per-tap Dask compute)."""
        key = (self.current_dataset, id(self._ddf))
        if self._grid_cache is not None and self._grid_cache_key == key:
            return
        if self._grid_thread is not None and self._grid_thread.is_alive():
            return  # already computing
        ddf = self._ddf

        def _bg():
            import datashader as _ds
            import panel as _pn_bg
            try:
                # 240 × 160 bins ≈ 38k cells — trivially small in memory to slice.
                cvs = _ds.Canvas(plot_width=240, plot_height=160)
                agg = cvs.points(ddf, 'timestamp_s', 'duration_ms', _ds.count())
                if hasattr(getattr(agg, 'data', None), 'compute'):
                    agg = agg.compute()  # materialize off the event loop
                self._grid_cache = agg
                self._grid_cache_key = key

                def _bump():
                    self.tap_grid_token = self.tap_grid_token + 1
                try:
                    _pn_bg.state.execute(_bump)  # schedule on the Bokeh loop
                except Exception:
                    self.tap_grid_token = self.tap_grid_token + 1
            except Exception:
                logger.exception("tap density-grid compute failed")

        self._grid_thread = threading.Thread(target=_bg, daemon=True)
        self._grid_thread.start()

    def _tap_component(self):
        """The "more sophisticated" view (Holoviews Tap stream): a datashaded
        density heatmap on the LEFT and, on the RIGHT, a latency-spectrum curve —
        a vertical cross-section of the SAME datashader aggregate at the tapped
        time. Tap a moment in the heatmap → see the distribution of span durations
        then. The cross-section grid is computed in a background thread, so the
        switch is instant: the heatmap + a spinner paint immediately and the linked
        curve appears once the grid is ready (tap_grid_token bump re-renders).
        (Tapping selects a time column; to instead show a latency band's volume
        over time, slice the grid on duration_ms — a one-line change.)"""
        try:
            # Heatmap: lazy rasterize (viewport-driven, NON-blocking), tap-enabled.
            points = hv.Points(self._ddf, kdims=['timestamp_s', 'duration_ms'])
            rasterized = rasterize(points, aggregator='count', dynamic=True).opts(
                cmap=self._cmap(),
                cnorm='eq_hist',
                colorbar=True,
                xlabel='Time (Unix seconds)',
                ylabel='Duration (ms)',
                title='Span Latency — tap a moment to inspect',
                tools=['tap', 'hover', 'box_zoom', 'wheel_zoom', 'pan', 'reset'],
                active_tools=['box_zoom'],
                width=480,
                height=CANVAS_HEIGHT,
            )
            # The Tap stream must source the EXACT plotted element (dynspread-wrapped
            # iff spread is on), so resolve it before building the stream.
            heatmap = dynspread(rasterized, max_px=3) if self.spread_enabled else rasterized

            key = (self.current_dataset, id(self._ddf))
            grid_ready = self._grid_cache is not None and self._grid_cache_key == key

            if not grid_ready:
                # Background compute; paint the heatmap + a spinner immediately so the
                # switch is visibly instant (no event-loop block).
                self._ensure_grid()
                spinner = pn.Column(
                    pn.indicators.LoadingSpinner(value=True, size=40, color='primary'),
                    pn.pane.Markdown('**Computing latency spectrum…**'),
                    width=320, height=CANVAS_HEIGHT,
                    styles={'display': 'flex', 'flex-direction': 'column',
                            'justify-content': 'center', 'align-items': 'center'},
                )
                return _wrap_canvas(pn.Row(
                    pn.pane.HoloViews(heatmap, min_height=CANVAS_HEIGHT),
                    spinner, sizing_mode='stretch_width', min_height=CANVAS_HEIGHT,
                ))

            # Grid ready → a single linked HoloViews Layout (heatmap + curve in ONE
            # pane), so the Tap stream is guaranteed to link them (cross-pane linking
            # is not relied on). Every tapped slice reads the cached datashader grid.
            agg = self._grid_cache
            times = agg.coords['timestamp_s'].values
            durs = agg.coords['duration_ms'].values
            x0 = float(times[len(times) // 2]) if len(times) else 0.0

            def latency_spectrum(x, y):
                xx = x0 if x is None else x
                col = agg.sel(timestamp_s=xx, method='nearest')
                when = datetime.fromtimestamp(float(xx), tz=timezone.utc).strftime('%H:%M:%S')
                return hv.Curve((durs, col.values), kdims='Duration (ms)', vdims='Span count').opts(
                    framewise=True,
                    width=320, height=CANVAS_HEIGHT,
                    color='#0072B5',
                    title=f'Latency spectrum @ {when} UTC',
                    tools=['hover'],
                    yaxis='right',
                )

            tap = hv.streams.Tap(source=heatmap, x=x0)
            spectrum = hv.DynamicMap(latency_spectrum, streams=[tap])
            return _wrap_canvas(pn.pane.HoloViews(
                heatmap + spectrum,
                sizing_mode='stretch_width',
                min_height=CANVAS_HEIGHT,
            ))

        except Exception as e:
            logger.exception("Tap view render failed")
            return _wrap_canvas(pn.pane.Alert(f"Render error: {e}", alert_type='warning'))

    @param.depends('active_view')
    def _view_badge(self):
        """Reactive indicator of the active viz component + the `switch` hint."""
        label = {
            'explorer': '◆ Density explorer',
            'tap': '◆ Linked latency spectrum',
        }.get(self.active_view, self.active_view)
        return pn.pane.HTML(
            f"<div style='font-size:11px;color:#0072B5;font-weight:600;'>{label}</div>"
            f"<div style='font-size:10px;color:#6c757d;'>type <code>switch</code> in the terminal to toggle</div>",
            margin=(2, 0, 0, 0),
        )

    def sidebar(self):
        """Sidebar with status and controls."""
        scheduler_short = DASK_SCHEDULER.split('.')[0] if DASK_SCHEDULER else 'N/A'
        return pn.Column(
            pn.pane.Markdown("# OTEL Navigator", margin=(0, 0, 10, 0)),
            self.status_panel,
            pn.layout.Divider(),
            pn.pane.Markdown("### Time Range"),
            pn.widgets.Select.from_param(self.param.time_preset, name='', sizing_mode='stretch_width'),
            pn.layout.Divider(),
            pn.pane.Markdown("### Visualization"),
            pn.widgets.Select.from_param(self.param.cmap, name='Colormap', sizing_mode='stretch_width'),
            pn.widgets.Checkbox.from_param(self.param.spread_enabled, name='Spread'),
            self._view_badge,
            pn.layout.Divider(),
            pn.pane.Markdown(f"""
**Dask**: `{scheduler_short}`
**Data**: `{self.current_dataset}/spans`
            """, styles={'font-size': '11px', 'color': '#6c757d'}),
            width=260,
        )

    def main_view(self):
        """Main content: active visualization + engine terminal."""
        return pn.Column(
            pn.pane.HTML(NAV_HTML),
            self.viz_view,
            pn.layout.Divider(),
            self._terminal_pane,
            sizing_mode='stretch_both',
            styles={
                'overflow': 'visible',
            },
        )

    def servable(self):
        """Build the app and auto-trigger load."""
        raw_css = """
        .main-content, .bk-root, .bk-Column {
            min-height: 500px !important;
        }
        #main {
            min-height: calc(100vh - 80px) !important;
        }
        .pn-loading {
            min-height: 500px !important;
        }
        #terminal_wrapper {
            overflow: hidden !important;
        }
        /* Let terminal wrapper fill its container without clipping */
        .pn-wrapper, fast-card.pn-wrapper {
            overflow: visible !important;
        }
        /* ghostty-web: let the WASM terminal control its own sizing */
        #terminal_container canvas {
            display: block;
        }
        """
        pn.config.raw_css.append(raw_css)

        template = pn.template.FastListTemplate(
            title="OTEL Navigator",
            sidebar=[self.sidebar()],
            main=[self.main_view],
            accent_base_color="#0072B5",
            header_background="#0072B5",
            sidebar_width=280,
            theme="dark",
            font=_AIRGAP_UI_FONT,
            font_url="",
        )

        # Auto-load data when page loads
        def on_load():
            self.load_data()

        pn.state.onload(on_load)

        return template


# Create and serve
explorer = SpanExplorer()
explorer.servable().servable()
print("[OTEL-NAV] App ready", flush=True)
