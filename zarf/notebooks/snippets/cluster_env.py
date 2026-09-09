"""Cluster configuration for sample notebooks — no hard-coded lab secrets.

Values are injected by JupyterHub singleuser ``extraEnv`` from Zarf vars that
converge passes at deploy (``--creds-file`` / ``S3_*`` / worker sizing).

Usage in a notebook first cell::

    import sys
    for p in ("/root/sample-notebooks", "/app", "/root"):
        if p not in sys.path:
            sys.path.insert(0, p)
    from cluster_env import load_cluster_config
    CFG = load_cluster_config()
    print(CFG.summary())

Never embed access keys in notebooks — only ``os.environ``.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _truthy(name: str, default: str = "0") -> bool:
    return _env(name, default).lower() in ("1", "true", "yes", "on")


# dask.config "scheduler" must be a *type* name (threads, distributed, …).
# JupyterHub / panel often set DASK_SCHEDULER=tcp://… as an *address*; dask then
# maps that env → config.scheduler and every Delayed.compute() during
# dd.read_parquet planning raises:
#   ValueError: Expected one of [distributed, threads, …]
_DASK_SCHEDULER_TYPES = frozenset(
    {
        "distributed",
        "multiprocessing",
        "processes",
        "single-threaded",
        "sync",
        "synchronous",
        "threading",
        "threads",
    }
)


def sanitize_dask_scheduler_env() -> Optional[str]:
    """Move tcp:// (etc.) out of ``DASK_SCHEDULER`` so dask.config is valid.

    Returns the cluster address if one was rescued from the env collision.
    Safe to call multiple times. Prefer ``DASK_SCHEDULER_ADDRESS`` for the
    distributed Client; keep ``DASK_SCHEDULER`` unset or a type name only.
    """
    raw = (os.environ.get("DASK_SCHEDULER") or "").strip()
    addr_env = (os.environ.get("DASK_SCHEDULER_ADDRESS") or "").strip()
    rescued: Optional[str] = None

    looks_like_addr = bool(raw) and (
        "://" in raw
        or raw.startswith(("tcp:", "tls:", "ucx:"))
        or (":" in raw and raw not in _DASK_SCHEDULER_TYPES)
    )
    if looks_like_addr:
        rescued = raw
        if not addr_env:
            os.environ["DASK_SCHEDULER_ADDRESS"] = raw
        # Drop invalid type so dask does not treat tcp:// as scheduler name
        os.environ.pop("DASK_SCHEDULER", None)

    try:
        import dask

        current = dask.config.get("scheduler", default=None)
        if current is not None and not callable(current):
            name = current if isinstance(current, str) else None
            if name is not None and name not in _DASK_SCHEDULER_TYPES:
                # Clear poisoned config (from env or prior set)
                dask.config.set({"scheduler": None})
                try:
                    dask.config.refresh()
                except Exception:
                    pass
                # refresh may re-read env; ensure still clear
                if (os.environ.get("DASK_SCHEDULER") or "").strip() and looks_like_addr:
                    os.environ.pop("DASK_SCHEDULER", None)
                still = dask.config.get("scheduler", default=None)
                if (
                    isinstance(still, str)
                    and still not in _DASK_SCHEDULER_TYPES
                    and "://" in still
                ):
                    dask.config.set({"scheduler": None})
    except Exception:
        pass

    return rescued or addr_env or None


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """s3://bucket/prefix/ → (bucket, prefix) with no leading/trailing slashes on prefix."""
    u = (uri or "").strip()
    if u.startswith("s3://"):
        rest = u[5:].strip("/")
        if "/" in rest:
            b, p = rest.split("/", 1)
            return b, p.strip("/")
        return rest, ""
    return u.strip("/"), ""


@dataclass
class ClusterConfig:
    """Resolved runtime config for notebooks on the air-gap / RKE2 stack."""

    s3_bucket: str
    s3_endpoint: str
    s3_region: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str
    otel_data_path: str  # s3://bucket/prefix/ from panel ConfigMap path
    otel_prefix: str  # prefix only (e.g. otel-notebook)
    dask_scheduler: str
    use_dask: bool = True
    # Optional sizing / profile knobs (also env)
    hdf5_profile: str = "lab"
    extras: Dict[str, str] = field(default_factory=dict)

    @property
    def dataset_root_key(self) -> str:
        """bucket/prefix for s3fs paths (no s3://)."""
        if self.otel_prefix:
            return f"{self.s3_bucket}/{self.otel_prefix}".rstrip("/")
        return self.s3_bucket

    @property
    def spans_glob(self) -> str:
        """OTEL_Data_Generator layout: {root}/spans/date=*/*.parquet (no hour=)."""
        return f"s3://{self.dataset_root_key}/spans/date=*/*.parquet"

    @property
    def spans_prefix_s3(self) -> str:
        return f"s3://{self.dataset_root_key}/spans/"

    def storage_options(self) -> Dict[str, Any]:
        """Kwargs for ``s3fs.S3FileSystem`` / ``dd.read_parquet(..., storage_options=)``."""
        opts: Dict[str, Any] = {
            "anon": False,
            "key": self.aws_access_key_id or None,
            "secret": self.aws_secret_access_key or None,
        }
        if self.aws_session_token:
            opts["token"] = self.aws_session_token
        if self.s3_endpoint:
            opts["client_kwargs"] = {
                "endpoint_url": self.s3_endpoint,
                "region_name": self.s3_region or "us-east-1",
            }
            # path-style for MinIO / gateways; timeouts avoid multi-minute hangs
            opts["config_kwargs"] = {
                "s3": {"addressing_style": "path"},
                "signature_version": "s3v4",
                "connect_timeout": 5,
                "read_timeout": 60,
                "retries": {"max_attempts": 3, "mode": "standard"},
            }
        elif self.s3_region:
            opts["client_kwargs"] = {"region_name": self.s3_region}
        return opts

    def pyarrow_s3_kwargs(self) -> Dict[str, Any]:
        """Kwargs for ``pyarrow.fs.S3FileSystem`` (writes / small metadata only)."""
        kw: Dict[str, Any] = {
            "region": self.s3_region or "us-east-1",
            "access_key": self.aws_access_key_id or "",
            "secret_key": self.aws_secret_access_key or "",
        }
        if self.aws_session_token:
            kw["session_token"] = self.aws_session_token
        if self.s3_endpoint:
            kw["endpoint_override"] = self.s3_endpoint
            kw["scheme"] = "https" if self.s3_endpoint.startswith("https") else "http"
        return kw

    def connect_dask(self):
        """Return a ``distributed.Client`` (requires dask.distributed)."""
        from dask.distributed import Client

        sanitize_dask_scheduler_env()
        if not self.dask_scheduler:
            raise RuntimeError(
                "DASK_SCHEDULER_ADDRESS not set — JupyterHub should inject it from "
                "the in-cluster scheduler Service"
            )
        # Pass address explicitly — never rely on DASK_SCHEDULER env (type vs addr).
        return Client(self.dask_scheduler)

    def summary(self) -> str:
        lines = [
            "ClusterConfig (from env — no notebook hard-codes)",
            f"  S3_BUCKET          = {self.s3_bucket}",
            f"  S3_ENDPOINT        = {self.s3_endpoint or '(AWS default)'}",
            f"  S3_REGION          = {self.s3_region}",
            f"  OTEL_DATA_PATH     = {self.otel_data_path or '(unset)'}",
            f"  otel_prefix        = {self.otel_prefix or '(bucket root)'}",
            f"  spans              = {self.spans_prefix_s3}",
            f"  DASK_SCHEDULER     = {self.dask_scheduler}",
            f"  USE_DASK           = {self.use_dask}",
            f"  AWS_ACCESS_KEY_ID  = {'set' if self.aws_access_key_id else 'empty'} "
            f"(len={len(self.aws_access_key_id)})",
            f"  AWS_SECRET         = {'set' if self.aws_secret_access_key else 'empty'} "
            f"(len={len(self.aws_secret_access_key)})",
            f"  HDF5_PROFILE       = {self.hdf5_profile}",
        ]
        return "\n".join(lines)


def load_cluster_config() -> ClusterConfig:
    """Build config from process environment (JupyterHub singleuser extraEnv)."""
    # Must run before any dd.read_parquet — see sanitize_dask_scheduler_env docstring.
    rescued = sanitize_dask_scheduler_env()

    bucket = _env("S3_BUCKET")
    otel_path = _env("OTEL_DATA_PATH")
    if not otel_path and bucket:
        # Match panel default when only bucket is set
        otel_path = f"s3://{bucket}/"
    b_from_path, prefix = _parse_s3_uri(otel_path) if otel_path else ("", "")
    if not bucket:
        bucket = b_from_path or "cyberphy"
    # Explicit prefix env wins (optional override)
    prefix = _env("OTEL_PREFIX") or _env("OTEL_DATASET_PREFIX") or prefix
    # If OTEL_DATA_PATH is s3://bucket/ only, allow PREFIX default for generators
    if not prefix:
        # Field default matches panel OTEL_DATA_PATH …/otel-notebook/ (not validation-30gb)
        prefix = _env("PREFIX", "") or "otel-notebook"

    region = (
        _env("AWS_REGION")
        or _env("AWS_DEFAULT_REGION")
        or _env("S3_REGION")
        or "us-east-1"
    )
    scheduler = (
        _env("DASK_SCHEDULER_ADDRESS")
        or rescued
        or "tcp://cybersec-dask-scheduler.dask.svc.cluster.local:8786"
    )

    return ClusterConfig(
        s3_bucket=bucket,
        s3_endpoint=_env("S3_ENDPOINT"),
        s3_region=region,
        aws_access_key_id=_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_env("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=_env("AWS_SESSION_TOKEN"),
        otel_data_path=otel_path or f"s3://{bucket}/",
        otel_prefix=prefix,
        dask_scheduler=scheduler,
        use_dask=_env("USE_DASK", "1").lower() not in ("0", "false", "no", "off"),
        hdf5_profile=_env("HDF5_PROFILE", "lab") or "lab",
        extras={
            k: _env(k)
            for k in (
                "DASK_WORKER_REPLICAS",
                "DASK_WORKER_NTHREADS",
                "DASK_WORKER_MEMORY",
                "HDF5_FORCE_REGENERATE",
                "DASK_VIZ_FRAC",
            )
            if _env(k)
        },
    )


def require_parquet_stack() -> dict:
    """Fail loud if Dask + PyArrow are missing (needed for distributed parquet I/O).

    The cybersec-dask image bakes both and fails the image build if imports break.
    Notebooks must not silently fall back to pandas / skip distributed reads.

    Note: ``engine="pyarrow"`` on ``dd.read_parquet`` is a legacy kwarg from when
    fastparquet was an alternative; modern Dask defaults to the Arrow path.
    Passing it is harmless (or deprecated) and does not change behavior when
    PyArrow is the only viable engine.
    """
    info: dict = {}
    try:
        import dask
        import dask.dataframe as dd  # noqa: F401
        import pyarrow as pa
        import pyarrow.parquet  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            f"Missing Dask/PyArrow in this kernel: {e}. "
            f"Use the cybersec-dask singleuser image (not a bare python kernel)."
        ) from e
    info["dask"] = getattr(dask, "__version__", "?")
    info["pyarrow"] = getattr(pa, "__version__", "?")
    try:
        from dask.dataframe.io.parquet.core import get_engine
        eng = get_engine("auto")
        info["parquet_backend"] = f"{type(eng).__module__}"
    except Exception as e:
        raise RuntimeError(
            f"dask cannot resolve a parquet backend (need PyArrow): {e}."
        ) from e
    return info


# ---------------------------------------------------------------------------
# O(files) discovery wall (field notebooks, 2026-07-30)
# ---------------------------------------------------------------------------
# Silent client-side grind (Dask dashboard empty until the first task renders):
#   1. recursive listing of every partition dir (sequential LISTs, high latency)
#   2. graph construction / optimization over one task per file
#   3. shipping a huge graph to the scheduler
# At dozens of files it's invisible; at months × many files/day it is minutes
# before workers start — then overhead-dominated tiny tasks.
#
# Near-term cures (escalating):
#   • Prune BEFORE discovery — narrow the root (date=YYYY-MM-DD/), not only
#     filters= after a full tree walk.
#   • List ONCE (fs.find on that root) and hand the file list to read_parquet
#     (otel-navigator enumerate-window-read pattern).
#   • Fatten tasks — aggregate_files=True / blocksize so small files merge.
#   • ddf.persist() if iterating — pay discovery+load once.
#
# Strategic: Iceberg/metadata plane (#42–45) replaces O(files) listing with
# manifest-based planning. Explicit-list keeps notebooks snappy until then.


def list_span_parquet_keys(
    cfg: Optional[ClusterConfig] = None,
    *,
    under: Optional[str] = None,
    date: Optional[str] = None,
    max_list: int = 500_000,
) -> list:
    """List parquet keys under spans/ — **one recursive find**, optionally pruned.

    Layouts:
      1. OTEL_Data_Generator — ``spans/date=YYYY-MM-DD/*.parquet`` (no hour=)
      2. Production — ``spans/date=*/hour=*/*.parquet``
      3. Shard layouts — ``spans/shard=*/date=*/…``

    Args:
      under: object-store prefix to list (default ``{bucket}/{otel_prefix}/spans``).
             Narrow here to prune listing (e.g. ``…/spans/date=2026-07-01``).
      date:  convenience — appends ``/date={date}`` under the default spans root.
             Prefer this over ``filters=[("date",…)]`` on a wide root: filters=
             only prune reads *after* full discovery.

    Never use per-directory ``ls`` loops as the primary path (O(partitions) RTT).
    Never use ``**`` globs (s3fs often empty / pathological).
    """
    import s3fs

    cfg = cfg or load_cluster_config()
    fs = s3fs.S3FileSystem(**cfg.storage_options())
    root = cfg.dataset_root_key.rstrip("/")
    spans = f"{root}/spans"
    if under:
        list_root = under.replace("s3://", "").rstrip("/")
    elif date:
        list_root = f"{spans}/date={date}"
    else:
        list_root = spans

    files: list = []
    # Single recursive LIST (paginated) — preferred over N× per-date ls
    try:
        if fs.exists(list_root):
            found = fs.find(list_root)
            files = [e for e in found if str(e).endswith(".parquet")]
    except Exception as e:
        print(f"list_span_parquet_keys: find({list_root!r}) failed: {e}")
        files = []

    # Fallback globs only if find returned nothing (empty or odd FS)
    if not files and list_root.rstrip("/").endswith("/spans"):
        for pat in (
            f"{list_root}/date=*/*.parquet",
            f"{list_root}/date=*/hour=*/*.parquet",
            f"{list_root}/shard=*/date=*/*.parquet",
        ):
            try:
                hits = [h for h in (fs.glob(pat) or []) if str(h).endswith(".parquet")]
            except Exception:
                hits = []
            if hits:
                files = hits
                break
    elif not files and "/date=" in list_root:
        try:
            files = [
                h for h in (fs.glob(f"{list_root}/*.parquet") or [])
                if str(h).endswith(".parquet")
            ]
        except Exception:
            files = []

    if len(files) > max_list:
        print(
            f"list_span_parquet_keys: truncating {len(files)} → {max_list} "
            f"(pass max_list= or narrow date=/under=)"
        )
        files = files[:max_list]
    return files


def load_active_spans_ddf(
    cfg: Optional[ClusterConfig] = None,
    *,
    columns: Optional[list] = None,
    require_dask: bool = True,
    date: Optional[str] = None,
    under: Optional[str] = None,
    aggregate_files: bool = True,
    persist: bool = False,
):
    """Lazy Dask DataFrame over partitioned span parquet — never full pyarrow load.

    Default field layout from **OTEL_Data_Generator** notebook::

        s3://{bucket}/{prefix}/spans/date=YYYY-MM-DD/batch_*.parquet

    Near-term anti-O(files) pattern (see module note above)::

        # prune listing to one day, fatten partitions, optional persist
        ddf = load_active_spans_ddf(date="2026-07-01", aggregate_files=True)
        ddf = ddf.persist()   # if iterating

    ``filters=`` on a wide ``spans/`` root is *not* enough — discovery still
    walks every partition. Use ``date=`` / ``under=`` to prune the LIST.
    """
    import time

    import dask
    import dask.dataframe as dd

    sanitize_dask_scheduler_env()
    stack = require_parquet_stack()
    print(
        f"parquet stack: dask={stack['dask']} pyarrow={stack['pyarrow']} "
        f"backend={stack.get('parquet_backend', '?')}"
    )

    cfg = cfg or load_cluster_config()
    if require_dask and not cfg.use_dask:
        raise RuntimeError("USE_DASK=0 but load_active_spans_ddf requires Dask for large data")

    spans = f"{cfg.dataset_root_key.rstrip('/')}/spans"
    t0 = time.time()
    files = list_span_parquet_keys(cfg, under=under, date=date)
    t_list = time.time() - t0
    if not files:
        scope = under or (f"{spans}/date={date}" if date else spans)
        raise FileNotFoundError(
            f"no parquet under s3://{scope}/.\n"
            f"OTEL_Data_Generator: s3://{spans}/date=YYYY-MM-DD/*.parquet "
            f"(date= only — no hour=).\n"
            f"Prune with date='YYYY-MM-DD' or under='bucket/prefix/spans/date=…'."
        )
    uris = [f"s3://{f}" if not str(f).startswith("s3://") else str(f) for f in files]
    sample = uris[:3]
    layout = "date=/hour=/*" if any("/hour=" in u for u in sample) else "date=/*"
    print(
        f"load_active_spans_ddf: listed {len(uris)} files in {t_list:.1f}s "
        f"(layout≈{layout}) under s3://{spans}/"
        f"{' date='+date if date else ''}\n  sample: {sample}"
    )
    if len(uris) > 500:
        print(
            f"  ⚠ {len(uris)} files → client graph construction can dominate "
            f"(dashboard empty until first task). Prefer date=/under= prune + "
            f"aggregate_files; durable fix is Iceberg/metadata plane (#42–45)."
        )

    # Explicit URI list (otel-navigator pattern). aggregate_files fattens tiny tasks.
    # Force a *local* scheduler for collection-plan .compute() inside read_parquet —
    # workers are for later Client.submit, not for planning metadata on the client.
    t1 = time.time()
    kwargs: dict = {
        "storage_options": cfg.storage_options(),
        "columns": columns,
        "aggregate_files": aggregate_files,
    }
    with dask.config.set({"scheduler": "synchronous"}):
        try:
            ddf = dd.read_parquet(uris, **kwargs)
        except TypeError:
            # older dask without aggregate_files
            kwargs.pop("aggregate_files", None)
            ddf = dd.read_parquet(uris, **kwargs)
        nparts = ddf.npartitions  # materialize plan under safe scheduler
    t_graph = time.time() - t1
    print(f"  graph built in {t_graph:.1f}s  partitions={nparts}")

    # ddf.columns is a pandas Index — never use `or []` (truth value is ambiguous).
    cols = list(ddf.columns) if getattr(ddf, "columns", None) is not None else []
    if len(cols) == 0:
        raise RuntimeError(
            f"dd.read_parquet returned columns=[] for {len(uris)} files; "
            f"sample={sample}"
        )
    if persist:
        ddf = ddf.persist()
        print("  persisted to workers (subsequent aggs skip rediscovery/reload)")
    return ddf


def paste_load_spans_cell() -> str:
    """Return a self-contained cell for *old* in-situ notebooks (no package wait).

    Fixes notebooks-03 hang: old cells call ``dd.read_parquet(glob, storage_options)``
    without path-style addressing / timeouts, and block forever on custom endpoints.
    """
    return r'''# --- SAFE load (paste over hung dd.read_parquet cell) ---
# OTEL generator layout: s3://$BUCKET/$PREFIX/spans/date=YYYY-MM-DD/*.parquet
# Do NOT pass globs to dd.read_parquet; do NOT omit path-style on custom endpoints.
import os, sys
for _p in ("/root/sample-notebooks", "/root", "/app"):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from cluster_env import load_cluster_config, list_span_parquet_keys, load_active_spans_ddf
    CFG = load_cluster_config()
    print(CFG.summary())
    keys = list_span_parquet_keys(CFG)
    print(f"listed {len(keys)} parquet; sample={keys[:3]}")
    if not keys:
        raise FileNotFoundError(f"no files under {CFG.spans_prefix_s3}")
    ddf = load_active_spans_ddf(CFG)
except ImportError:
    # Fallback without cluster_env — still path-style + explicit file list
    import s3fs
    import dask.dataframe as dd
    endpoint = os.environ.get("S3_ENDPOINT") or ""
    bucket = os.environ.get("S3_BUCKET") or "dhfo"
    prefix = os.environ.get("OTEL_PREFIX") or "otel-notebook"
    opts = {
        "anon": False,
        "key": os.environ.get("AWS_ACCESS_KEY_ID") or None,
        "secret": os.environ.get("AWS_SECRET_ACCESS_KEY") or None,
        "token": os.environ.get("AWS_SESSION_TOKEN") or None,
    }
    if endpoint:
        opts["client_kwargs"] = {"endpoint_url": endpoint, "region_name": os.environ.get("AWS_REGION", "us-east-1")}
        opts["config_kwargs"] = {
            "s3": {"addressing_style": "path"},
            "connect_timeout": 5,
            "read_timeout": 30,
            "retries": {"max_attempts": 2, "mode": "standard"},
        }
    fs = s3fs.S3FileSystem(**opts)
    spans = f"{bucket}/{prefix}/spans"
    print(f"listing s3://{spans}/ (find, no glob)…")
    keys = [k for k in fs.find(spans) if str(k).endswith(".parquet")]
    print(f"found {len(keys)}; sample={keys[:3]}")
    if not keys:
        # date-only generator layout
        keys = [k for k in (fs.glob(f"{spans}/date=*/*.parquet") or []) if str(k).endswith(".parquet")]
        print(f"glob date=*/* → {len(keys)}")
    if not keys:
        raise FileNotFoundError(f"no parquet under s3://{spans}/date=*/*.parquet")
    uris = [f"s3://{k}" if not str(k).startswith("s3://") else k for k in keys]
    ddf = dd.read_parquet(uris, storage_options=opts)

print("partitions", ddf.npartitions, "columns", list(ddf.columns))
assert list(ddf.columns), "columns=[] — still path/creds; not schema"
ddf
'''


def paste_validate_workers_cell() -> str:
    """Minimal one-cell paste: list once → read_parquet(uris) → workers.

    No project imports — only s3fs / dask / env vars. For hand-carry to remotes.
    """
    return r'''# minimal: explicit LIST + distributed parquet read (no project imports)
import os, time
import s3fs
import dask
import dask.dataframe as dd
from dask.distributed import Client

endpoint = os.environ["S3_ENDPOINT"]
bucket   = os.environ["S3_BUCKET"]
prefix   = os.environ.get("OTEL_PREFIX", "otel-notebook").strip("/")
date     = (os.environ.get("SPANS_DATE") or "").strip() or None
sched    = os.environ.get("DASK_SCHEDULER_ADDRESS") or os.environ.get("DASK_SCHEDULER")
assert sched and "://" in sched, "set DASK_SCHEDULER_ADDRESS=tcp://scheduler:8786"

if os.environ.get("DASK_SCHEDULER", "").startswith(("tcp://", "tls://")):
    os.environ.pop("DASK_SCHEDULER", None)
    dask.config.set({"scheduler": None})

storage_options = {
    "key": os.environ.get("AWS_ACCESS_KEY_ID"),
    "secret": os.environ.get("AWS_SECRET_ACCESS_KEY"),
    "client_kwargs": {"endpoint_url": endpoint, "region_name": os.environ.get("AWS_REGION", "us-east-1")},
    "config_kwargs": {"s3": {"addressing_style": "path"}},
}

root = f"{bucket}/{prefix}/spans" + (f"/date={date}" if date else "")
fs = s3fs.S3FileSystem(**storage_options)
keys = [k for k in fs.find(root) if k.endswith(".parquet")]
print(f"listed {len(keys)} under s3://{root}/  sample={keys[:2]}")
assert keys, "no parquet"

uris = [f"s3://{k}" for k in keys]
with dask.config.set({"scheduler": "synchronous"}):
    ddf = dd.read_parquet(uris, storage_options=storage_options, aggregate_files=True)
    print("partitions", ddf.npartitions, "cols", list(ddf.columns)[:6])

client = Client(sched)
n_workers = len(client.scheduler_info().get("workers", {}))
t0 = time.time()
nrows = int(ddf.shape[0].compute())
print(f"rows≈{nrows:,}  workers={n_workers}  {time.time()-t0:.1f}s")
print("ok — explicit list + worker parquet read")
'''
