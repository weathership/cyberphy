#!/usr/bin/env python3
"""Synthetic HDF5 telemetry generator (OpenTelemetry-framed, CPHY domain).

Generates synthetic OpenTelemetry / cyber-physical (CPHY) datasets in HDF5 and
lands them in S3 (RustFS locally, AWS S3 in the cloud). The container layout is
a dense multi-series acquisition layout framed entirely in OpenTelemetry / CPHY terms:

  * depth-4 group tree (root → ResourceMetrics → {Scope, Resource_0, Metric_0})
  * rich node-level metadata: fixed-length |S strings, float64 + ".unit" pairs,
    int64/int32 counts, bool flags (GaugeLength.uom pattern)
  * one dominant 2D Values array (series × time) + 1D Timestamps index
  * compound side tables: Windows, SeriesAnchor summary + full map
  * default **contiguous** storage (Iceberg/kerchunk: one byte range per dataset)
  * optional chunked mode for native-ref experiments

Iceberg File Format API readiness
---------------------------------
Many fixed-window .h5 files under a product prefix (Iceberg data-file style keys),
with time bounds in filename + group attrs + dataset attrs, uuid chain for registration,
and Arrow/pointer-table inventory rows (see docs/current/.../hdf5-iceberg-metadata-plane.md).
Object keys are flat under datasets/hdf5/<product>/ — no Hive key=value path segments.

Profiles
--------
  lab        — small (space-constrained Jupyter/Dask demo)
  airgap_2tb — ~2 TiB of Values payload across many 1 s fidelity parts

Usage:
    python generate_hdf5.py --profile lab --out s3://cyberphy/
    python generate_hdf5.py --profile lab --out ./local_hdf5 --contiguous
    python generate_hdf5.py --profile lab --out s3://cyberphy/ --force   # always write
    python generate_hdf5.py --profile airgap_2tb --out s3://cyberphy/ \\
        --dry-run   # print plan only

By default the CLI is **idempotent**: if enough full-size parts already exist under
the product prefix, they are reused (same as the sample notebook). Pass ``--force``
to write a new run.

Dependencies: h5py, numpy; boto3 for S3.
    uv run --with h5py,numpy,boto3 python zarf/scripts/generate_hdf5.py ...
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Optional
from urllib.parse import urlparse

import numpy as np

try:
    import h5py
except ImportError:  # pragma: no cover
    raise SystemExit(
        "h5py is required: uv run --with h5py,numpy python zarf/scripts/generate_hdf5.py ..."
    )

SCHEMA_VERSION = "2.1"
OTEL_SCHEMA_URL = "https://opentelemetry.io/schemas/1.27.0"
DEFAULT_DTYPE = "int16"
# int16 full-scale encoding factor for normalized metric values
DEFAULT_SCALE_FACTOR = 10430.0

TimeUnit = Literal["ns", "us"]
GroupStyle = Literal["underscore", "brackets"]
ProfileName = Literal["lab", "airgap_2tb", "custom"]

# ---------------------------------------------------------------------------
# Profiles
# ---------------------------------------------------------------------------
# Values bytes ≈ n_series * n_time * itemsize * n_parts  (plus small HDF5 overhead)
PROFILES: dict[str, dict[str, Any]] = {
    # Default lab on RAID (~10 GiB Values). Needs /raid free space; use lab_tiny for smoke.
    # 5001 × 10000 × 2 B ≈ 95.39 MiB Values/part × 108 parts ≈ 10.05 GiB.
    "lab": {
        "n_series": 5001,
        "n_time": 10_000,
        "n_parts": 108,
        "sampling_interval_ms": 0.1,  # 10 kHz → 1 s window (fidelity-shaped)
        "sample_rate_hz_declared": 10_000.0,
        "contiguous": True,
        "description": (
            "Lab (RAID): 5001 × 10000 × 108 parts ≈ 10.05 GiB Values (int16). "
            "Interesting multi-file Dask/datashader workload; store under RUSTFS_DATA_DIR on /raid."
        ),
    },
    # Quick smoke / CI-sized (seconds, not GiB).
    "lab_tiny": {
        "n_series": 256,
        "n_time": 500,
        "n_parts": 24,
        "sampling_interval_ms": 2.0,
        "sample_rate_hz_declared": 10_000.0,
        "contiguous": True,
        "description": (
            "Tiny smoke: 256 × 500 × 24 parts ≈ 6.1 MiB Values. Fast notebook/CI check."
        ),
    },
    # Large air-gap: ~2 TiB of int16 Values using fidelity-shaped parts.
    # 5001 × 10000 × 2 B ≈ 95.4 MiB Values/part → 21_500 parts ≈ 2.00 TiB.
    "airgap_2tb": {
        "n_series": 5001,
        "n_time": 10_000,
        "n_parts": 21_500,
        "sampling_interval_ms": 0.1,  # 10 kHz → 1 s window
        "sample_rate_hz_declared": 10_000.0,
        "contiguous": True,
        "description": (
            "Air-gap scale: 5001 × 10000 int16 × 21500 parts ≈ 2.00 TiB Values. "
            "Needs significant S3-like capacity + cluster RAM for out-of-core Dask."
        ),
    },
}


@dataclass
class GenConfig:
    """Generation configuration (notebook- and CLI-friendly)."""

    n_series: int = 5001
    n_time: int = 10_000
    n_parts: int = 108
    contiguous: bool = True
    chunk_series: int = 512
    chunk_time: int = 2048
    sampling_interval_ms: float = 2.0
    sample_rate_hz_declared: float = 10_000.0
    dtype: str = DEFAULT_DTYPE
    value_unit: str = "normalized_units"
    scale_factor: float = DEFAULT_SCALE_FACTOR
    service_name: str = "cphy-collector"
    service_namespace: str = "cyberphy.otel"
    # Acquisition-level start (full fiber); Metric_0 uses a sub-window offset
    start_series_index: int = 3489
    metric_start_series_index: int = 3500  # ≠ start_series_index (sub-window parity)
    export_timeout_s: float = 30.0
    batch_max_size: int = 8192
    spatial_resolution: float = 1.021  # meters per series index step
    sensing_span: float = 2.0  # meters (instrument sense window)
    seed: int = 0
    time_unit: TimeUnit = "ns"  # OTel-native; set "us" for microsecond timestamps
    group_style: GroupStyle = "underscore"  # URL/HOCON-safe
    product: str = "cphy"
    # Object keys: datasets/hdf5/<product>/<filename> (Iceberg data-file style; not Hive)
    # Time/partition semantics live in file attrs + metadata tables (Arrow/Iceberg), not paths.
    product_prefix: bool = True
    base_time: Optional[datetime] = None  # UTC; default = now

    def estimated_values_bytes(self) -> int:
        item = np.dtype(self.dtype).itemsize
        return int(self.n_series) * int(self.n_time) * item * int(self.n_parts)

    def estimated_values_tib(self) -> float:
        return self.estimated_values_bytes() / (1024**4)


def profile_config(name: str, **overrides: Any) -> GenConfig:
    """Build GenConfig from a named profile, with optional field overrides."""
    if name not in PROFILES and name != "custom":
        raise ValueError(f"Unknown profile {name!r}; choose from {list(PROFILES)}")
    base = PROFILES.get(name, {})
    cfg = GenConfig(
        n_series=int(base.get("n_series", 256)),
        n_time=int(base.get("n_time", 500)),
        n_parts=int(base.get("n_parts", 1)),
        contiguous=bool(base.get("contiguous", True)),
        sampling_interval_ms=float(base.get("sampling_interval_ms", 2.0)),
        sample_rate_hz_declared=float(base.get("sample_rate_hz_declared", 10_000.0)),
    )
    for k, v in overrides.items():
        if v is not None and hasattr(cfg, k):
            setattr(cfg, k, v)
    return cfg


# ---------------------------------------------------------------------------
# Attribute helpers
# ---------------------------------------------------------------------------
def _fix(node, name: str, s: str) -> None:
    """Fixed-length ASCII |S attr (reference-faithful; avoid h5py vlen UTF-8)."""
    b = s.encode("ascii", "replace")
    node.attrs.create(name, np.bytes_(b), dtype=np.dtype(f"S{max(1, len(b))}"))


def _pair(node, name: str, value: float, unit: str) -> None:
    """float64 value + '.unit' sibling (GaugeLength / GaugeLength.uom pattern)."""
    node.attrs.create(name, np.float64(value))
    _fix(node, name + ".unit", unit)


def _uuid() -> str:
    return str(uuid.uuid4())


def _iso(ns: int) -> str:
    """ISO-8601 micros + UTC → 32 chars → |S32."""
    dt = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=ns // 1000)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "+00:00"


def _iso_compact(ns: int) -> str:
    """Filename component YYYYMMDDTHHMMSS (UTC)."""
    dt = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=ns // 1000)
    return dt.strftime("%Y%m%dT%H%M%S")


def _gname(base: str, index: int, style: GroupStyle) -> str:
    if style == "brackets":
        return f"{base}[{index}]"
    return f"{base}_{index}"


# ---------------------------------------------------------------------------
# Signal model
# ---------------------------------------------------------------------------
def synth_signal(n_series: int, n_time: int, rng: np.random.Generator, dtype: str) -> np.ndarray:
    """Spatio-temporal structure for datashader (baseline + periodic + cascades).

    Cascades model CPHY phenomena: correlated anomalies propagating along a
    contiguous band of series (sensor chain / conduit segment / joint group).
    """
    t = np.arange(n_time, dtype=np.float32)
    base_level = rng.uniform(40, 120, size=(n_series, 1)).astype(np.float32)
    sig = base_level + rng.normal(0, 8, size=(n_series, n_time)).astype(np.float32)

    n_cycles = float(rng.uniform(2, 6))
    phase = rng.uniform(0, 2 * np.pi, size=(n_series, 1)).astype(np.float32)
    sig += 15.0 * np.sin(2 * np.pi * n_cycles * t / max(n_time, 1) + phase).astype(np.float32)

    n_events = max(1, int(n_series * n_time / 5_000_000) + int(rng.integers(1, 4)))
    for _ in range(n_events):
        s0 = int(rng.integers(0, n_series))
        band = int(rng.integers(20, max(21, n_series // 8)))
        t0 = int(rng.integers(0, n_time))
        offset = float(rng.uniform(0.2, 2.0))
        width = int(rng.integers(30, 200))
        amp = float(rng.uniform(60, 200))
        for k in range(band):
            s = s0 + k
            if s >= n_series:
                break
            c = int(t0 + k * offset)
            lo, hi = max(0, c - width), min(n_time, c + width)
            if lo >= hi:
                continue
            env = amp * np.exp(-((np.arange(lo, hi) - c) ** 2) / (2 * (width / 3.0) ** 2))
            sig[s, lo:hi] += env.astype(np.float32)

    if np.dtype(dtype).kind in ("i", "u"):
        info = np.iinfo(dtype)
        sig = np.clip(np.rint(sig), info.min, info.max)
    return sig.astype(dtype)


# ---------------------------------------------------------------------------
# HDF5 writer
# ---------------------------------------------------------------------------
def _create_values_dataset(group, name: str, shape, dtype: str, chunks):
    """Primary 2D array. chunks=None → CONTIGUOUS (reference / Iceberg-friendly)."""
    if chunks is None:
        return group.create_dataset(name, shape=tuple(shape), dtype=np.dtype(dtype))
    space = h5py.h5s.create_simple(tuple(shape))
    dcpl = h5py.h5p.create(h5py.h5p.DATASET_CREATE)
    dcpl.set_chunk(tuple(chunks))
    dcpl.set_fill_time(h5py.h5d.FILL_TIME_NEVER)
    tid = h5py.h5t.py_create(np.dtype(dtype), logical=True)
    dsid = h5py.h5d.create(group.id, name.encode("utf-8"), tid, space, dcpl)
    return h5py.Dataset(dsid)


def part_time_bounds(cfg: GenConfig, part_idx: int) -> tuple[int, int, int]:
    """Return (start_ns, end_ns, step_ns) for a part (always computed in ns)."""
    step_ns = int(round(cfg.sampling_interval_ms * 1_000_000))
    if step_ns < 1:
        step_ns = 1
    if cfg.base_time is not None:
        base_ns = int(cfg.base_time.replace(tzinfo=timezone.utc).timestamp() * 1e9)
    else:
        base_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    # Align base to whole seconds for clean filenames
    base_ns = (base_ns // 1_000_000_000) * 1_000_000_000
    start_ns = base_ns + part_idx * cfg.n_time * step_ns
    end_ns = start_ns + cfg.n_time * step_ns
    return start_ns, end_ns, step_ns


def timestamps_array(start_ns: int, n_time: int, step_ns: int, time_unit: TimeUnit) -> np.ndarray:
    """1D int64 index in ns (default) or µs."""
    if time_unit == "us":
        start = start_ns // 1000
        step = max(1, step_ns // 1000)
        return start + np.arange(n_time, dtype=np.int64) * step
    return start_ns + np.arange(n_time, dtype=np.int64) * step_ns


def part_filename(cfg: GenConfig, part_idx: int, start_ns: int) -> str:
    """cphy_<YYYYMMDDTHHMMSS>_<part>Z.h5"""
    return f"{cfg.product}_{_iso_compact(start_ns)}_{part_idx:04d}Z.h5"


def object_key(cfg: GenConfig, part_idx: int, start_ns: int, filename: str) -> str:
    """Relative object key under bucket (no leading slash).

    Flat product prefix — Iceberg/Arrow metadata carry time bounds and tags,
    not Hive-style service=/date=/hour= directories.
    """
    if not cfg.product_prefix:
        return filename
    return f"datasets/hdf5/{cfg.product}/{filename}"


# Backward-compatible alias (deprecated)
def hive_key(cfg: GenConfig, part_idx: int, start_ns: int, filename: str) -> str:
    return object_key(cfg, part_idx, start_ns, filename)


def structural_fingerprint(cfg: GenConfig, collection_uuid: str) -> str:
    """Idempotency key: layout geometry + uuid (not payload bytes)."""
    payload = json.dumps(
        {
            "uuid": collection_uuid,
            "n_series": cfg.n_series,
            "n_time": cfg.n_time,
            "dtype": cfg.dtype,
            "contiguous": cfg.contiguous,
            "time_unit": cfg.time_unit,
            "group_style": cfg.group_style,
            "schema": SCHEMA_VERSION,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def write_part(
    path: str,
    part_idx: int,
    cfg: GenConfig,
    rng: np.random.Generator,
) -> dict[str, Any]:
    """Write one acquisition-window HDF5 file. Returns inventory metadata."""
    n_series, n_time = cfg.n_series, cfg.n_time
    chunks = (
        None
        if cfg.contiguous
        else (min(cfg.chunk_series, n_series), min(cfg.chunk_time, n_time))
    )
    start_ns, end_ns, step_ns = part_time_bounds(cfg, part_idx)
    si_s = cfg.sampling_interval_ms / 1000.0
    declared_hz = cfg.sample_rate_hz_declared or (1.0 / si_s if si_s else 0.0)

    collection_uuid = _uuid()
    collection_id = _uuid()
    metric_uuid = _uuid()
    layout = "contiguous" if cfg.contiguous else "chunked"

    with h5py.File(path, "w", libver="latest") as f:
        # --- root -------------------------------------------------------------
        _fix(f, "collection.uuid", collection_uuid)
        _fix(f, "product", cfg.product)
        _fix(f, "domain", "cphy")

        # --- ResourceMetrics (≈ Acquisition physics) --------------------------
        rm = f.create_group("ResourceMetrics")
        _pair(rm, "collection.interval", si_s * n_time, "s")  # window duration
        _pair(rm, "export.timeout", float(cfg.export_timeout_s), "s")
        _pair(rm, "sample.rate.max", float(declared_hz), "Hz")
        _pair(rm, "sample.rate.min", 0.0, "Hz")
        _pair(rm, "batch.flush.latency", float(rng.uniform(1.0, 9.0)), "ms")
        _pair(rm, "spatial.resolution", float(cfg.spatial_resolution), "m")
        _pair(rm, "sensing.span", float(cfg.sensing_span), "m")
        rm.attrs.create("series.count", np.int64(n_series))
        rm.attrs.create("start.series.index", np.int64(cfg.start_series_index))
        rm.attrs.create("aggregation.temporality.is_delta", np.bool_(False))
        _fix(rm, "schema.url", OTEL_SCHEMA_URL)
        _fix(rm, "collection.id", collection_id)
        _fix(rm, "service.name", cfg.service_name)
        _fix(rm, "service.instance.id", _uuid())
        _fix(rm, "start.time", _iso(start_ns))
        _fix(rm, "end.time", _iso(end_ns))
        _fix(rm, "otel.schema.version", SCHEMA_VERSION)
        _fix(rm, "band.low", "0")
        _fix(rm, "band.high", "5000")
        _fix(rm, "band.high.unit", "Hz")

        # --- Scope (instrumentation / collector config) -----------------------
        scope = rm.create_group("Scope")
        scope.attrs.create("batch.max.size", np.int32(cfg.batch_max_size))
        scope.attrs.create("queue.capacity", np.int32(2048))
        scope.attrs.create("export.retry.count", np.int32(5))
        scope.attrs.create("sampling.ratio", np.float64(1.0))
        scope.attrs.create("compression.ratio", np.float64(1.0))  # contiguous: none
        scope.attrs.create("data.transposed", np.bool_(True))  # series-major
        scope.attrs.create("layout.series_major", np.bool_(True))
        scope.attrs.create("values.relative", np.bool_(False))
        scope.attrs.create("temporal.downsampling", np.int32(1))
        scope.attrs.create("instrument.channels", np.int32(4))
        _fix(scope, "telemetry.sdk.name", "opentelemetry")
        _fix(scope, "telemetry.sdk.language", "python")
        _fix(scope, "instrument.serial", f"CPHY-{rng.integers(1000, 9999)}")
        _fix(scope, "instrument.model", "cphy-collector")

        win_dt = np.dtype([("StartSeries", "<i4"), ("EndSeries", "<i4"), ("Stride", "<i4")])
        # A few zone windows along series axis
        n_win = max(1, min(8, n_series // 32))
        edges = np.linspace(0, n_series, n_win + 1).astype(np.int32)
        windows = np.zeros(n_win, dtype=win_dt)
        windows["StartSeries"], windows["EndSeries"], windows["Stride"] = edges[:-1], edges[1:], 1
        scope.create_dataset("Windows", data=windows)

        # --- Resource_0 (site geometry / series position map) -----------------
        res = rm.create_group(_gname("Resource", 0, cfg.group_style))
        _fix(res, "resource.schema.url", OTEL_SCHEMA_URL)
        _fix(res, "service.namespace", cfg.service_namespace)
        _fix(res, "host.name", cfg.service_name + "-host")
        _fix(res, "host.arch", "amd64")
        _fix(res, "reference.datum", "site-origin")

        anchor_dt = np.dtype(
            [
                ("SeriesIndex", "<i8"),
                ("PathDistance", "<f8"),
                ("SiteLength", "<f8"),
            ]
        )
        for ai, note in enumerate(("AnchorPair", "AllSeries")):
            sa = res.create_group(_gname("SeriesAnchor", ai, cfg.group_style))
            _fix(sa, "scope.note", note)
            _fix(sa, "reference.frame", "collection-start")
            _pair(sa, "distance.scale", 1.0, "m")
            npts = 2 if ai == 0 else n_series
            anchor = np.zeros(npts, dtype=anchor_dt)
            # Absolute series indices (acquisition start + offset within window)
            abs_idx = cfg.start_series_index + np.linspace(0, n_series - 1, npts)
            anchor["SeriesIndex"] = abs_idx.astype(np.int64)
            anchor["PathDistance"] = abs_idx * cfg.spatial_resolution
            anchor["SiteLength"] = abs_idx * cfg.spatial_resolution
            sa.create_dataset("SeriesPosition", data=anchor)

        # --- Metric_0 (payload sub-window of the full series range) -----------
        met = rm.create_group(_gname("Metric", 0, cfg.group_style))
        met.attrs.create("series.count", np.int64(n_series))
        # Sub-window: deliberately ≠ ResourceMetrics.start.series.index
        met.attrs.create("start.series.index", np.int64(cfg.metric_start_series_index))
        _pair(met, "output.data.rate", float(declared_hz), "Hz")
        met.attrs.create("scale.factor", np.float64(cfg.scale_factor))
        _fix(
            met,
            "scale.note",
            f"int16 full-scale encoding via scale.factor={cfg.scale_factor}",
        )
        _fix(met, "value.unit", cfg.value_unit)
        _fix(met, "metric.uuid", metric_uuid)
        _fix(met, "metric.name", "cphy.acquisition.signal")

        values = _create_values_dataset(met, "Values", (n_series, n_time), cfg.dtype, chunks)
        stripe = chunks[0] if chunks else max(1, min(n_series, 1024))
        for s0 in range(0, n_series, stripe):
            s1 = min(s0 + stripe, n_series)
            values[s0:s1, :] = synth_signal(s1 - s0, n_time, rng, cfg.dtype)
        values.attrs.create("count", np.int64(n_series) * np.int64(n_time))
        values.attrs.create("start.index", np.int64(0))
        _fix(values, "dimensions", "series,time")
        _fix(values, "part.start.time", _iso(start_ns))
        _fix(values, "part.end.time", _iso(end_ns))
        values.attrs.create("layout.contiguous", np.bool_(cfg.contiguous))

        ts_data = timestamps_array(start_ns, n_time, step_ns, cfg.time_unit)
        ts = met.create_dataset("Timestamps", data=ts_data)
        ts.attrs.create("count", np.int64(n_time))
        ts.attrs.create("start.index", np.int64(part_idx * n_time))  # multi-part stitch
        _fix(ts, "part.start.time", _iso(start_ns))
        _fix(ts, "part.end.time", _iso(end_ns))
        _fix(ts, "start.time", _iso(start_ns))
        _fix(ts, "unit", cfg.time_unit)

    size = os.path.getsize(path)
    fname = part_filename(cfg, part_idx, start_ns)
    key = object_key(cfg, part_idx, start_ns, fname)
    fp = structural_fingerprint(cfg, collection_uuid)
    t_min_ns, t_max_ns = start_ns, end_ns - step_ns
    return {
        "dataset_uuid": collection_uuid,
        "metric_uuid": metric_uuid,
        "collection_id": collection_id,
        "fingerprint": fp,
        "part_idx": part_idx,
        "filename": fname,
        "key": key,
        "local_path": path,
        "size_bytes": size,
        "layout": layout,
        "n_series": n_series,
        "n_time": n_time,
        "dtype": cfg.dtype,
        "t_min_ns": t_min_ns,
        "t_max_ns": t_max_ns,
        "time_unit": cfg.time_unit,
        "service_name": cfg.service_name,
        "start_series_index": cfg.start_series_index,
        "metric_start_series_index": cfg.metric_start_series_index,
    }


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------
def emit_kerchunk(local_path: str, out_json: str, url: str | None = None):
    """Write a kerchunk reference so datasets read as virtual Zarr ranges."""
    try:
        from kerchunk.hdf import SingleHdf5ToZarr
    except ImportError:
        print("  kerchunk not installed; skipping reference (uv run --with kerchunk ...)")
        return None
    with open(local_path, "rb") as fo:
        refs = SingleHdf5ToZarr(fo, url=url or local_path, inline_threshold=0).translate()
    with open(out_json, "w") as f:
        json.dump(refs, f)
    return out_json


def _boto3_s3_client(
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
):
    import boto3
    from botocore.client import Config

    kwargs: dict[str, Any] = {
        "endpoint_url": endpoint,
        "config": Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        "region_name": os.environ.get("AWS_REGION", os.environ.get("S3_REGION", "us-east-1")),
    }
    ak = access_key or os.environ.get("AWS_ACCESS_KEY_ID")
    sk = secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if ak and sk:
        kwargs["aws_access_key_id"] = ak
        kwargs["aws_secret_access_key"] = sk
    return boto3.client("s3", **kwargs)


def upload_s3(
    local_path: str,
    bucket: str,
    key: str,
    endpoint: str | None,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> str:
    _boto3_s3_client(endpoint, access_key, secret_key).upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"


def expected_part_min_bytes(cfg: GenConfig, fraction: float = 0.5) -> int:
    """Minimum object size to count as a 'full' part (Values payload + HDF5 overhead)."""
    item = np.dtype(cfg.dtype).itemsize
    values = int(cfg.n_series) * int(cfg.n_time) * item
    return max(1, int(values * fraction))


def list_existing_parts(
    out: str,
    cfg: GenConfig,
    *,
    s3_endpoint: str | None = None,
    min_size_bytes: int | None = None,
) -> list[dict[str, Any]]:
    """List existing acquisition .h5 objects under the product object prefix.

    Idempotent explore path: discovers prior runs without rewriting data.
    Filters by minimum size so tiny smoke leftovers are ignored when cfg is lab-sized.
    """
    min_sz = expected_part_min_bytes(cfg) if min_size_bytes is None else min_size_bytes
    inventory: list[dict[str, Any]] = []

    if out.startswith("s3://"):
        u = urlparse(out)
        bucket = u.netloc
        prefix = f"datasets/hdf5/{cfg.product}/"
        client = _boto3_s3_client(s3_endpoint)
        token = None
        keys: list[tuple[str, int]] = []
        while True:
            kw: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = client.list_objects_v2(**kw)
            for obj in resp.get("Contents") or []:
                key = obj["Key"]
                if not key.rstrip("/").endswith("Z.h5") and not key.endswith(".h5"):
                    # RustFS may list leaf objects; accept keys that look like part files
                    if ".h5" not in key:
                        continue
                # Prefer the object key that ends with .h5 (directory layout stores children)
                if not (key.endswith(".h5") or key.endswith("Z.h5")):
                    continue
                size = int(obj.get("Size") or 0)
                if size < min_sz:
                    continue
                keys.append((key, size))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")

        # Deduplicate and sort
        seen: set[str] = set()
        ordered: list[tuple[str, int]] = []
        for key, size in sorted(keys):
            if key in seen:
                continue
            seen.add(key)
            ordered.append((key, size))

        for i, (key, size) in enumerate(ordered):
            fname = key.rsplit("/", 1)[-1]
            inventory.append(
                {
                    "dataset_uuid": None,
                    "fingerprint": hashlib.sha256(key.encode()).hexdigest()[:16],
                    "part_idx": i,
                    "filename": fname,
                    "key": key,
                    "uri": f"s3://{bucket}/{key}",
                    "size_bytes": size,
                    "layout": "contiguous" if cfg.contiguous else "chunked",
                    "n_series": cfg.n_series,
                    "n_time": cfg.n_time,
                    "dtype": cfg.dtype,
                    "t_min_ns": None,
                    "t_max_ns": None,
                    "time_unit": cfg.time_unit,
                    "service_name": cfg.service_name,
                    "reused": True,
                }
            )
        return inventory

    # Local filesystem
    root = Path(out)
    if cfg.product_prefix:
        search_root = root / "datasets" / "hdf5" / cfg.product
    else:
        search_root = root
    if not search_root.exists():
        return []
    for i, path in enumerate(sorted(search_root.rglob("*Z.h5"))):
        size = path.stat().st_size
        if size < min_sz:
            continue
        rel = str(path.relative_to(root)) if cfg.product_prefix else path.name
        inventory.append(
            {
                "dataset_uuid": None,
                "fingerprint": hashlib.sha256(str(path).encode()).hexdigest()[:16],
                "part_idx": i,
                "filename": path.name,
                "key": rel,
                "local_path": str(path),
                "uri": f"file://{path}",
                "size_bytes": size,
                "layout": "contiguous" if cfg.contiguous else "chunked",
                "n_series": cfg.n_series,
                "n_time": cfg.n_time,
                "dtype": cfg.dtype,
                "t_min_ns": None,
                "t_max_ns": None,
                "time_unit": cfg.time_unit,
                "service_name": cfg.service_name,
                "reused": True,
            }
        )
    return inventory


def ensure_parts(
    cfg: GenConfig,
    out: str,
    *,
    s3_endpoint: str | None = None,
    emit_kerchunk_refs: bool = False,
    dry_run: bool = False,
    progress_every: int = 1,
    reuse_existing: bool = True,
    force: bool = False,
) -> list[dict[str, Any]]:
    """Idempotent ensure: reuse existing full-size parts when present.

    Default ``reuse_existing=True``: if at least ``cfg.n_parts`` objects already
    exist at the expected size, return them and write nothing. Set ``force=True``
    to always generate a new run (new timestamps / keys).

    Reuse is a cheap S3/list (typically well under a second for a few hundred
    objects). If this function takes minutes, it is *generating*, not reusing —
    check the log line after the list step.
    """
    import time as _time

    t0 = _time.time()
    print(
        f"Profile geometry: {cfg.n_parts} parts × {cfg.n_series}×{cfg.n_time} "
        f"{cfg.dtype} ≈ {cfg.estimated_values_bytes() / 1e6:.1f} MB Values "
        f"({cfg.estimated_values_tib():.3f} TiB)  layout="
        f"{'contiguous' if cfg.contiguous else 'chunked'}  time_unit={cfg.time_unit}"
    )
    print(
        f"Idempotency: reuse_existing={reuse_existing} force={force}  "
        f"(FORCE_REGENERATE / --force bypasses reuse)"
    )

    if reuse_existing and not force and not dry_run:
        print("Listing existing full-size parts under product prefix (no rewrite)…")
        t_list = _time.time()
        existing = list_existing_parts(out, cfg, s3_endpoint=s3_endpoint)
        print(f"  list done in {_time.time() - t_list:.2f}s → {len(existing)} candidate(s)")
        if len(existing) >= cfg.n_parts:
            chosen = existing[: cfg.n_parts]
            total = sum(m["size_bytes"] for m in chosen)
            print(
                f"Idempotent reuse: found {len(existing)} full-size part(s); "
                f"using {len(chosen)} ({total / 1e9:.2f} GB) in {_time.time() - t0:.2f}s total. "
                f"Set force=True / FORCE_REGENERATE to write new data."
            )
            for m in chosen[:3]:
                print(f"  reuse {m['uri']}")
            if len(chosen) > 3:
                print(f"  … and {len(chosen) - 3} more")
            return chosen
        if existing:
            print(
                f"Found {len(existing)} existing full-size part(s) "
                f"(need {cfg.n_parts}); generating a full new set."
            )
        else:
            print("No existing full-size parts found; generating.")

    if force:
        print("force=True → writing a new run (this is the slow path).")
    inv = generate_parts(
        cfg,
        out,
        s3_endpoint=s3_endpoint,
        emit_kerchunk_refs=emit_kerchunk_refs,
        dry_run=dry_run,
        progress_every=progress_every,
    )
    print(f"generate_parts finished in {_time.time() - t0:.1f}s ({len(inv)} file(s))")
    return inv


def generate_parts(
    cfg: GenConfig,
    out: str,
    *,
    s3_endpoint: str | None = None,
    emit_kerchunk_refs: bool = False,
    dry_run: bool = False,
    progress_every: int = 1,
) -> list[dict[str, Any]]:
    """Generate all parts to local dir or s3://bucket[/prefix] (always writes).

    Prefer :func:`ensure_parts` for notebook/CI idempotency. For s3:// URLs the
    object key is absolute under the bucket when ``cfg.product_prefix`` is True.
    """
    is_s3 = out.startswith("s3://")
    if is_s3:
        u = urlparse(out)
        bucket, url_prefix = u.netloc, u.path.lstrip("/")
        if url_prefix and not url_prefix.endswith("/"):
            url_prefix += "/"
        workdir = tempfile.mkdtemp(prefix="hdf5-cphy-")
    else:
        bucket, url_prefix = "", ""
        workdir = out
        os.makedirs(workdir, exist_ok=True)

    if dry_run:
        start_ns, _, _ = part_time_bounds(cfg, 0)
        fname = part_filename(cfg, 0, start_ns)
        print(f"Dry-run sample key: {hive_key(cfg, 0, start_ns, fname)}")
        print(f"Out: {out}")
        return []

    inventory: list[dict[str, Any]] = []
    for i in range(cfg.n_parts):
        rng = np.random.default_rng(cfg.seed + i)
        start_ns, _, _ = part_time_bounds(cfg, i)
        fname = part_filename(cfg, i, start_ns)
        local = os.path.join(workdir, fname)
        meta = write_part(local, i, cfg, rng)
        meta["reused"] = False

        if is_s3:
            key = meta["key"] if cfg.product_prefix else f"{url_prefix}{fname}"
            uri = upload_s3(local, bucket, key, s3_endpoint)
            meta["uri"] = uri
            meta["key"] = key
            if emit_kerchunk_refs:
                ref_local = local + ".kerchunk.json"
                if emit_kerchunk(local, ref_local, url=uri):
                    ref_key = f"indexes/kerchunk/{meta['fingerprint']}.json"
                    upload_s3(ref_local, bucket, ref_key, s3_endpoint)
                    meta["ref_uri"] = f"s3://{bucket}/{ref_key}"
            try:
                os.remove(local)
            except OSError:
                pass
        else:
            # Local product-prefix dirs
            if cfg.product_prefix:
                dest = os.path.join(workdir, meta["key"])
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                if dest != local:
                    os.replace(local, dest)
                    meta["local_path"] = dest
            meta["uri"] = f"file://{meta['local_path']}"
            if emit_kerchunk_refs:
                ref_path = meta["local_path"] + ".kerchunk.json"
                if emit_kerchunk(meta["local_path"], ref_path, url=meta["uri"]):
                    meta["ref_uri"] = f"file://{ref_path}"

        inventory.append(meta)
        if (i + 1) % max(1, progress_every) == 0 or i == 0:
            print(
                f"  [{i + 1}/{cfg.n_parts}] {meta.get('filename')}  "
                f"({meta['size_bytes'] / 1e6:.2f} MB) → {meta.get('uri', meta.get('key'))}"
            )

    print(f"Done. {len(inventory)} file(s).")
    return inventory


def inventory_to_pointer_rows(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Shape inventory as telemetry.hdf5_datasets pointer-table rows."""
    rows = []
    for m in inventory:
        rows.append(
            {
                "dataset_uuid": m["dataset_uuid"],
                "fingerprint": m["fingerprint"],
                "uri": m.get("uri", m.get("key")),
                "size_bytes": m["size_bytes"],
                "layout": m["layout"],
                "n_series": m["n_series"],
                "n_time": m["n_time"],
                "dtype": m["dtype"],
                "t_min_ns": m["t_min_ns"],
                "t_max_ns": m["t_max_ns"],
                "ref_uri": m.get("ref_uri"),
                "service_name": m.get("service_name"),
                "part_idx": m["part_idx"],
            }
        )
    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(
        description="Generate CPHY/OTel-framed HDF5 acquisition windows for Iceberg-ready layout testing."
    )
    p.add_argument(
        "--profile",
        choices=["lab", "lab_tiny", "airgap_2tb", "custom"],
        default="lab",
        help="lab (~10 GiB RAID) | lab_tiny (~6 MiB) | airgap_2tb (~2 TiB) | custom",
    )
    p.add_argument("--out", required=True, help="Output dir or s3://bucket[/prefix]")
    p.add_argument("--n-series", type=int, default=None)
    p.add_argument("--n-time", type=int, default=None)
    p.add_argument("--n-parts", type=int, default=None)
    p.add_argument("--chunk-series", type=int, default=512)
    p.add_argument("--chunk-time", type=int, default=2048)
    p.add_argument("--contiguous", action="store_true", default=None)
    p.add_argument("--chunked", action="store_true", help="Force chunked layout")
    p.add_argument("--sampling-interval-ms", type=float, default=None)
    p.add_argument("--dtype", default=None)
    p.add_argument("--service-name", default=None)
    p.add_argument("--service-namespace", default=None)
    p.add_argument("--time-unit", choices=["ns", "us"], default="ns")
    p.add_argument("--group-style", choices=["underscore", "brackets"], default="underscore")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--emit-kerchunk", action="store_true")
    p.add_argument("--flat", action="store_true", help="Keys = filename only (no datasets/hdf5/<product>/ prefix)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--force",
        action="store_true",
        help="Always generate new parts (skip idempotent reuse of existing objects)",
    )
    p.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT") or None)
    p.add_argument("--progress-every", type=int, default=1)
    args = p.parse_args()

    overrides: dict[str, Any] = {
        "time_unit": args.time_unit,
        "group_style": args.group_style,
        "seed": args.seed,
        "chunk_series": args.chunk_series,
        "chunk_time": args.chunk_time,
        "product_prefix": not args.flat,
    }
    for key, attr in [
        ("n_series", "n_series"),
        ("n_time", "n_time"),
        ("n_parts", "n_parts"),
        ("sampling_interval_ms", "sampling_interval_ms"),
        ("dtype", "dtype"),
        ("service_name", "service_name"),
        ("service_namespace", "service_namespace"),
    ]:
        val = getattr(args, attr.replace("-", "_") if False else attr, None)
        # argparse uses underscores
        pass
    if args.n_series is not None:
        overrides["n_series"] = args.n_series
    if args.n_time is not None:
        overrides["n_time"] = args.n_time
    if args.n_parts is not None:
        overrides["n_parts"] = args.n_parts
    if args.sampling_interval_ms is not None:
        overrides["sampling_interval_ms"] = args.sampling_interval_ms
    if args.dtype is not None:
        overrides["dtype"] = args.dtype
    if args.service_name is not None:
        overrides["service_name"] = args.service_name
    if args.service_namespace is not None:
        overrides["service_namespace"] = args.service_namespace
    if args.chunked:
        overrides["contiguous"] = False
    elif args.contiguous:
        overrides["contiguous"] = True

    if args.profile == "custom":
        cfg = GenConfig(**{k: v for k, v in overrides.items() if v is not None})
    else:
        cfg = profile_config(args.profile, **overrides)

    if args.profile in PROFILES:
        print(f"Profile {args.profile}: {PROFILES[args.profile]['description']}")

    inv = ensure_parts(
        cfg,
        args.out,
        s3_endpoint=args.s3_endpoint,
        emit_kerchunk_refs=args.emit_kerchunk,
        dry_run=args.dry_run,
        progress_every=args.progress_every,
        reuse_existing=not args.force,
        force=args.force,
    )
    if inv:
        rows = inventory_to_pointer_rows(inv)
        inv_path = None
        if not args.out.startswith("s3://"):
            inv_path = os.path.join(args.out, "_inventory.json")
            with open(inv_path, "w") as f:
                json.dump(rows, f, indent=2)
            print(f"Pointer-table preview: {inv_path} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
