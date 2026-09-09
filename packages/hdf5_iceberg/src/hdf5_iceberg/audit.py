"""Structural audit of one HDF5 acquisition (geometry + time bounds + fingerprint)."""

from __future__ import annotations

import hashlib
import json
import tempfile
from typing import Any, Optional

from hdf5_iceberg.descriptor import DatasetDescriptor
from hdf5_iceberg.fsutil import is_s3, open_fs, parse_s3_uri


def _decode_attr(val: Any) -> Any:
    if isinstance(val, bytes):
        try:
            return val.decode("utf-8").rstrip("\x00")
        except Exception:
            return val.hex()
    return val


def audit_local_hdf5(path: str, *, uri: Optional[str] = None) -> DatasetDescriptor:
    """Open a local .h5 path and extract pointer-table fields."""
    import h5py
    import numpy as np

    uri = uri or f"file://{path}"
    with h5py.File(path, "r") as f:
        # uuid
        ds_uuid = None
        for k in ("collection.uuid", "uuid", "dataset_uuid"):
            if k in f.attrs:
                ds_uuid = str(_decode_attr(f.attrs[k]))
                break

        rm = f["ResourceMetrics"] if "ResourceMetrics" in f else f
        met = None
        if "Metric_0" in rm:
            met = rm["Metric_0"]
        elif "Metric[0]" in rm:
            met = rm["Metric[0]"]
        else:
            # first group/dataset that looks like Values
            for name in rm:
                obj = rm[name]
                if isinstance(obj, h5py.Group) and "Values" in obj:
                    met = obj
                    break

        n_series = n_time = None
        dtype = None
        layout = "contiguous"
        t_min = t_max = None
        if met is not None and "Values" in met:
            vals = met["Values"]
            shape = vals.shape
            if len(shape) >= 2:
                n_series, n_time = int(shape[0]), int(shape[1])
            dtype = str(vals.dtype)
            layout = "chunked" if vals.chunks is not None else "contiguous"
        if met is not None and "Timestamps" in met:
            ts = met["Timestamps"]
            if ts.shape[0] > 0:
                t_min = int(ts[0])
                t_max = int(ts[-1])

        # structural fingerprint
        parts: list[str] = []

        def walk(name: str, obj: Any) -> None:
            kind = "G" if isinstance(obj, h5py.Group) else "D"
            extra = ""
            if isinstance(obj, h5py.Dataset):
                extra = f":{obj.shape}:{obj.dtype}:c={obj.chunks is not None}"
            parts.append(f"{kind}/{name}#{len(obj.attrs)}{extra}")

        f.visititems(walk)
        fp = hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]

        audit = {
            "n_nodes": len(parts),
            "layout": layout,
            "n_series": n_series,
            "n_time": n_time,
        }

    import os

    st = os.stat(path)
    return DatasetDescriptor(
        uri=uri,
        size_bytes=int(st.st_size),
        mtime=float(st.st_mtime),
        fingerprint=fp,
        dataset_uuid=ds_uuid or fp,
        layout=layout,
        n_series=n_series,
        n_time=n_time,
        dtype=dtype,
        t_min_ns=t_min,
        t_max_ns=t_max,
        audit_json=json.dumps(audit),
    )


def audit_uri(
    uri: str,
    *,
    endpoint_url: Optional[str] = None,
    key: Optional[str] = None,
    secret: Optional[str] = None,
    token: Optional[str] = None,
) -> DatasetDescriptor:
    """Audit s3:// or local file URI."""
    if not is_s3(uri):
        path = uri.removeprefix("file://")
        return audit_local_hdf5(path, uri=uri if uri.startswith("file:") else f"file://{path}")

    fs = open_fs(endpoint_url=endpoint_url, key=key, secret=secret, token=token)
    bucket, obj_key = parse_s3_uri(uri)
    s3_path = f"{bucket}/{obj_key}"
    info = fs.info(s3_path)
    size = int(info.get("size") or info.get("Size") or 0)
    mtime = info.get("LastModified")
    mtime_f = mtime.timestamp() if hasattr(mtime, "timestamp") else None

    with tempfile.NamedTemporaryFile(suffix=".h5") as tmp:
        fs.get(s3_path, tmp.name)
        desc = audit_local_hdf5(tmp.name, uri=uri)
    desc.size_bytes = size or desc.size_bytes
    if mtime_f is not None:
        desc.mtime = mtime_f
    desc.key = obj_key
    return desc
