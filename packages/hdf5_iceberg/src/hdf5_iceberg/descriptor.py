"""DatasetDescriptor — stable hand-off between discovery, registration, and semantics."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass
class DatasetDescriptor:
    """One registered (or candidate) HDF5 acquisition window.

    Mirrors the pointer-table row shape in
    ``telemetry.hdf5_datasets`` (see hdf5-iceberg-metadata-plane design).
    """

    uri: str
    size_bytes: int = 0
    mtime: Optional[float] = None
    fingerprint: Optional[str] = None
    dataset_uuid: Optional[str] = None
    layout: str = "contiguous"  # contiguous | chunked
    n_series: Optional[int] = None
    n_time: Optional[int] = None
    dtype: Optional[str] = None
    t_min_ns: Optional[int] = None
    t_max_ns: Optional[int] = None
    ref_uri: Optional[str] = None
    # Soft path-derived tags (optional; never required for correctness)
    tags: dict[str, str] = field(default_factory=dict)
    # Provenance
    registered_at: Optional[str] = None
    audit_json: Optional[str] = None
    key: Optional[str] = None  # object key relative to bucket/root

    def to_pointer_row(self) -> dict[str, Any]:
        """Shape as telemetry.hdf5_datasets pointer-table row."""
        return {
            "dataset_uuid": self.dataset_uuid,
            "fingerprint": self.fingerprint,
            "uri": self.uri,
            "size_bytes": self.size_bytes,
            "mtime": self.mtime,
            "layout": self.layout,
            "n_series": self.n_series,
            "n_time": self.n_time,
            "dtype": self.dtype,
            "t_min_ns": self.t_min_ns,
            "t_max_ns": self.t_max_ns,
            "ref_uri": self.ref_uri,
            "registered_at": self.registered_at
            or datetime.now(timezone.utc).isoformat(),
            "service_name": self.tags.get("service"),
            "key": self.key,
            **{f"tag_{k}": v for k, v in self.tags.items()},
        }

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DatasetDescriptor":
        tags = dict(d.get("tags") or {})
        # recover soft tags from tag_* columns
        for k, v in list(d.items()):
            if k.startswith("tag_") and v is not None:
                tags[k[4:]] = str(v)
        return cls(
            uri=str(d["uri"]),
            size_bytes=int(d.get("size_bytes") or 0),
            mtime=d.get("mtime"),
            fingerprint=d.get("fingerprint"),
            dataset_uuid=d.get("dataset_uuid"),
            layout=str(d.get("layout") or "contiguous"),
            n_series=d.get("n_series"),
            n_time=d.get("n_time"),
            dtype=d.get("dtype"),
            t_min_ns=d.get("t_min_ns"),
            t_max_ns=d.get("t_max_ns"),
            ref_uri=d.get("ref_uri"),
            tags=tags,
            registered_at=d.get("registered_at"),
            audit_json=d.get("audit_json"),
            key=d.get("key"),
        )
