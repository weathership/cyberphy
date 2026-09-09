"""LayoutAdapter protocol — discover candidate HDF5 objects under a root."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Protocol, runtime_checkable

from hdf5_iceberg.descriptor import DatasetDescriptor


@dataclass
class Candidate:
    """Pre-audit discovery hit (URI + soft path tags)."""

    uri: str
    size_bytes: int = 0
    key: Optional[str] = None
    tags: dict[str, str] = field(default_factory=dict)


@runtime_checkable
class LayoutAdapter(Protocol):
    name: str

    def list_candidates(
        self,
        root: str,
        *,
        fs: Any = None,
        min_size_bytes: int = 1,
        max_files: Optional[int] = None,
    ) -> list[Candidate]:
        """List .h5 candidates under root (s3:// or local path)."""
        ...


def candidate_to_stub(c: Candidate) -> DatasetDescriptor:
    return DatasetDescriptor(
        uri=c.uri,
        size_bytes=c.size_bytes,
        key=c.key,
        tags=dict(c.tags),
    )
