"""Lightweight FormatModel façade for contiguous HDF5 Values.

Not a JVM Iceberg FormatModel yet — mirrors the File Format API responsibilities
so registration + Dask readers share one vocabulary.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from hdf5_iceberg.audit import audit_uri
from hdf5_iceberg.descriptor import DatasetDescriptor


@dataclass
class ValuesInfo:
    uri: str
    n_series: int
    n_time: int
    dtype: str
    layout: str
    contiguous: bool

    def virtual_chunk_bytes(self, series_lo: int, series_hi: int, itemsize: int = 2) -> tuple[int, int]:
        """Byte range for series-block hyperslab on contiguous row-major Values."""
        rows = max(0, series_hi - series_lo)
        offset = series_lo * self.n_time * itemsize
        length = rows * self.n_time * itemsize
        return offset, length


@dataclass
class Hdf5FormatModel:
    """Named format plugin entry (registry-ready)."""

    name: str = "hdf5"

    def describe(self, uri: str, **audit_kw: Any) -> DatasetDescriptor:
        return audit_uri(uri, **audit_kw)

    def values_info(self, desc: DatasetDescriptor) -> ValuesInfo:
        if not desc.n_series or not desc.n_time or not desc.dtype:
            raise ValueError("descriptor missing Values geometry")
        return ValuesInfo(
            uri=desc.uri,
            n_series=int(desc.n_series),
            n_time=int(desc.n_time),
            dtype=str(desc.dtype),
            layout=desc.layout,
            contiguous=desc.layout == "contiguous",
        )


def open_values_info(uri: str, **audit_kw: Any) -> ValuesInfo:
    model = Hdf5FormatModel()
    return model.values_info(model.describe(uri, **audit_kw))
