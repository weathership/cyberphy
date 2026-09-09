"""DatasetProvider — read-only access to Layer A (HDF5 data plane)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence, Union

from hdf5_iceberg.adapters import get_adapter
from hdf5_iceberg.adapters.base import Candidate, LayoutAdapter
from hdf5_iceberg.audit import audit_uri
from hdf5_iceberg.descriptor import DatasetDescriptor
from hdf5_iceberg.fsutil import is_s3, open_fs


@dataclass
class DatasetProvider:
    """RO provider for one or more dataset roots.

    Never writes. Strict mode refuses non-empty write credentials being used
    for metadata (credentials here are only for GET on the data plane).
    """

    roots: Sequence[str]
    readonly: bool = True
    adapter: Union[str, LayoutAdapter] = "flat_prefix"
    endpoint_url: Optional[str] = None
    key: Optional[str] = None
    secret: Optional[str] = None
    token: Optional[str] = None
    min_size_bytes: int = 1
    _adapter_obj: Optional[LayoutAdapter] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.readonly:
            raise ValueError(
                "DatasetProvider is read-only by design; use MetadataProvider for writes"
            )
        if isinstance(self.adapter, str):
            self._adapter_obj = get_adapter(self.adapter)
        else:
            self._adapter_obj = self.adapter
        # env fallbacks for lab
        self.endpoint_url = self.endpoint_url or os.environ.get("S3_ENDPOINT") or None
        self.key = self.key or os.environ.get("AWS_ACCESS_KEY_ID") or None
        self.secret = self.secret or os.environ.get("AWS_SECRET_ACCESS_KEY") or None
        self.token = self.token or os.environ.get("AWS_SESSION_TOKEN") or None

    @property
    def layout_adapter(self) -> LayoutAdapter:
        assert self._adapter_obj is not None
        return self._adapter_obj

    def _fs(self) -> Any:
        needs_s3 = any(is_s3(r) for r in self.roots)
        if not needs_s3:
            return None
        return open_fs(
            endpoint_url=self.endpoint_url,
            key=self.key,
            secret=self.secret,
            token=self.token,
        )

    def list_candidates(self, *, max_files: Optional[int] = None) -> list[Candidate]:
        fs = self._fs()
        out: list[Candidate] = []
        for root in self.roots:
            out.extend(
                self.layout_adapter.list_candidates(
                    root,
                    fs=fs,
                    min_size_bytes=self.min_size_bytes,
                    max_files=(
                        None
                        if max_files is None
                        else max(0, max_files - len(out))
                    ),
                )
            )
            if max_files is not None and len(out) >= max_files:
                break
        return out

    def audit(self, uri: str) -> DatasetDescriptor:
        return audit_uri(
            uri,
            endpoint_url=self.endpoint_url,
            key=self.key,
            secret=self.secret,
            token=self.token,
        )

    def discover(
        self,
        *,
        max_files: Optional[int] = None,
        audit: bool = True,
    ) -> list[DatasetDescriptor]:
        """List candidates; optionally audit each file (geometry/time/fingerprint)."""
        cands = self.list_candidates(max_files=max_files)
        if not audit:
            from hdf5_iceberg.adapters.base import candidate_to_stub

            return [candidate_to_stub(c) for c in cands]

        descs: list[DatasetDescriptor] = []
        for c in cands:
            d = self.audit(c.uri)
            d.tags = {**c.tags, **d.tags}
            d.key = d.key or c.key
            if c.size_bytes:
                d.size_bytes = c.size_bytes
            descs.append(d)
        return descs
