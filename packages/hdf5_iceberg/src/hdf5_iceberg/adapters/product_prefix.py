"""product_prefix — list **/*.h5 under datasets/hdf5/<product>/ (Iceberg-style keys).

No Hive key=value path segments. Soft tags come from filename / file audit only.
"""

from __future__ import annotations

from typing import Any, Optional

from hdf5_iceberg.adapters.base import Candidate
from hdf5_iceberg.adapters.flat_prefix import FlatPrefixAdapter


class ProductPrefixAdapter:
    """Discover acquisitions under a product object prefix."""

    name = "product_prefix"

    def __init__(self, product: str = "cphy") -> None:
        self.product = product
        self._flat = FlatPrefixAdapter()

    def list_candidates(
        self,
        root: str,
        *,
        fs: Any = None,
        min_size_bytes: int = 1,
        max_files: Optional[int] = None,
    ) -> list[Candidate]:
        r = root.rstrip("/")
        if f"datasets/hdf5/{self.product}" in r or r.endswith(f"/{self.product}"):
            scan_root = root
        else:
            scan_root = f"{r}/datasets/hdf5/{self.product}/"

        cands = self._flat.list_candidates(
            scan_root,
            fs=fs,
            min_size_bytes=min_size_bytes,
            max_files=max_files,
        )
        out: list[Candidate] = []
        for c in cands:
            tags = dict(c.tags)
            tags["product"] = self.product
            tags["filename"] = (c.key or c.uri).rsplit("/", 1)[-1]
            out.append(
                Candidate(
                    uri=c.uri,
                    size_bytes=c.size_bytes,
                    key=c.key,
                    tags=tags,
                )
            )
        return out
