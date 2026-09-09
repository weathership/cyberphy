"""MetadataProvider — isolated Layer B warehouse (all writes under a chosen prefix)."""

from __future__ import annotations

import io
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, Sequence
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq

from hdf5_iceberg.descriptor import DatasetDescriptor
from hdf5_iceberg.fsutil import is_s3, join_uri, open_fs, parse_s3_uri
from hdf5_iceberg.semantic.dcat_ttl import emit_dcat_ttl, emit_shacl_shapes


@dataclass
class MetadataProvider:
    """RW metadata plane.

    All write paths are constrained under ``warehouse`` (e.g.
    ``s3://cyberphy-md/iceberg/warehouse``). Dataset plane URIs are never written.
    """

    warehouse: str
    endpoint_url: Optional[str] = None
    key: Optional[str] = None
    secret: Optional[str] = None
    token: Optional[str] = None
    namespace: str = "telemetry"
    table_name: str = "hdf5_datasets"
    # relative keys under warehouse
    pointer_relpath: str = "telemetry/hdf5_datasets/parts.parquet"
    semantic_relpath: str = "semantic"
    _fs: Any = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.warehouse = self.warehouse.rstrip("/")
        self.endpoint_url = self.endpoint_url or os.environ.get("S3_ENDPOINT") or None
        self.key = self.key or os.environ.get("AWS_ACCESS_KEY_ID") or None
        self.secret = self.secret or os.environ.get("AWS_SECRET_ACCESS_KEY") or None
        self.token = self.token or os.environ.get("AWS_SESSION_TOKEN") or None
        if is_s3(self.warehouse):
            self._fs = open_fs(
                endpoint_url=self.endpoint_url,
                key=self.key,
                secret=self.secret,
                token=self.token,
            )

    def assert_write_uri(self, uri: str) -> None:
        """Raise if uri is not under the warehouse root (write isolation)."""
        wh = self.warehouse.rstrip("/") + "/"
        u = uri.rstrip("/")
        if u == self.warehouse.rstrip("/") or u.startswith(wh):
            return
        # also allow warehouse without trailing path for bucket root tables
        if is_s3(self.warehouse) and is_s3(uri):
            wb, wk = parse_s3_uri(self.warehouse)
            ub, uk = parse_s3_uri(uri)
            if ub == wb and (not wk or uk.startswith(wk.rstrip("/") + "/") or uk == wk):
                return
        raise PermissionError(
            f"write refused: {uri!r} is outside MetadataProvider warehouse {self.warehouse!r}"
        )

    def _s3_path(self, rel: str) -> str:
        if not is_s3(self.warehouse):
            from pathlib import Path

            p = Path(self.warehouse.removeprefix("file://")) / rel
            return str(p)
        bucket, prefix = parse_s3_uri(self.warehouse)
        key = f"{prefix.rstrip('/')}/{rel}".lstrip("/")
        return f"{bucket}/{key}"

    def pointer_table_uri(self) -> str:
        return join_uri(self.warehouse, self.pointer_relpath)

    def load_pointer_rows(self) -> list[dict[str, Any]]:
        """Load existing pointer table if present."""
        uri = self.pointer_table_uri()
        try:
            if is_s3(uri):
                path = self._s3_path(self.pointer_relpath)
                if not self._fs.exists(path):
                    return []
                with self._fs.open(path, "rb") as f:
                    table = pq.read_table(f)
            else:
                from pathlib import Path

                p = Path(self.pointer_relpath)
                if not p.is_absolute():
                    p = Path(self.warehouse.removeprefix("file://")) / self.pointer_relpath
                if not p.exists():
                    return []
                table = pq.read_table(p)
            return table.to_pylist()
        except Exception:
            return []

    def existing_fingerprints(self) -> set[str]:
        rows = self.load_pointer_rows()
        return {str(r["fingerprint"]) for r in rows if r.get("fingerprint")}

    def existing_uris(self) -> set[str]:
        rows = self.load_pointer_rows()
        return {str(r["uri"]) for r in rows if r.get("uri")}

    def write_pointer_table(self, descriptors: Sequence[DatasetDescriptor]) -> str:
        """Write/replace pointer-table parquet under warehouse. Returns table URI."""
        rows = [d.to_pointer_row() for d in descriptors]
        # merge with existing by fingerprint
        by_fp: dict[str, dict[str, Any]] = {}
        for r in self.load_pointer_rows():
            fp = r.get("fingerprint")
            if fp:
                by_fp[str(fp)] = r
        for r in rows:
            fp = r.get("fingerprint")
            if fp:
                by_fp[str(fp)] = r
        merged = list(by_fp.values())
        table = pa.Table.from_pylist(merged) if merged else pa.table({"uri": pa.array([], type=pa.string())})

        uri = self.pointer_table_uri()
        self.assert_write_uri(uri)

        if is_s3(uri):
            path = self._s3_path(self.pointer_relpath)
            parent = path.rsplit("/", 1)[0]
            try:
                self._fs.makedirs(parent, exist_ok=True)
            except Exception:
                pass
            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)
            with self._fs.open(path, "wb") as f:
                f.write(buf.read())
        else:
            from pathlib import Path

            p = Path(self.warehouse.removeprefix("file://")) / self.pointer_relpath
            p.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, p)
        return uri

    def write_semantic_stub(
        self,
        descriptors: Sequence[DatasetDescriptor],
        *,
        catalog_iri: str = "https://example.org/hdf5-iceberg/catalog",
    ) -> dict[str, str]:
        """Emit DCAT-shaped Turtle + SHACL-Core shapes under warehouse/semantic/.

        No JSON-LD — kvasir-friendly syntax (TTL / SHACL-Core subset).
        """
        ttl = emit_dcat_ttl(descriptors, catalog_iri=catalog_iri)
        shapes = emit_shacl_shapes()
        written: dict[str, str] = {}
        for name, body in (
            ("catalog.ttl", ttl),
            ("shapes.ttl", shapes),
        ):
            rel = f"{self.semantic_relpath}/{name}"
            uri = join_uri(self.warehouse, rel)
            self.assert_write_uri(uri)
            if is_s3(self.warehouse):
                path = self._s3_path(rel)
                parent = path.rsplit("/", 1)[0]
                try:
                    self._fs.makedirs(parent, exist_ok=True)
                except Exception:
                    pass
                with self._fs.open(path, "wb") as f:
                    f.write(body.encode("utf-8"))
            else:
                from pathlib import Path

                p = Path(self.warehouse.removeprefix("file://")) / rel
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(body, encoding="utf-8")
            written[name] = uri
        return written

    def list_warehouse_keys(self, *, max_keys: int = 50) -> list[str]:
        """List objects under warehouse (for isolation checks in notebooks)."""
        if is_s3(self.warehouse):
            bucket, prefix = parse_s3_uri(self.warehouse)
            base = f"{bucket}/{prefix}".rstrip("/")
            try:
                found = self._fs.find(base)
            except Exception:
                found = []
            return [str(p) for p in found[:max_keys]]
        from pathlib import Path

        root = Path(self.warehouse.removeprefix("file://"))
        if not root.exists():
            return []
        return [str(p) for p in sorted(root.rglob("*")) if p.is_file()][:max_keys]
