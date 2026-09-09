"""flat_prefix — list all **/*.h5 under a root (path tags empty)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from hdf5_iceberg.adapters.base import Candidate
from hdf5_iceberg.fsutil import is_s3, open_fs, parse_s3_uri


class FlatPrefixAdapter:
    name = "flat_prefix"

    def list_candidates(
        self,
        root: str,
        *,
        fs: Any = None,
        min_size_bytes: int = 1,
        max_files: Optional[int] = None,
    ) -> list[Candidate]:
        out: list[Candidate] = []
        if is_s3(root):
            if fs is None:
                raise ValueError("s3 root requires fs=")
            bucket, prefix = parse_s3_uri(root if "://" in root else f"s3://{root}")
            if prefix and not prefix.endswith("/"):
                prefix += "/"
            # s3fs glob
            pattern = f"{bucket}/{prefix}**/*.h5" if prefix else f"{bucket}/**/*.h5"
            try:
                paths = fs.glob(pattern)
            except Exception:
                # fallback list
                paths = [
                    p
                    for p in fs.find(f"{bucket}/{prefix}".rstrip("/"))
                    if str(p).endswith(".h5")
                ]
            for p in sorted(paths):
                p = str(p)
                try:
                    info = fs.info(p)
                    size = int(info.get("size") or info.get("Size") or 0)
                except Exception:
                    size = 0
                if size < min_size_bytes:
                    continue
                key = p.split("/", 1)[-1] if p.startswith(bucket + "/") else p
                out.append(
                    Candidate(
                        uri=f"s3://{bucket}/{key}" if not p.startswith("s3://") else p,
                        size_bytes=size,
                        key=key,
                    )
                )
                if max_files and len(out) >= max_files:
                    break
            return out

        # local
        base = Path(root.removeprefix("file://"))
        if not base.exists():
            return []
        for path in sorted(base.rglob("*.h5")):
            size = path.stat().st_size
            if size < min_size_bytes:
                continue
            out.append(
                Candidate(
                    uri=f"file://{path.resolve()}",
                    size_bytes=size,
                    key=str(path.relative_to(base)),
                )
            )
            if max_files and len(out) >= max_files:
                break
        return out
