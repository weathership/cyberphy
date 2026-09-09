"""Filesystem helpers (fsspec / s3fs) for dataset + metadata planes."""

from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urlparse


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Return (bucket, key) for s3://bucket/key."""
    u = urlparse(uri)
    if u.scheme not in ("s3", "s3a", "s3n"):
        raise ValueError(f"not an s3 URI: {uri}")
    return u.netloc, u.path.lstrip("/")


def is_s3(uri: str) -> bool:
    return uri.startswith("s3://") or uri.startswith("s3a://")


def join_uri(root: str, *parts: str) -> str:
    root = root.rstrip("/")
    rest = "/".join(p.strip("/") for p in parts if p)
    return f"{root}/{rest}" if rest else root


def open_fs(
    *,
    endpoint_url: Optional[str] = None,
    key: Optional[str] = None,
    secret: Optional[str] = None,
    token: Optional[str] = None,
    anon: bool = False,
) -> Any:
    """Build an s3fs filesystem (path-style for RustFS/MinIO)."""
    import s3fs

    kwargs: dict[str, Any] = {
        "anon": anon,
        "key": key,
        "secret": secret,
        "token": token,
    }
    if endpoint_url:
        kwargs["client_kwargs"] = {"endpoint_url": endpoint_url}
        kwargs["config_kwargs"] = {
            "s3": {"addressing_style": "path"},
            "signature_version": "s3v4",
        }
    return s3fs.S3FileSystem(**kwargs)


def local_or_s3_exists(path_or_uri: str, fs: Any = None) -> bool:
    if is_s3(path_or_uri):
        if fs is None:
            raise ValueError("s3 path requires fs")
        bucket, key = parse_s3_uri(path_or_uri)
        return fs.exists(f"{bucket}/{key}")
    from pathlib import Path

    return Path(path_or_uri).exists()
