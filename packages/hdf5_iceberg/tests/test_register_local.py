"""Local-filesystem registration: metadata writes isolated; dataset never written."""

from __future__ import annotations

import json
from pathlib import Path

import h5py
import numpy as np
import pytest

from hdf5_iceberg import DatasetProvider, MetadataProvider, register_root
from hdf5_iceberg.metadata import MetadataProvider as MP
from hdf5_iceberg.semantic.dcat_ttl import emit_dcat_ttl, emit_shacl_shapes


def _write_mini_h5(path: Path, n_series: int = 8, n_time: int = 16) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as f:
        f.attrs["collection.uuid"] = b"test-uuid-001"
        rm = f.create_group("ResourceMetrics")
        met = rm.create_group("Metric_0")
        met.create_dataset("Values", data=np.zeros((n_series, n_time), dtype=np.int16))
        met.create_dataset(
            "Timestamps",
            data=np.arange(n_time, dtype=np.int64) * 1000,
        )


def test_register_local_isolation(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    meta_root = tmp_path / "cyberphy-md" / "iceberg" / "warehouse"
    h5 = data_root / "datasets" / "hdf5" / "cphy" / "part0.h5"
    _write_mini_h5(h5)

    # mark data tree mtime baseline
    data_files_before = {p: p.stat().st_mtime for p in data_root.rglob("*") if p.is_file()}

    data = DatasetProvider(roots=[str(data_root)], adapter="flat_prefix", min_size_bytes=1)
    meta = MetadataProvider(warehouse=str(meta_root))
    r1 = register_root(data, meta, max_files=10)
    assert not r1.errors, r1.errors
    assert r1.n_registered >= 1
    assert r1.pointer_table_uri
    ptr = Path(meta_root) / "telemetry/hdf5_datasets/parts.parquet"
    assert ptr.exists()

    # isolation: all warehouse files under meta_root
    for p in meta_root.rglob("*"):
        if p.is_file():
            assert str(p).startswith(str(meta_root))

    # data plane untouched
    for p, mtime in data_files_before.items():
        assert p.exists()
        assert p.stat().st_mtime == mtime

    # semantic stubs
    assert "catalog.ttl" in r1.semantic_uris
    cat = Path(meta_root) / "semantic" / "catalog.ttl"
    assert cat.exists()
    text = cat.read_text()
    assert "dcat:Dataset" in text
    assert "dcat:Distribution" in text
    assert "json-ld" not in text.lower()
    assert "@context" not in text

    shapes = Path(meta_root) / "semantic" / "shapes.ttl"
    assert shapes.exists()
    assert "sh:NodeShape" in shapes.read_text()

    # idempotent second pass
    r2 = register_root(data, meta, max_files=10)
    assert r2.n_reused >= 1
    assert r2.n_registered == 0


def test_metadata_write_isolation_guard(tmp_path: Path) -> None:
    meta = MetadataProvider(warehouse=str(tmp_path / "md"))
    with pytest.raises(PermissionError):
        meta.assert_write_uri("s3://other-bucket/evil.parquet")


def test_dcat_ttl_no_jsonld() -> None:
    from hdf5_iceberg.descriptor import DatasetDescriptor

    d = DatasetDescriptor(
        uri="s3://data/a.h5",
        size_bytes=100,
        fingerprint="abcd",
        dataset_uuid="u1",
        layout="contiguous",
        n_series=10,
        n_time=20,
    )
    ttl = emit_dcat_ttl([d])
    assert "dcat:Catalog" in ttl
    assert "@context" not in ttl
    sh = emit_shacl_shapes()
    assert "sh:targetClass" in sh
