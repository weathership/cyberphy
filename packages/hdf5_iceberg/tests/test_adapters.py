from pathlib import Path

import h5py
import numpy as np

from hdf5_iceberg.adapters import get_adapter


def test_flat_and_product_prefix_local(tmp_path: Path) -> None:
    prod = tmp_path / "datasets" / "hdf5" / "cphy"
    prod.mkdir(parents=True)
    p = prod / "cphy_20260101T000000_0000Z.h5"
    with h5py.File(p, "w") as f:
        f.create_dataset("x", data=np.zeros(4))

    flat = get_adapter("flat_prefix")
    cands = flat.list_candidates(str(tmp_path), min_size_bytes=1)
    assert len(cands) == 1
    assert "service=" not in cands[0].uri

    prod_ad = get_adapter("product_prefix")
    cands2 = prod_ad.list_candidates(str(tmp_path), min_size_bytes=1)
    assert len(cands2) == 1
    assert cands2[0].tags.get("product") == "cphy"
    assert cands2[0].tags.get("filename", "").endswith(".h5")
