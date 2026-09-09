"""Built-in layout adapters for dataset discovery (no Hive branding)."""

from __future__ import annotations

from typing import Any

from hdf5_iceberg.adapters.base import LayoutAdapter
from hdf5_iceberg.adapters.flat_prefix import FlatPrefixAdapter
from hdf5_iceberg.adapters.product_prefix import ProductPrefixAdapter

ADAPTERS: dict[str, type] = {
    "flat": FlatPrefixAdapter,
    "flat_prefix": FlatPrefixAdapter,
    "product_prefix": ProductPrefixAdapter,
    "cphy": ProductPrefixAdapter,  # lab product profile
    # Temporary aliases — remove after callers migrate
    "cyberphy_hive": ProductPrefixAdapter,
    "hive": ProductPrefixAdapter,
}


def get_adapter(name: str, **kwargs: Any) -> LayoutAdapter:
    key = (name or "product_prefix").strip().lower()
    if key not in ADAPTERS:
        raise KeyError(f"unknown layout adapter {name!r}; known={sorted(ADAPTERS)}")
    return ADAPTERS[key](**kwargs)  # type: ignore[call-arg]


__all__ = [
    "ADAPTERS",
    "FlatPrefixAdapter",
    "LayoutAdapter",
    "ProductPrefixAdapter",
    "get_adapter",
]
