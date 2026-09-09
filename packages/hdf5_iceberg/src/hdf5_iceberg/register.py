"""register_root — scan dataset plane, audit, idempotent write to metadata plane."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from hdf5_iceberg.dataset import DatasetProvider
from hdf5_iceberg.descriptor import DatasetDescriptor
from hdf5_iceberg.metadata import MetadataProvider


@dataclass
class RegistrationResult:
    n_candidates: int = 0
    n_audited: int = 0
    n_registered: int = 0  # newly written fingerprints
    n_reused: int = 0  # already present
    pointer_table_uri: Optional[str] = None
    semantic_uris: dict[str, str] = field(default_factory=dict)
    descriptors: list[DatasetDescriptor] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"candidates={self.n_candidates} audited={self.n_audited} "
            f"new={self.n_registered} reused={self.n_reused} "
            f"pointer={self.pointer_table_uri}"
        )


def register_root(
    dataset: DatasetProvider,
    metadata: MetadataProvider,
    *,
    adapter: Optional[str] = None,
    max_files: Optional[int] = None,
    skip_audit: bool = False,
    emit_semantic: bool = True,
    min_size_bytes: Optional[int] = None,
) -> RegistrationResult:
    """Discover HDF5 under dataset roots and register into metadata warehouse.

    Idempotent on ``fingerprint`` (and secondarily uri). Never writes into
    dataset roots.
    """
    if adapter is not None:
        from hdf5_iceberg.adapters import get_adapter

        dataset._adapter_obj = get_adapter(adapter)
    if min_size_bytes is not None:
        dataset.min_size_bytes = min_size_bytes

    result = RegistrationResult()
    existing_fp = metadata.existing_fingerprints()
    existing_uri = metadata.existing_uris()

    try:
        descs = dataset.discover(max_files=max_files, audit=not skip_audit)
    except Exception as e:
        result.errors.append(f"discover failed: {type(e).__name__}: {e}")
        return result

    result.n_candidates = len(descs)
    result.n_audited = len(descs) if not skip_audit else 0

    new_or_all: list[DatasetDescriptor] = []
    for d in descs:
        fp = d.fingerprint
        if fp and fp in existing_fp:
            result.n_reused += 1
            new_or_all.append(d)
            continue
        if d.uri in existing_uri and not fp:
            result.n_reused += 1
            new_or_all.append(d)
            continue
        result.n_registered += 1
        new_or_all.append(d)

    result.descriptors = new_or_all
    try:
        result.pointer_table_uri = metadata.write_pointer_table(new_or_all)
    except Exception as e:
        result.errors.append(f"pointer table write failed: {type(e).__name__}: {e}")
        return result

    if emit_semantic and new_or_all:
        try:
            result.semantic_uris = metadata.write_semantic_stub(new_or_all)
        except Exception as e:
            result.errors.append(f"semantic stub write failed: {type(e).__name__}: {e}")

    return result
