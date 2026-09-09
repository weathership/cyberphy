"""hdf5_iceberg — standalone HDF5 ↔ Iceberg metadata plane SDK.

Import path is intentionally top-level (not cybersec/cyberphy)::

    from hdf5_iceberg import DatasetProvider, MetadataProvider, register_root
"""

from hdf5_iceberg.dataset import DatasetProvider
from hdf5_iceberg.descriptor import DatasetDescriptor
from hdf5_iceberg.metadata import MetadataProvider
from hdf5_iceberg.register import RegistrationResult, register_root

__all__ = [
    "DatasetDescriptor",
    "DatasetProvider",
    "MetadataProvider",
    "RegistrationResult",
    "register_root",
    "__version__",
]

__version__ = "0.1.0"
