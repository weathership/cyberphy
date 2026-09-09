"""T5.sample-notebooks must require HDF5 keys, not mere ConfigMap presence."""

from converge.catalog import _SAMPLE_NOTEBOOK_KEYS


def test_expected_notebooks_include_hdf5():
    assert "HDF5_CPHY_Acquisition_Generator.ipynb" in _SAMPLE_NOTEBOOK_KEYS
    assert "HDF5_Iceberg_Metadata_Provider.ipynb" in _SAMPLE_NOTEBOOK_KEYS
    assert "OTEL_Data_Generator.ipynb" in _SAMPLE_NOTEBOOK_KEYS
    assert "Dask_S3_Validation.ipynb" in _SAMPLE_NOTEBOOK_KEYS
    assert len(_SAMPLE_NOTEBOOK_KEYS) >= 4
