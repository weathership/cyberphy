"""Deploy timeout/retries scaling + engine OOM memory helpers (engine 0.5.13)."""

from converge.catalog import (
    _zarf_deploy_retries,
    _zarf_deploy_timeout,
    _parse_mem_to_mi,
    _ENGINE_MEM_LIMIT,
    _ENGINE_MEM_REQUEST,
)


def test_retries_light_vs_heavy():
    assert _zarf_deploy_retries("navigator-engine") == 2
    assert _zarf_deploy_retries("panel-viz") == 2
    assert _zarf_deploy_retries("cybersec-images,navigator-engine") == 3
    assert _zarf_deploy_retries("jupyterhub,sample-notebooks") == 3
    assert _zarf_deploy_retries("dask-cluster") == 3


def test_timeout_scaled_down_from_7200():
    assert _zarf_deploy_timeout("cybersec-images,navigator-engine") == 1800
    assert _zarf_deploy_timeout("navigator-engine") == 900
    assert _zarf_deploy_timeout("jupyterhub") == 3600
    assert _zarf_deploy_timeout("dask-cluster") == 2400
    # Never the old blanket 7200 for these
    for c in (
        "cybersec-images,navigator-engine",
        "navigator-engine",
        "panel-viz",
        "jupyterhub",
    ):
        assert _zarf_deploy_timeout(c) < 7200


def test_parse_mem_to_mi():
    assert _parse_mem_to_mi("4Gi") == 4096
    assert _parse_mem_to_mi("8Gi") == 8192
    assert _parse_mem_to_mi("512Mi") == 512
    assert _parse_mem_to_mi("") is None
    assert _ENGINE_MEM_REQUEST == "4Gi"
    assert _ENGINE_MEM_LIMIT == "8Gi"
