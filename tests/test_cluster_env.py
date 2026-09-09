"""Notebook cluster_env — config from env (converge → zarf → JupyterHub)."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "zarf" / "notebooks" / "snippets"))

from cluster_env import load_cluster_config  # noqa: E402


def test_otel_path_prefix_and_spans(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "dhfo")
    monkeypatch.setenv("S3_ENDPOINT", "http://10.200.42.91")
    monkeypatch.setenv("OTEL_DATA_PATH", "s3://dhfo/otel-notebook/")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.delenv("OTEL_PREFIX", raising=False)
    cfg = load_cluster_config()
    assert cfg.s3_bucket == "dhfo"
    assert cfg.otel_prefix == "otel-notebook"
    assert cfg.spans_prefix_s3 == "s3://dhfo/otel-notebook/spans/"
    assert "otel-notebook/spans" in cfg.spans_glob
    opts = cfg.storage_options()
    assert opts["client_kwargs"]["endpoint_url"] == "http://10.200.42.91"
    assert opts["config_kwargs"]["s3"]["addressing_style"] == "path"


def test_use_dask_default_on(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "b")
    monkeypatch.delenv("USE_DASK", raising=False)
    assert load_cluster_config().use_dask is True
    monkeypatch.setenv("USE_DASK", "0")
    assert load_cluster_config().use_dask is False


def test_default_prefix_otel_notebook_not_validation(monkeypatch):
    """Empty path env must not default to validation-30gb (path drift → empty cols)."""
    monkeypatch.setenv("S3_BUCKET", "dhfo")
    for k in ("OTEL_DATA_PATH", "OTEL_PREFIX", "PREFIX", "OTEL_DATASET_PREFIX"):
        monkeypatch.delenv(k, raising=False)
    cfg = load_cluster_config()
    assert cfg.otel_prefix == "otel-notebook"
    assert "validation" not in cfg.spans_prefix_s3
    assert cfg.spans_prefix_s3.endswith("/otel-notebook/spans/")
