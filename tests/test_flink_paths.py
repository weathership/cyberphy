"""Unit tests for relocatable Flink path resolution."""
from __future__ import annotations

from pathlib import Path

from cybersec.flink_paths import (
    DEFAULT_CHECKPOINT_URI,
    checkpoint_uri,
    flink_common_jar,
    flink_conf_dir,
    flink_dist_relpath,
    flink_home,
    flink_state_dir,
    repo_root,
)


def test_repo_root_finds_this_checkout():
    root = repo_root()
    assert (root / "pyproject.toml").is_file()
    assert (root / "flink-cyber").is_dir()


def test_dist_relpath_is_relative():
    rel = flink_dist_relpath("1.20.1")
    assert not rel.is_absolute()
    assert rel.parts[0] == "thirdparty"


def test_flink_home_honors_env(monkeypatch, tmp_path):
    monkeypatch.setenv("FLINK_HOME", str(tmp_path / "flink"))
    assert flink_home() == tmp_path / "flink"


def test_flink_home_falls_back_to_repo_relative(monkeypatch):
    monkeypatch.delenv("FLINK_HOME", raising=False)
    home = flink_home()
    assert home == repo_root() / flink_dist_relpath()


def test_conf_dir_prefers_overlay_env(monkeypatch, tmp_path):
    monkeypatch.setenv("FLINK_CONF_DIR", str(tmp_path / "overlay"))
    assert flink_conf_dir() == tmp_path / "overlay"


def test_conf_dir_prefers_devenv_state_over_dist(monkeypatch, tmp_path):
    monkeypatch.delenv("FLINK_CONF_DIR", raising=False)
    monkeypatch.setenv("DEVENV_STATE", str(tmp_path / "state"))
    monkeypatch.setenv("FLINK_HOME", str(tmp_path / "dist"))
    assert flink_conf_dir() == tmp_path / "state" / "flink" / "conf"


def test_checkpoint_uri_prefers_explicit_then_state(monkeypatch, tmp_path):
    monkeypatch.setenv("FLINK_CHECKPOINT_DIR", "s3://other/ckpts")
    assert checkpoint_uri() == "s3://other/ckpts"

    monkeypatch.delenv("FLINK_CHECKPOINT_DIR")
    monkeypatch.setenv("FLINK_STATE_DIR", str(tmp_path / "state"))
    assert checkpoint_uri() == f"file://{tmp_path / 'state' / 'checkpoints'}"

    monkeypatch.delenv("FLINK_STATE_DIR")
    assert checkpoint_uri() == DEFAULT_CHECKPOINT_URI


def test_flink_common_jar_honors_env(monkeypatch, tmp_path):
    jar = tmp_path / "custom.jar"
    jar.write_bytes(b"")
    monkeypatch.setenv("FLINK_COMMON_JAR", str(jar))
    assert flink_common_jar() == jar


def test_state_dir_honors_env(monkeypatch, tmp_path):
    monkeypatch.setenv("FLINK_STATE_DIR", str(tmp_path / "st"))
    assert flink_state_dir() == tmp_path / "st"


def test_datagen_does_not_set_pipeline_jars_file_uri():
    src = Path(__file__).resolve().parents[1] / "flink_jobs" / "cloudtrail_datagen.py"
    text = src.read_text()
    assert 'set("pipeline.jars"' not in text
    assert "file://{flink_home}" not in text
    assert 'os.path.join(os.getcwd(), "thirdparty/flink' not in text
