"""Guards: clone-portable GitHub remotes and PyFlink without the Java submodule.

A fresh checkout must be able to `uv sync` and clone submodules without
rch-specific SSH remotes or a 5+ GB `thirdparty/flink` tree.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _gitmodules_entries() -> list[tuple[str, str, str]]:
    """Return (name, path, url) from .gitmodules."""
    text = (ROOT / ".gitmodules").read_text(encoding="utf-8")
    entries: list[tuple[str, str, str]] = []
    name = path = url = None
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("[submodule "):
            if name and path and url:
                entries.append((name, path, url))
            name = line.split('"', 1)[1].rsplit('"', 1)[0]
            path = url = None
        elif line.startswith("path ="):
            path = line.split("=", 1)[1].strip()
        elif line.startswith("url ="):
            url = line.split("=", 1)[1].strip()
    if name and path and url:
        entries.append((name, path, url))
    return entries


def test_gitmodules_urls_are_https_github():
    entries = _gitmodules_entries()
    assert entries, ".gitmodules must list submodules"
    bad = []
    for name, path, url in entries:
        if url.startswith("../"):
            continue
        if not url.startswith("https://github.com/"):
            bad.append(f"{name} ({path}): {url}")
        if url.startswith("git@"):
            bad.append(f"{name} ({path}) uses SSH: {url}")
    assert not bad, "submodule URLs must be https://github.com/… or relative ../:\n  " + "\n  ".join(bad)


def test_gitmodules_has_no_dead_solr_indexer():
    text = (ROOT / ".gitmodules").read_text(encoding="utf-8")
    assert "flink-solr-log-indexer" not in text


def test_iceberg_points_at_apache_upstream():
    urls = {path: url for _, path, url in _gitmodules_entries()}
    assert urls.get("thirdparty/iceberg") == "https://github.com/apache/iceberg.git"


def test_nifi_and_polaris_point_at_apache_upstream():
    urls = {path: url for _, path, url in _gitmodules_entries()}
    assert urls.get("thirdparty/nifi") == "https://github.com/apache/nifi.git"
    assert urls.get("thirdparty/polaris") == "https://github.com/apache/polaris.git"


def test_flink_submodule_is_https_github():
    urls = {path: url for _, path, url in _gitmodules_entries()}
    url = urls.get("thirdparty/flink", "")
    assert url.startswith("https://github.com/"), url
    assert url.endswith("/asf-flink.git") or url.endswith("/oss-flink.git"), url


def test_pyproject_apache_flink_is_vendored_tree():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'apache-flink = { path = "thirdparty/flink-python", editable = true }' in text
    assert "thirdparty/flink/flink-python" not in text


def test_vendored_pyflink_tree_is_present():
    setup = ROOT / "thirdparty" / "flink-python" / "setup.py"
    assert setup.is_file(), "thirdparty/flink-python must be committed (not the Java submodule)"
    src = setup.read_text(encoding="utf-8", errors="replace")
    assert "apache-beam>=2.71.0" in src
    assert "cloudpickle>=3.0.0" in src


def test_uv_lock_editable_path_is_vendored_tree():
    text = (ROOT / "uv.lock").read_text(encoding="utf-8")
    assert 'editable = "thirdparty/flink-python"' in text
    assert "thirdparty/flink/flink-python" not in text


def test_gitignore_does_not_drop_vendored_pyflink():
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "!thirdparty/flink-python/" in gitignore


def test_no_ssh_or_host_paths_in_source_pins():
    for rel in (".gitmodules", "pyproject.toml"):
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert "git@github.com" not in text, f"{rel} must not use SSH GitHub remotes"
        assert not re.search(r"/home/[^/\s]+/local/src/", text), f"{rel} has a host checkout path"
