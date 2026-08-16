"""Resolve Flink locations without baking host-specific absolute paths into jobs.

The Flink distribution is relocatable: `bin/config.sh` derives FLINK_HOME from
the script location. Callers must do the same — `$FLINK_HOME` if set, otherwise
a path relative to the repo / devenv root. Never persist `file:///home/…` or
`file:///Users/…` into job graphs (`pipeline.jars`, checkpoint dirs) or into
the dist's `conf/` (those become the artifact).

Connector JARs belong in `$FLINK_HOME/lib/` so submitted jobs do not need
`pipeline.jars=file://…`. Checkpoints go to `$FLINK_STATE_DIR` or an S3 URI.
"""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_FLINK_VERSION = "1.20.1"
DEFAULT_CHECKPOINT_URI = "s3://cybersec/checkpoints"
_FLINK_COMMON_JAR_NAME = "flink-common-2.4.0.jar"


def repo_root() -> Path:
    """Project root: DEVENV_ROOT, else walk up from this file to pyproject.toml."""
    env = os.environ.get("DEVENV_ROOT")
    if env:
        return Path(env)
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").is_file() and (parent / "flink-cyber").is_dir():
            return parent
    return Path.cwd()


def flink_dist_relpath(version: str | None = None) -> Path:
    ver = version or os.environ.get("FLINK_VERSION", DEFAULT_FLINK_VERSION)
    return Path("thirdparty") / "flink" / "flink-dist" / "target" / f"flink-{ver}-bin" / f"flink-{ver}"


def flink_home(version: str | None = None) -> Path:
    """Runtime Flink home. Honors FLINK_HOME; otherwise repo-relative dist."""
    env = os.environ.get("FLINK_HOME")
    if env:
        return Path(env)
    return repo_root() / flink_dist_relpath(version)


def flink_conf_dir(home: Path | None = None) -> Path:
    """Config overlay. FLINK_CONF_DIR if set, else `$DEVENV_STATE/flink/conf`.

    Host-specific keys (python.executable) belong in the overlay so they
    never land in the Maven `target/` dist.
    """
    env = os.environ.get("FLINK_CONF_DIR")
    if env:
        return Path(env)
    devenv_state = os.environ.get("DEVENV_STATE")
    if devenv_state:
        return Path(devenv_state) / "flink" / "conf"
    return (home or flink_home()) / "conf"


def flink_state_dir() -> Path:
    env = os.environ.get("FLINK_STATE_DIR")
    if env:
        return Path(env)
    devenv_state = os.environ.get("DEVENV_STATE")
    if devenv_state:
        return Path(devenv_state) / "flink"
    return repo_root() / ".devenv" / "state" / "flink"


def checkpoint_uri() -> str:
    """URI for Flink checkpoints. S3 (or explicit env) so job graphs stay portable."""
    explicit = os.environ.get("FLINK_CHECKPOINT_DIR")
    if explicit:
        return explicit
    state = os.environ.get("FLINK_STATE_DIR")
    if state:
        return f"file://{Path(state) / 'checkpoints'}"
    return DEFAULT_CHECKPOINT_URI


def flink_common_jar() -> Path:
    env = os.environ.get("FLINK_COMMON_JAR")
    if env:
        return Path(env)
    target = repo_root() / "flink-cyber" / "flink-common" / "target"
    preferred = target / _FLINK_COMMON_JAR_NAME
    if preferred.is_file():
        return preferred
    # Legacy shaded name from earlier Iceberg packaging experiments
    for candidate in sorted(target.glob("flink-common-*.jar")):
        name = candidate.name
        if name.endswith("-sources.jar") or name.endswith("-javadoc.jar"):
            continue
        return candidate
    return preferred
