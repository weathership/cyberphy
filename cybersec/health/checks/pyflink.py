"""PyFlink health checks.

Checks for:
- PYFLINK_001: PyFlink not installed
- PYFLINK_011: Iceberg AWS bundle missing
- PYFLINK_012: Iceberg Flink runtime missing
- PYFLINK_013: Git submodules not initialized
"""

import os
import subprocess
import time
from pathlib import Path
from typing import Any

from ..models import CheckResult, HealthContext
from ..catalog import get_failure_mode


async def check_pyflink_installed(ctx: HealthContext) -> CheckResult:
    """PYFLINK_001: Check PyFlink is installed and accessible.

    Checks if any Python in the environment has pyflink installed.
    PyFlink is an editable install of vendored thirdparty/flink-python via uv sync.
    """
    start = time.monotonic()

    devenv_root = os.environ.get("DEVENV_ROOT", os.getcwd())

    flink_python_dir = Path(devenv_root) / "thirdparty" / "flink-python"
    flink_python_ready = (flink_python_dir / "setup.py").exists()

    # Check candidate Python paths in order of preference
    candidates = [
        ("project_venv", Path(devenv_root) / ".venv" / "bin" / "python"),
        ("project_venv", Path(devenv_root) / ".venv" / "bin" / "python3"),
        ("uv_venv", Path(devenv_root) / ".devenv" / "state" / "venv" / "bin" / "python3"),
        ("devenv_profile", Path(devenv_root) / ".devenv" / "profile" / "bin" / "python3"),
    ]

    pyflink_found = None
    for name, python_path in candidates:
        if not python_path.exists():
            continue
        try:
            # Check import succeeds and get location (version attr may not exist)
            result = subprocess.run(
                [str(python_path), "-c", "import pyflink; print(pyflink.__file__)"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                pyflink_path = result.stdout.strip()
                # Try to get version, fall back to "installed"
                ver_result = subprocess.run(
                    [str(python_path), "-c",
                     "import pyflink; print(getattr(pyflink, '__version__', 'installed'))"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                version = ver_result.stdout.strip() if ver_result.returncode == 0 else "installed"
                pyflink_found = (name, str(python_path), version)
                break
        except (subprocess.TimeoutExpired, Exception):
            continue

    duration = int((time.monotonic() - start) * 1000)

    if pyflink_found:
        name, python_path, version = pyflink_found
        result_obj = CheckResult.ok(
            f"PyFlink {version} found in {name}",
            version=version,
            python=python_path,
            location=name,
            source="thirdparty/flink-python" if flink_python_ready else "unknown",
        )
        result_obj.duration_ms = duration
        return result_obj
    else:
        fm = get_failure_mode("PYFLINK_001")
        rpn = fm.calculate_rpn() if fm else None

        if not flink_python_ready:
            remediation = (
                "Vendored PyFlink tree is missing (thirdparty/flink-python/setup.py).\n"
                "Restore it from git, then run: uv sync"
            )
        else:
            remediation = "Run: /health fix PYFLINK_001 --apply (runs uv sync)"

        return CheckResult.critical(
            "PyFlink not installed in any Python environment",
            failure_mode_id="PYFLINK_001",
            rpn=rpn,
            remediation=remediation,
            checked_paths=[str(p) for _, p in candidates if p.exists()],
            flink_python_ready=flink_python_ready,
            flink_python_dir=str(flink_python_dir),
            duration_ms=duration,
        )


async def check_flink_python_config(ctx: HealthContext) -> CheckResult:
    """PYFLINK_002: Check Flink is configured to use a Python with pyflink.

    Verifies python.executable in config.yaml (Flink 1.20+) points to a Python that has pyflink.
    """
    import yaml

    start = time.monotonic()

    # Get Flink home / conf overlay (FLINK_CONF_DIR, not the Maven dist)
    from cybersec.flink_paths import flink_conf_dir, flink_home as resolve_flink_home

    flink_home = ctx.config.get_flink_home() if ctx.config else None
    if not flink_home:
        flink_home = resolve_flink_home()

    # Flink 1.20+ uses config.yaml
    flink_conf = flink_conf_dir(flink_home) / "config.yaml"

    if not flink_conf.exists():
        duration = int((time.monotonic() - start) * 1000)
        result = CheckResult.skipped(f"config.yaml not found: {flink_conf}")
        result.duration_ms = duration
        return result

    # Read configured Python path from YAML structure
    try:
        content = flink_conf.read_text()
        config = yaml.safe_load(content) or {}
        python_config = config.get("python", {})
        # Check both python.executable and python.client.executable
        configured_python = python_config.get("executable")
        if not configured_python:
            client_config = python_config.get("client", {})
            configured_python = client_config.get("executable")
    except yaml.YAMLError:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.error(f"Failed to parse config.yaml", duration_ms=duration)

    duration = int((time.monotonic() - start) * 1000)

    if not configured_python:
        fm = get_failure_mode("PYFLINK_002")
        rpn = fm.calculate_rpn() if fm else None
        return CheckResult.warning(
            "python.executable not configured in config.yaml",
            failure_mode_id="PYFLINK_002",
            rpn=rpn,
            remediation="Run: /health fix --apply",
            flink_conf=str(flink_conf),
            duration_ms=duration,
        )

    # Check if configured Python has pyflink
    if not Path(configured_python).exists():
        fm = get_failure_mode("PYFLINK_002")
        rpn = fm.calculate_rpn() if fm else None
        return CheckResult.critical(
            f"Configured Python does not exist: {configured_python}",
            failure_mode_id="PYFLINK_002",
            rpn=rpn,
            remediation="Run: /health fix --apply",
            configured_python=configured_python,
            duration_ms=duration,
        )

    try:
        result = subprocess.run(
            [configured_python, "-c", "import pyflink; print(pyflink.__version__)"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0:
            version = result.stdout.strip()
            result_obj = CheckResult.ok(
                f"Flink configured with PyFlink {version}",
                version=version,
                configured_python=configured_python,
            )
            result_obj.duration_ms = duration
            return result_obj
        else:
            fm = get_failure_mode("PYFLINK_002")
            rpn = fm.calculate_rpn() if fm else None
            return CheckResult.critical(
                f"Configured Python missing pyflink: {configured_python}",
                failure_mode_id="PYFLINK_002",
                rpn=rpn,
                remediation="Run: /health fix --apply",
                configured_python=configured_python,
                error=result.stderr.strip()[:200],
                duration_ms=duration,
            )

    except subprocess.TimeoutExpired:
        return CheckResult.error("Python check timed out", duration_ms=duration)
    except Exception as e:
        return CheckResult.error(f"Failed to check Python config: {e}", duration_ms=duration)


async def check_submodules(ctx: HealthContext) -> CheckResult:
    """PYFLINK_013: Check git submodules are initialized.

    Verifies thirdparty/iceberg and thirdparty/flink are properly initialized.
    """
    start = time.monotonic()

    devenv_root = os.environ.get("DEVENV_ROOT", os.getcwd())

    # Check Iceberg submodule
    iceberg_gradlew = Path(devenv_root) / "thirdparty" / "iceberg" / "gradlew"
    iceberg_initialized = iceberg_gradlew.exists()

    # Check Flink submodule
    flink_pom = Path(devenv_root) / "thirdparty" / "flink" / "pom.xml"
    flink_initialized = flink_pom.exists()

    duration = int((time.monotonic() - start) * 1000)

    if not iceberg_initialized or not flink_initialized:
        fm = get_failure_mode("PYFLINK_013")
        rpn = fm.calculate_rpn() if fm else None

        missing = []
        if not iceberg_initialized:
            missing.append("thirdparty/iceberg")
        if not flink_initialized:
            missing.append("thirdparty/flink")

        return CheckResult.critical(
            f"Git submodules not initialized: {', '.join(missing)}",
            failure_mode_id="PYFLINK_013",
            rpn=rpn,
            remediation=(
                "Needed only for a local Flink/Iceberg source build (disk-heavy): "
                "git submodule update --init thirdparty/flink thirdparty/iceberg. "
                "PyFlink does not require this — uv sync uses thirdparty/flink-python."
            ),
            missing_submodules=missing,
            duration_ms=duration,
        )

    result = CheckResult.ok(
        "Git submodules initialized",
        iceberg=str(iceberg_gradlew.parent),
        flink=str(flink_pom.parent),
    )
    result.duration_ms = duration
    return result


async def check_iceberg_jars(ctx: HealthContext) -> CheckResult:
    """PYFLINK_011/012: Check Iceberg JARs are installed in Flink lib.

    Verifies iceberg-flink-runtime and iceberg-aws-bundle JARs exist.
    """
    start = time.monotonic()

    # Get Flink home (relocatable — FLINK_HOME or repo-relative dist)
    from cybersec.flink_paths import flink_home as resolve_flink_home

    flink_home = ctx.config.get_flink_home() if ctx.config else None
    if not flink_home:
        flink_home = resolve_flink_home()

    if not flink_home.exists():
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.skipped(
            "Flink not installed - skipping JAR check",
            flink_home=str(flink_home),
            duration_ms=duration,
        )

    lib_dir = flink_home / "lib"
    if not lib_dir.exists():
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.error(
            f"Flink lib directory not found: {lib_dir}",
            duration_ms=duration,
        )

    # Check for JARs
    flink_runtime_jars = list(lib_dir.glob("iceberg-flink-runtime-1.20-*.jar"))
    aws_bundle_jars = list(lib_dir.glob("iceberg-aws-bundle-*.jar"))

    duration = int((time.monotonic() - start) * 1000)

    missing = []
    if not flink_runtime_jars:
        missing.append("iceberg-flink-runtime-1.20-*.jar")
    if not aws_bundle_jars:
        missing.append("iceberg-aws-bundle-*.jar")

    if missing:
        # Use PYFLINK_011 for AWS bundle (more common/critical)
        fm = get_failure_mode("PYFLINK_011")
        rpn = fm.calculate_rpn() if fm else None

        return CheckResult.critical(
            f"Missing Iceberg JARs: {', '.join(missing)}",
            failure_mode_id="PYFLINK_011",
            rpn=rpn,
            remediation="Run: /health fix --apply",
            missing_jars=missing,
            lib_dir=str(lib_dir),
            duration_ms=duration,
        )

    result = CheckResult.ok(
        f"Iceberg JARs OK: {len(flink_runtime_jars)} runtime, {len(aws_bundle_jars)} AWS bundle",
        flink_runtime=[j.name for j in flink_runtime_jars],
        aws_bundle=[j.name for j in aws_bundle_jars],
    )
    result.duration_ms = duration
    return result


# Registry of all pyflink checks
CHECKS: dict[str, Any] = {
    "PYFLINK_001": check_pyflink_installed,
    "PYFLINK_002": check_flink_python_config,
    "PYFLINK_011": check_iceberg_jars,
    "PYFLINK_012": check_iceberg_jars,  # Same check covers both
    "PYFLINK_013": check_submodules,
}
