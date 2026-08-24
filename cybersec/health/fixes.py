"""Health fix implementations.

Provides automated fixes for detected health issues across all categories:
- Flink/PyFlink environment issues
- Infrastructure issues (shared memory, etc.)

Uses FMEA tier system to determine which fixes can be auto-applied.

DESIGN PRINCIPLES
=================

1. Root Cause Detection, Not Service Restarts
---------------------------------------------
The health framework should detect ROOT CAUSES that prevent services from
starting, NOT just restart services. Service lifecycle is devenv/process-compose's
job.

WRONG: "Flink not running" -> Start Flink
RIGHT: "Shared memory limits too low" -> Fix limits (PostgreSQL will then start)
RIGHT: "PyFlink shadowed by conflicting package" -> Remove shadow, run uv sync
RIGHT: "Iceberg JARs missing" -> Build from submodule

Example of correct detection (from process-compose logs):
  FATAL: could not create shared memory segment: No space left on device
  HINT: all available shared memory IDs have been taken...

This leads to SYSTEM_001 which proactively detects low kern.sysv.shmmax/shmall
BEFORE services try to start, rather than waiting for PostgreSQL to fail.

Avoid superficial "service not running" checks that duplicate what
`devenv tasks run restart:clean` already handles.

2. Submodule-Aware Development Over Downloads
---------------------------------------------
All health fixes, AIOps heuristics, and self-healing automation MUST prefer
building from thirdparty/ submodules over downloading binaries:

- Flink: Build from thirdparty/flink (mvn install)
- PyFlink: Install from thirdparty/flink-python (uv sync editable)
- Iceberg: Build JARs from thirdparty/iceberg (gradlew shadowJar)
- NiFi: Build from thirdparty/nifi (mvn install)
- Polaris: Build from thirdparty/polaris (gradlew assemble)

This ensures:
1. Reproducible builds across developer machines
2. Consistent versions tied to git commits
3. Ability to apply patches and customizations
4. No external download dependencies during development

When implementing new fixes or health checks, always check for submodule
availability before falling back to download-based installation.
"""

import os
import platform
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any


async def _find_python_with_pyflink() -> str | None:
    """Find a Python interpreter that has pyflink installed.

    Checks multiple candidate paths in order of preference:
    1. UV virtualenv Python (.devenv/state/venv/bin/python3)
    2. Devenv profile Python (.devenv/profile/bin/python3)
    3. Current Python (sys.executable)

    Returns the first one that can import pyflink, or None if none found.
    """
    import sys

    devenv_root = os.environ.get("DEVENV_ROOT", os.getcwd())

    candidates = [
        # UV virtualenv - where uv sync installs packages
        Path(devenv_root) / ".devenv" / "state" / "venv" / "bin" / "python3",
        # Devenv profile Python
        Path(devenv_root) / ".devenv" / "profile" / "bin" / "python3",
        # Current Python
        Path(sys.executable),
    ]

    for python_path in candidates:
        if not python_path.exists():
            continue

        try:
            result = subprocess.run(
                [str(python_path), "-c", "import pyflink"],
                capture_output=True,
                timeout=5,
            )
            if result.returncode == 0:
                return str(python_path)
        except (subprocess.TimeoutExpired, Exception):
            continue

    return None


async def apply_fixes(diagnostics: dict, dry_run: bool = True) -> list[dict[str, Any]]:
    """Apply fixes for detected health issues.

    Args:
        diagnostics: Dict containing 'issues' list from health checks
        dry_run: If True, show what would be done without making changes

    Returns:
        List of fix results with status and details
    """
    from ..bootstrap import BootstrapService

    results: list[dict[str, Any]] = []
    issues = diagnostics.get("issues", [])

    if not issues:
        return results

    service = BootstrapService()
    config = service.get_config()
    flink_home = config.get_flink_home()

    for issue in issues:
        failure_mode_id = issue.get("failure_mode_id", "")

        if failure_mode_id == "FLINK_001":
            # Flink JobManager not running - diagnose and attempt to start
            result = await _fix_flink_not_running(flink_home, dry_run)
            result["failure_mode_id"] = "FLINK_001"
            results.append(result)

        elif failure_mode_id == "PYFLINK_001":
            # PyFlink not installed - run uv sync with submodule awareness
            result = await _fix_pyflink_not_installed(dry_run)
            results.append(result)

        elif failure_mode_id == "PYFLINK_002":
            # Python path mismatch - fix config.yaml
            result = await _fix_python_path_mismatch(diagnostics, flink_home, dry_run)
            results.append(result)

        elif failure_mode_id == "PYFLINK_004":
            # FLINK_HOME not set - manual fix required
            results.append({
                "failure_mode_id": "PYFLINK_004",
                "action": "manual_required",
                "success": False,
                "message": "FLINK_HOME not set. Run: devenv tasks run restart:clean",
                "details": "This will build Flink from source if needed.",
            })

        elif failure_mode_id == "PYFLINK_005":
            # macOS Python configuration - same fix as PYFLINK_002
            # Already handled by PYFLINK_002, skip duplicate
            if not any(r.get("failure_mode_id") == "PYFLINK_002" for r in results):
                result = await _fix_python_path_mismatch(diagnostics, flink_home, dry_run)
                result["failure_mode_id"] = "PYFLINK_005"
                results.append(result)

        elif failure_mode_id == "PYFLINK_006":
            # Log errors - informational only
            results.append({
                "failure_mode_id": "PYFLINK_006",
                "action": "review_required",
                "success": True,
                "message": "Log errors detected - review recommended",
                "details": "Check /tmp/cloudtrail_submit.log for specific errors",
            })

        elif failure_mode_id == "PYFLINK_007":
            # Config written but not applied - need cluster restart
            result = await _fix_flink_cluster_restart(flink_home, dry_run)
            result["failure_mode_id"] = "PYFLINK_007"
            results.append(result)

        elif failure_mode_id == "PYFLINK_008":
            # Flink cluster stale - need cluster restart
            result = await _fix_flink_cluster_restart(flink_home, dry_run)
            result["failure_mode_id"] = "PYFLINK_008"
            results.append(result)

        elif failure_mode_id == "PYFLINK_009":
            # Python executable not found - re-run config fix
            result = await _fix_python_path_mismatch(diagnostics, flink_home, dry_run)
            result["failure_mode_id"] = "PYFLINK_009"
            result["message"] = "Re-detected Python path and updated config.yaml"
            results.append(result)

        elif failure_mode_id == "PYFLINK_011":
            # Iceberg AWS bundle missing - build and install
            result = await _fix_iceberg_jars_missing(flink_home, dry_run)
            result["failure_mode_id"] = "PYFLINK_011"
            results.append(result)

        elif failure_mode_id == "PYFLINK_012":
            # Iceberg Flink runtime missing - build and install
            # Same fix as PYFLINK_011 - builds both JARs
            if not any(r.get("failure_mode_id") == "PYFLINK_011" for r in results):
                result = await _fix_iceberg_jars_missing(flink_home, dry_run)
                result["failure_mode_id"] = "PYFLINK_012"
                results.append(result)

        elif failure_mode_id == "FLINK_004":
            # DataGen job finishes immediately - fix bounded source
            result = await _fix_datagen_bounded_source(dry_run)
            results.append(result)

        elif failure_mode_id == "FLINK_005":
            # Job stuck initializing - provide diagnostic guidance
            results.append({
                "failure_mode_id": "FLINK_005",
                "action": "review_required",
                "success": True,
                "message": "Job stuck in CREATED/INITIALIZING state - manual investigation required",
                "details": "Check Flink logs for errors, verify resources available, run /health pyflink",
            })

        elif failure_mode_id == "PYFLINK_014":
            # Iceberg JAR version mismatch - clean rebuild
            result = await _fix_iceberg_version_mismatch(flink_home, dry_run)
            results.append(result)

        elif failure_mode_id == "INFRA_004":
            # Shared memory exhaustion - clean up orphaned IPC segments
            result = await _fix_shared_memory_exhaustion(dry_run)
            results.append(result)

        elif failure_mode_id == "SYSTEM_001":
            # macOS shared memory limits too low
            result = await _fix_shared_memory_limits(dry_run)
            results.append(result)

        elif failure_mode_id == "NIFI_001":
            # NiFi not installed - download and install
            result = await _fix_nifi_not_installed(dry_run)
            results.append(result)

        elif failure_mode_id == "K8S_001":
            # Kubeconfig stale - refresh from system copy
            result = await _fix_kubeconfig_stale(config, dry_run)
            results.append(result)

    return results


async def _fix_python_path_mismatch(
    diagnostics: dict,
    flink_home: Path | None,
    dry_run: bool
) -> dict[str, Any]:
    """Fix: Update config.yaml (Flink 1.20+) with correct Python path."""
    import yaml

    result = {
        "failure_mode_id": "PYFLINK_002",
        "action": "update_flink_config",
    }

    if not flink_home or not flink_home.exists():
        result["success"] = False
        result["message"] = "FLINK_HOME not found - cannot update config"
        return result

    # Write host-specific python.executable into the runtime overlay
    # (FLINK_CONF_DIR), never into the Maven dist under thirdparty/flink.
    from cybersec.flink_paths import flink_conf_dir

    overlay_dir = flink_conf_dir(flink_home)
    flink_conf_path = overlay_dir / "config.yaml"
    stock_conf = flink_home / "conf" / "config.yaml"
    if not flink_conf_path.exists():
        if stock_conf.exists() and overlay_dir != stock_conf.parent:
            if dry_run:
                flink_conf_path = stock_conf
            else:
                overlay_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(stock_conf, flink_conf_path)
        else:
            result["success"] = False
            result["message"] = f"config.yaml not found at {flink_conf_path}"
            return result

    # Determine the correct Python path - must have pyflink installed
    python_path = await _find_python_with_pyflink()

    if not python_path:
        result["success"] = False
        result["message"] = "Could not find a Python with pyflink installed. Run: uv sync"
        return result

    result["python_path"] = python_path
    result["config_file"] = str(flink_conf_path)

    # YAML structure to add
    python_config = {
        "executable": python_path,
        "client": {
            "executable": python_path,
        },
    }
    result["python_config"] = python_config

    if dry_run:
        result["success"] = True
        result["dry_run"] = True
        result["message"] = f"Would update config.yaml with python.executable: {python_path}"
        return result

    # Read and update YAML config
    try:
        content = flink_conf_path.read_text()

        # Backup original
        backup_path = flink_conf_path.with_suffix(".yaml.bak")
        shutil.copy(flink_conf_path, backup_path)
        result["backup"] = str(backup_path)

        # Parse existing config
        config = yaml.safe_load(content) or {}

        # Update python section
        config["python"] = python_config

        # Preserve the file structure: append python config as YAML block at end
        # This maintains comments in the original file
        if "python:" not in content:
            # Append new section
            python_yaml = yaml.dump({"python": python_config}, default_flow_style=False)
            new_content = content.rstrip() + "\n\n# PyFlink Python configuration (added by cybersec health fix)\n" + python_yaml
        else:
            # Full rewrite (loses comments but updates correctly)
            new_content = yaml.dump(config, default_flow_style=False, sort_keys=False)

        flink_conf_path.write_text(new_content)

        result["success"] = True
        result["message"] = f"Updated {flink_conf_path} with Python configuration"
        result["restart_required"] = True
        result["restart_command"] = "devenv tasks run restart:clean"

    except Exception as e:
        result["success"] = False
        result["message"] = f"Error updating config: {e}"

    return result


async def _fix_iceberg_jars_missing(flink_home: Path | None, dry_run: bool) -> dict[str, Any]:
    """Fix: Build and install Iceberg JARs (Flink runtime + AWS bundle).

    This builds the iceberg-flink-runtime-1.20 and iceberg-aws-bundle JARs
    from the thirdparty/iceberg submodule and copies them to Flink's lib directory.
    """
    import subprocess

    result = {
        "action": "build_iceberg_jars",
        "command": "./gradlew :iceberg-flink:iceberg-flink-runtime-1.20:shadowJar :iceberg-aws-bundle:shadowJar",
    }

    if not flink_home or not flink_home.exists():
        result["success"] = False
        result["message"] = "FLINK_HOME not found - cannot install Iceberg JARs"
        return result

    # Determine iceberg directory
    devenv_root = os.environ.get("DEVENV_ROOT", os.getcwd())
    iceberg_dir = Path(devenv_root) / "thirdparty" / "iceberg"

    if not iceberg_dir.exists():
        result["success"] = False
        result["message"] = f"Iceberg source not found at {iceberg_dir}. Run: git submodule update --init --recursive"
        return result

    # Check if submodule is initialized (has gradlew)
    gradlew = iceberg_dir / "gradlew"
    if not gradlew.exists():
        result["success"] = False
        result["message"] = "Iceberg submodule not initialized. Run: git submodule update --init --recursive"
        result["command"] = "git submodule update --init --recursive"
        return result

    lib_dir = flink_home / "lib"

    if dry_run:
        result["success"] = True
        result["dry_run"] = True
        result["message"] = "Would build and install Iceberg JARs"
        result["steps"] = [
            f"cd {iceberg_dir}",
            "./gradlew -PflinkVersions=1.20 :iceberg-flink:iceberg-flink-runtime-1.20:shadowJar :iceberg-aws-bundle:shadowJar -x test",
            f"cp flink/v1.20/flink-runtime/build/libs/iceberg-flink-runtime-1.20-*.jar {lib_dir}/",
            f"cp aws-bundle/build/libs/iceberg-aws-bundle-*.jar {lib_dir}/",
        ]
        return result

    # Build the JARs
    try:
        result["build_output"] = []

        build_cmd = [
            "./gradlew",
            "-PflinkVersions=1.20",
            ":iceberg-flink:iceberg-flink-runtime-1.20:shadowJar",
            ":iceberg-aws-bundle:shadowJar",
            "-x", "test",
            "-x", "integrationTest",
            "-x", "generateGitProperties",
        ]

        result["build_command"] = " ".join(build_cmd)

        proc = subprocess.run(
            build_cmd,
            capture_output=True,
            text=True,
            timeout=900,  # 15 min timeout for build
            cwd=str(iceberg_dir),
        )

        if proc.returncode != 0:
            result["success"] = False
            result["message"] = "Gradle build failed"
            result["error"] = proc.stderr[-2000:] if len(proc.stderr) > 2000 else proc.stderr
            return result

        result["build_output"].append("Gradle build succeeded")

        # Copy the JARs
        copied_jars = []

        # Copy Flink runtime JAR
        flink_runtime_dir = iceberg_dir / "flink" / "v1.20" / "flink-runtime" / "build" / "libs"
        for jar in flink_runtime_dir.glob("iceberg-flink-runtime-1.20-*.jar"):
            if not jar.name.endswith("-sources.jar") and not jar.name.endswith("-javadoc.jar"):
                dest = lib_dir / jar.name
                shutil.copy(jar, dest)
                copied_jars.append(str(dest))

        # Copy AWS bundle JAR
        aws_bundle_dir = iceberg_dir / "aws-bundle" / "build" / "libs"
        for jar in aws_bundle_dir.glob("iceberg-aws-bundle-*.jar"):
            if not jar.name.endswith("-sources.jar") and not jar.name.endswith("-javadoc.jar"):
                dest = lib_dir / jar.name
                shutil.copy(jar, dest)
                copied_jars.append(str(dest))

        if copied_jars:
            result["success"] = True
            result["message"] = f"Built and installed {len(copied_jars)} Iceberg JAR(s)"
            result["installed_jars"] = copied_jars
            result["restart_required"] = True
            result["restart_command"] = "devenv tasks run restart:clean"
        else:
            result["success"] = False
            result["message"] = "Build succeeded but no JARs found to copy"
            result["searched"] = [str(flink_runtime_dir), str(aws_bundle_dir)]

    except subprocess.TimeoutExpired:
        result["success"] = False
        result["message"] = "Gradle build timed out after 15 minutes"
    except Exception as e:
        result["success"] = False
        result["message"] = f"Error building/installing JARs: {e}"

    return result


async def _fix_iceberg_version_mismatch(flink_home: Path | None, dry_run: bool) -> dict[str, Any]:
    """Fix: Clean rebuild of Iceberg JARs to resolve version mismatch.

    This removes all existing Iceberg JARs and rebuilds from source to ensure
    consistent serialVersionUID across all Iceberg classes.
    """
    import subprocess

    result = {
        "failure_mode_id": "PYFLINK_014",
        "action": "rebuild_iceberg_jars",
    }

    if not flink_home or not flink_home.exists():
        result["success"] = False
        result["message"] = "FLINK_HOME not found - cannot fix Iceberg JARs"
        return result

    lib_dir = flink_home / "lib"
    devenv_root = os.environ.get("DEVENV_ROOT", os.getcwd())
    iceberg_dir = Path(devenv_root) / "thirdparty" / "iceberg"

    if not iceberg_dir.exists():
        result["success"] = False
        result["message"] = f"Iceberg source not found at {iceberg_dir}. Run: git submodule update --init --recursive"
        return result

    gradlew = iceberg_dir / "gradlew"
    if not gradlew.exists():
        result["success"] = False
        result["message"] = "Iceberg submodule not initialized. Run: git submodule update --init --recursive"
        return result

    # Find existing Iceberg JARs
    existing_jars = list(lib_dir.glob("iceberg-*.jar"))
    result["existing_jars"] = [str(j) for j in existing_jars]

    if dry_run:
        result["success"] = True
        result["dry_run"] = True
        result["message"] = "Would clean rebuild Iceberg JARs to fix version mismatch"
        result["steps"] = [
            f"Remove {len(existing_jars)} existing Iceberg JAR(s) from {lib_dir}",
            f"cd {iceberg_dir}",
            "./gradlew clean",
            "./gradlew -PflinkVersions=1.20 :iceberg-flink:iceberg-flink-runtime-1.20:shadowJar :iceberg-aws-bundle:shadowJar -x test",
            f"Copy new JARs to {lib_dir}",
            "Restart Flink cluster",
        ]
        return result

    try:
        # Step 1: Remove existing Iceberg JARs
        removed_jars = []
        for jar in existing_jars:
            jar.unlink()
            removed_jars.append(str(jar))
        result["removed_jars"] = removed_jars

        # Step 2: Clean build
        clean_cmd = ["./gradlew", "clean"]
        clean_proc = subprocess.run(
            clean_cmd,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(iceberg_dir),
        )

        if clean_proc.returncode != 0:
            result["success"] = False
            result["message"] = "Gradle clean failed"
            result["error"] = clean_proc.stderr[-1000:] if len(clean_proc.stderr) > 1000 else clean_proc.stderr
            return result

        # Step 3: Build fresh JARs
        build_cmd = [
            "./gradlew",
            "-PflinkVersions=1.20",
            ":iceberg-flink:iceberg-flink-runtime-1.20:shadowJar",
            ":iceberg-aws-bundle:shadowJar",
            "-x", "test",
            "-x", "integrationTest",
            "-x", "generateGitProperties",
        ]

        build_proc = subprocess.run(
            build_cmd,
            capture_output=True,
            text=True,
            timeout=900,  # 15 min timeout
            cwd=str(iceberg_dir),
        )

        if build_proc.returncode != 0:
            result["success"] = False
            result["message"] = "Gradle build failed"
            result["error"] = build_proc.stderr[-2000:] if len(build_proc.stderr) > 2000 else build_proc.stderr
            return result

        # Step 4: Copy new JARs
        copied_jars = []

        flink_runtime_dir = iceberg_dir / "flink" / "v1.20" / "flink-runtime" / "build" / "libs"
        for jar in flink_runtime_dir.glob("iceberg-flink-runtime-1.20-*.jar"):
            if not jar.name.endswith("-sources.jar") and not jar.name.endswith("-javadoc.jar"):
                dest = lib_dir / jar.name
                shutil.copy(jar, dest)
                copied_jars.append(str(dest))

        aws_bundle_dir = iceberg_dir / "aws-bundle" / "build" / "libs"
        for jar in aws_bundle_dir.glob("iceberg-aws-bundle-*.jar"):
            if not jar.name.endswith("-sources.jar") and not jar.name.endswith("-javadoc.jar"):
                dest = lib_dir / jar.name
                shutil.copy(jar, dest)
                copied_jars.append(str(dest))

        if copied_jars:
            result["success"] = True
            result["message"] = f"Clean rebuilt and installed {len(copied_jars)} Iceberg JAR(s)"
            result["installed_jars"] = copied_jars
            result["restart_required"] = True
            result["restart_command"] = "devenv tasks run restart:clean"
        else:
            result["success"] = False
            result["message"] = "Build succeeded but no JARs found to copy"

    except subprocess.TimeoutExpired:
        result["success"] = False
        result["message"] = "Gradle build timed out after 15 minutes"
    except Exception as e:
        result["success"] = False
        result["message"] = f"Error rebuilding JARs: {e}"

    return result


async def _fix_datagen_bounded_source(dry_run: bool) -> dict[str, Any]:
    """Fix: Remove bounded row configuration from DataGen source.

    The datagen connector has 'fields.event_id.end' which makes it bounded.
    For continuous streaming, we need to remove this or use unbounded mode.
    """
    result = {
        "failure_mode_id": "FLINK_004",
        "action": "update_datagen_source",
    }

    # Find the cloudtrail_datagen.py file
    devenv_root = os.environ.get("DEVENV_ROOT", os.getcwd())
    datagen_file = Path(devenv_root) / "flink_jobs" / "cloudtrail_datagen.py"

    if not datagen_file.exists():
        result["success"] = False
        result["message"] = f"DataGen file not found at {datagen_file}"
        return result

    result["file"] = str(datagen_file)

    try:
        content = datagen_file.read_text()
        original_content = content

        # Pattern to find the bounded field configuration
        # Look for: 'fields.event_id.end' = '1000000' (or any number)
        bounded_pattern = r"'fields\.event_id\.end'\s*=\s*'[^']+'"

        if not re.search(bounded_pattern, content):
            result["success"] = True
            result["message"] = "DataGen source already unbounded (no fields.event_id.end found)"
            result["already_fixed"] = True
            return result

        if dry_run:
            result["success"] = True
            result["dry_run"] = True
            result["message"] = "Would remove bounded row configuration from DataGen source"
            result["changes"] = [
                "Remove: 'fields.event_id.end' = '1000000'",
                "This makes the datagen source unbounded (continuous streaming)",
            ]
            return result

        # Backup original
        backup_path = datagen_file.with_suffix(".py.bak")
        shutil.copy(datagen_file, backup_path)
        result["backup"] = str(backup_path)

        # Remove the bounded field line
        # The line looks like: 'fields.event_id.end' = '1000000',
        # We need to remove the entire line including the comma
        lines = content.split("\n")
        new_lines = []
        removed_lines = []

        for line in lines:
            if re.search(bounded_pattern, line):
                removed_lines.append(line.strip())
                continue
            new_lines.append(line)

        # Write updated content
        datagen_file.write_text("\n".join(new_lines))

        result["success"] = True
        result["message"] = "Removed bounded row configuration from DataGen source"
        result["removed_lines"] = removed_lines
        result["restart_required"] = True
        result["restart_command"] = "devenv tasks run restart:clean"

    except Exception as e:
        result["success"] = False
        result["message"] = f"Error updating DataGen source: {e}"

    return result


async def _fix_flink_not_running(flink_home: Path | None, dry_run: bool) -> dict[str, Any]:
    """Fix: Diagnose why Flink isn't running and attempt to start it.

    Checks for common causes:
    - Flink not built (missing bin/flink)
    - Process already running but unhealthy
    - Port conflicts
    - Missing JARs
    """
    import subprocess

    result: dict[str, Any] = {
        "action": "start_flink_cluster",
        "diagnostics": {},
    }

    # Check FLINK_HOME
    if not flink_home:
        result["success"] = False
        result["message"] = "FLINK_HOME not configured"
        result["diagnostics"]["flink_home"] = "not set"
        result["remediation"] = "Run: devenv tasks run restart:clean"
        return result

    result["diagnostics"]["flink_home"] = str(flink_home)

    # Check if Flink is built
    flink_bin = flink_home / "bin" / "flink"
    if not flink_bin.exists():
        result["success"] = False
        result["message"] = "Flink not built - bin/flink not found"
        result["diagnostics"]["flink_built"] = False
        result["remediation"] = "Run: devenv tasks run restart:clean (builds Flink from source)"
        return result

    result["diagnostics"]["flink_built"] = True

    # Check for existing Flink processes
    try:
        ps_result = subprocess.run(
            ["pgrep", "-f", "org.apache.flink"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if ps_result.stdout.strip():
            pids = ps_result.stdout.strip().split("\n")
            result["diagnostics"]["existing_processes"] = pids
            result["diagnostics"]["process_count"] = len(pids)
        else:
            result["diagnostics"]["existing_processes"] = []
            result["diagnostics"]["process_count"] = 0
    except Exception as e:
        result["diagnostics"]["process_check_error"] = str(e)

    # Check if port 8081 is in use
    try:
        lsof_result = subprocess.run(
            ["lsof", "-i", ":8081", "-t"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if lsof_result.stdout.strip():
            result["diagnostics"]["port_8081_pids"] = lsof_result.stdout.strip().split("\n")
        else:
            result["diagnostics"]["port_8081_pids"] = []
    except Exception:
        result["diagnostics"]["port_8081_pids"] = "check_failed"

    # Check for required JARs
    lib_dir = flink_home / "lib"
    if lib_dir.exists():
        iceberg_jars = list(lib_dir.glob("iceberg-flink-runtime-*.jar"))
        aws_bundle_jars = list(lib_dir.glob("iceberg-aws-bundle-*.jar"))
        result["diagnostics"]["iceberg_jars"] = [j.name for j in iceberg_jars]
        result["diagnostics"]["aws_bundle_jars"] = [j.name for j in aws_bundle_jars]
        result["diagnostics"]["jars_ok"] = bool(iceberg_jars) and bool(aws_bundle_jars)
    else:
        result["diagnostics"]["jars_ok"] = False
        result["diagnostics"]["lib_dir_exists"] = False

    # Check Flink logs for recent errors
    log_dir = flink_home / "log"
    if log_dir.exists():
        log_files = sorted(log_dir.glob("flink-*-jobmanager-*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
        if log_files:
            latest_log = log_files[0]
            try:
                # Read last 50 lines of most recent log
                with open(latest_log) as f:
                    lines = f.readlines()
                    last_lines = lines[-50:] if len(lines) > 50 else lines
                    # Look for errors
                    errors = [l.strip() for l in last_lines if "ERROR" in l or "Exception" in l]
                    if errors:
                        result["diagnostics"]["recent_log_errors"] = errors[-5:]  # Last 5 errors
                    result["diagnostics"]["latest_log"] = str(latest_log)
            except Exception as e:
                result["diagnostics"]["log_read_error"] = str(e)

    if dry_run:
        result["success"] = True
        result["dry_run"] = True
        result["message"] = "Would attempt to start Flink cluster"
        result["steps"] = [
            "Kill any orphaned Flink processes",
            f"Start JobManager: {flink_home}/bin/jobmanager.sh start",
            f"Start TaskManager: {flink_home}/bin/taskmanager.sh start",
            "Or use: devenv tasks run restart:clean",
        ]
        return result

    # Attempt to start Flink
    try:
        # Kill any orphaned processes first
        subprocess.run(["pkill", "-9", "-f", "org.apache.flink"], capture_output=True, timeout=5)
        import time
        time.sleep(2)

        # Start using start-cluster.sh
        start_script = flink_home / "bin" / "start-cluster.sh"
        if start_script.exists():
            start_proc = subprocess.run(
                [str(start_script)],
                capture_output=True,
                text=True,
                timeout=60,
                cwd=str(flink_home),
            )
            result["start_output"] = start_proc.stdout
            result["start_stderr"] = start_proc.stderr

            # Wait a bit and check if it's running
            time.sleep(5)
            import httpx
            try:
                resp = httpx.get("http://localhost:8081/overview", timeout=5.0)
                if resp.status_code == 200:
                    result["success"] = True
                    result["message"] = "Flink cluster started successfully"
                else:
                    result["success"] = False
                    result["message"] = f"Flink started but API returned {resp.status_code}"
            except Exception:
                result["success"] = False
                result["message"] = "Flink started but API not responding - check logs"
        else:
            result["success"] = False
            result["message"] = "start-cluster.sh not found"

    except Exception as e:
        result["success"] = False
        result["message"] = f"Failed to start Flink: {e}"

    return result


async def _fix_flink_cluster_restart(flink_home: Path | None, dry_run: bool) -> dict[str, Any]:
    """Fix: Restart Flink cluster to apply configuration changes."""
    import subprocess

    result = {
        "action": "restart_flink_cluster",
        "command": "devenv tasks run restart:clean",
    }

    if not flink_home or not flink_home.exists():
        result["success"] = False
        result["message"] = "FLINK_HOME not found - cannot restart cluster"
        return result

    if dry_run:
        result["success"] = True
        result["dry_run"] = True
        result["message"] = "Would restart Flink cluster via devenv tasks"
        result["steps"] = [
            f"Stop cluster: {flink_home}/bin/stop-cluster.sh",
            f"Start cluster: {flink_home}/bin/start-cluster.sh",
            "Or: devenv tasks run restart:clean",
        ]
        return result

    # Execute restart using stop/start scripts directly for targeted restart
    try:
        stop_script = flink_home / "bin" / "stop-cluster.sh"
        start_script = flink_home / "bin" / "start-cluster.sh"

        if not stop_script.exists() or not start_script.exists():
            result["success"] = False
            result["message"] = "Flink cluster scripts not found"
            return result

        # Stop cluster
        stop_proc = subprocess.run(
            [str(stop_script)],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(flink_home),
        )

        # Small delay to ensure clean shutdown
        import time
        time.sleep(2)

        # Start cluster
        start_proc = subprocess.run(
            [str(start_script)],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(flink_home),
        )

        if start_proc.returncode == 0:
            result["success"] = True
            result["message"] = "Flink cluster restarted successfully"
            result["stop_output"] = stop_proc.stdout
            result["start_output"] = start_proc.stdout
        else:
            result["success"] = False
            result["message"] = "Failed to restart Flink cluster"
            result["error"] = start_proc.stderr

    except subprocess.TimeoutExpired:
        result["success"] = False
        result["message"] = "Timeout restarting Flink cluster"
    except Exception as e:
        result["success"] = False
        result["message"] = f"Error restarting cluster: {e}"

    return result


async def _fix_shared_memory_exhaustion(dry_run: bool) -> dict[str, Any]:
    """Fix: Clean up orphaned shared memory segments using ipcrm.

    On macOS, orphaned IPC shared memory segments from previous devenv crashes
    can accumulate and exhaust system limits, preventing PostgreSQL from starting.
    """
    import getpass
    import subprocess

    result = {
        "failure_mode_id": "INFRA_004",
        "action": "cleanup_shared_memory",
    }

    # Check platform - this is primarily a macOS issue
    if platform.system() not in ("Darwin", "Linux"):
        result["success"] = True
        result["message"] = f"Platform {platform.system()} - shared memory cleanup not applicable"
        result["skipped"] = True
        return result

    # List current segments using ipcs -m
    try:
        list_proc = subprocess.run(
            ["ipcs", "-m"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if list_proc.returncode != 0:
            result["success"] = False
            result["message"] = f"Failed to list shared memory segments: {list_proc.stderr}"
            return result

        # Parse segments owned by current user
        current_user = getpass.getuser()
        segments_to_remove = []

        # ipcs -m output format varies by platform:
        # macOS: T ID KEY MODE OWNER GROUP
        # Linux: key shmid owner perms bytes nattch
        lines = list_proc.stdout.strip().split('\n')

        for line in lines:
            # Skip header lines
            if not line.strip() or line.startswith('---') or 'shmid' in line.lower() or 'key' in line.lower():
                continue

            parts = line.split()
            if len(parts) >= 3:
                # On macOS, format is: T ID KEY MODE OWNER GROUP
                # On Linux, format is: key shmid owner perms bytes nattch
                if current_user in line:
                    # Extract shmid - second field on macOS, second on Linux
                    if platform.system() == "Darwin":
                        # macOS: m 65536 0x00000000 --rw------- ryanhill staff
                        if len(parts) >= 2 and parts[0] in ('m', 's', 'q'):
                            shmid = parts[1]
                            segments_to_remove.append(shmid)
                    else:
                        # Linux: 0x00000000 65536 ryanhill 600 56 0
                        if len(parts) >= 2:
                            shmid = parts[1]
                            segments_to_remove.append(shmid)

        result["current_user"] = current_user
        result["segments_found"] = len(segments_to_remove)

        if not segments_to_remove:
            result["success"] = True
            result["message"] = "No orphaned shared memory segments found"
            return result

        result["segments"] = segments_to_remove

        if dry_run:
            result["success"] = True
            result["dry_run"] = True
            result["message"] = f"Would remove {len(segments_to_remove)} shared memory segment(s)"
            return result

        # Remove each segment
        removed = []
        failed = []

        for shmid in segments_to_remove:
            proc = subprocess.run(
                ["ipcrm", "-m", shmid],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if proc.returncode == 0:
                removed.append(shmid)
            else:
                failed.append({"shmid": shmid, "error": proc.stderr.strip()})

        result["removed"] = removed
        result["failed"] = failed
        result["success"] = len(removed) > 0
        result["message"] = f"Removed {len(removed)}/{len(segments_to_remove)} shared memory segment(s)"

        if removed:
            result["restart_required"] = True
            result["restart_command"] = "devenv up"

    except subprocess.TimeoutExpired:
        result["success"] = False
        result["message"] = "Timeout running ipcs/ipcrm commands"
    except FileNotFoundError:
        result["success"] = False
        result["message"] = "ipcs command not found - cannot list shared memory segments"
    except Exception as e:
        result["success"] = False
        result["message"] = f"Error cleaning up shared memory: {e}"

    return result


async def _fix_shared_memory_limits(dry_run: bool) -> dict[str, Any]:
    """Fix: Increase macOS shared memory kernel limits.

    On macOS, the default kern.sysv.shmmax (4MB) is too low for PostgreSQL
    and other services that use shared memory. This fix applies sysctl
    settings to increase the limits.

    Note: Requires sudo. For permanent fix, user should create /etc/sysctl.conf
    (macOS) or /etc/sysctl.d/99-postgresql.conf (Linux).
    """
    import platform
    import subprocess

    result: dict[str, Any] = {
        "failure_mode_id": "SYSTEM_001",
        "action": "increase_shared_memory_limits",
    }

    system = platform.system()

    # Only applies to macOS and Linux
    if system not in ("Darwin", "Linux"):
        result["success"] = True
        result["message"] = f"Platform {system} - shared memory limits fix not applicable"
        return result

    # Platform-specific sysctl keys
    if system == "Darwin":
        settings = {
            "kern.sysv.shmmax": "1073741824",  # 1GB
            "kern.sysv.shmall": "262144",       # pages
            "kern.sysv.shmmni": "256",          # segments
        }
        persistent_file = "/etc/sysctl.conf"
    else:  # Linux
        settings = {
            "kernel.shmmax": "1073741824",  # 1GB
            "kernel.shmall": "262144",       # pages
            "kernel.shmmni": "256",          # segments
        }
        persistent_file = "/etc/sysctl.d/99-postgresql.conf"

    if dry_run:
        result["success"] = True
        result["dry_run"] = True
        result["settings"] = settings
        result["message"] = (
            f"Would set shared memory limits: {settings}\n"
            "Note: This requires sudo. Run with --apply to execute."
        )
        return result

    # Apply settings
    errors = []
    applied = []

    for key, value in settings.items():
        try:
            proc = subprocess.run(
                ["sudo", "sysctl", "-w", f"{key}={value}"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if proc.returncode == 0:
                applied.append(f"{key}={value}")
            else:
                errors.append(f"{key}: {proc.stderr.strip()}")
        except subprocess.TimeoutExpired:
            errors.append(f"{key}: timeout")
        except Exception as e:
            errors.append(f"{key}: {e}")

    if errors:
        result["success"] = False
        result["applied"] = applied
        result["errors"] = errors
        result["message"] = f"Partially applied ({len(applied)}/{len(settings)}): {', '.join(errors)}"
    else:
        result["success"] = True
        result["applied"] = applied
        result["message"] = (
            f"Applied shared memory limits: {', '.join(applied)}\n"
            f"Note: For permanent fix, add to {persistent_file} and reboot."
        )

    return result


async def _fix_nifi_not_installed(dry_run: bool) -> dict[str, Any]:
    """Fix: Build NiFi from thirdparty/nifi submodule.

    Builds NiFi from source using the thirdparty/nifi git submodule,
    following the same pattern as Flink and Iceberg submodule builds.
    """
    import subprocess

    result: dict[str, Any] = {
        "failure_mode_id": "NIFI_001",
        "action": "build_nifi",
    }

    devenv_root = os.environ.get("DEVENV_ROOT", os.getcwd())
    nifi_dir = Path(devenv_root) / "thirdparty" / "nifi"
    nifi_version = "2.0.0"

    result["version"] = nifi_version

    # Check if submodule exists
    if not nifi_dir.exists():
        result["success"] = False
        result["message"] = f"NiFi submodule not found at {nifi_dir}"
        result["command"] = "git submodule update --init thirdparty/nifi"
        return result

    # Check if submodule is initialized (has pom.xml)
    pom_file = nifi_dir / "pom.xml"
    if not pom_file.exists():
        result["success"] = False
        result["message"] = "NiFi submodule not initialized"
        result["command"] = "git submodule update --init thirdparty/nifi"
        return result

    # Check for mvnw
    mvnw = nifi_dir / "mvnw"
    if not mvnw.exists():
        result["success"] = False
        result["message"] = "Maven wrapper not found in NiFi submodule"
        return result

    # Expected build output location
    build_output = nifi_dir / "nifi-assembly" / "target" / f"nifi-{nifi_version}-bin" / f"nifi-{nifi_version}"
    result["build_output"] = str(build_output)

    # Check if already built
    if build_output.exists() and (build_output / "bin" / "nifi.sh").exists():
        result["success"] = True
        result["message"] = f"NiFi already built at {build_output}"
        result["already_built"] = True
        return result

    result["source_dir"] = str(nifi_dir)

    if dry_run:
        result["success"] = True
        result["dry_run"] = True
        result["message"] = f"Would build NiFi {nifi_version} from thirdparty/nifi submodule"
        result["steps"] = [
            f"cd {nifi_dir}",
            "./mvnw clean install -DskipTests -T2C -Pinclude-grpc",
            f"NiFi will be built to: {build_output}",
        ]
        return result

    # Build NiFi from source
    try:
        result["build_output_log"] = []

        # Maven build command
        # -DskipTests: Skip tests for faster build
        # -T2C: Use 2 threads per CPU core
        # -Pinclude-grpc: Include gRPC support for OTLP
        build_cmd = [
            "./mvnw",
            "clean",
            "install",
            "-DskipTests",
            "-T2C",
            "-Pinclude-grpc",
            "-pl", "!nifi-external",  # Skip external modules that may have issues
        ]

        result["build_command"] = " ".join(build_cmd)

        # Run the build
        proc = subprocess.run(
            build_cmd,
            capture_output=True,
            text=True,
            timeout=1800,  # 30 minute timeout for build
            cwd=str(nifi_dir),
        )

        if proc.returncode != 0:
            result["success"] = False
            result["message"] = "NiFi Maven build failed"
            result["error"] = proc.stderr[-2000:] if len(proc.stderr) > 2000 else proc.stderr
            # Include last part of stdout which often has the actual error
            if proc.stdout:
                result["build_log_tail"] = proc.stdout[-2000:] if len(proc.stdout) > 2000 else proc.stdout
            return result

        # Verify build succeeded
        if build_output.exists() and (build_output / "bin" / "nifi.sh").exists():
            result["success"] = True
            result["message"] = f"Built NiFi {nifi_version} from source"
            result["nifi_home"] = str(build_output)
            result["restart_required"] = True
            result["restart_command"] = "devenv up nifi"
        else:
            result["success"] = False
            result["message"] = "Build succeeded but output not found"
            result["expected_location"] = str(build_output)
            # Check what was actually built
            target_dir = nifi_dir / "nifi-assembly" / "target"
            if target_dir.exists():
                result["target_contents"] = [p.name for p in target_dir.iterdir()][:10]

    except subprocess.TimeoutExpired:
        result["success"] = False
        result["message"] = "NiFi build timed out after 30 minutes"
    except Exception as e:
        result["success"] = False
        result["message"] = f"Error building NiFi: {e}"

    return result


async def _fix_pyflink_not_installed(dry_run: bool) -> dict[str, Any]:
    """Fix: Install PyFlink from thirdparty/flink-python via uv sync.

    PyFlink is an editable install of the vendored apache-flink 1.20.1 tree
    (cloudpickle 3.x / Dask patches). This does not require the Java Flink
    submodule. The fix:
    1. Verifies thirdparty/flink-python/setup.py exists
    2. Cleans up any shadowing pyflink directory from apache-flink-libraries
    3. Runs uv sync

    The apache-flink-libraries package can create a pyflink/ directory in
    site-packages that shadows the editable install, causing pyflink.__file__
    to be None. This fix removes that directory before running uv sync.
    """
    import subprocess

    result: dict[str, Any] = {
        "failure_mode_id": "PYFLINK_001",
        "action": "install_pyflink",
    }

    devenv_root = os.environ.get("DEVENV_ROOT", os.getcwd())
    flink_python_dir = Path(devenv_root) / "thirdparty" / "flink-python"

    if not (flink_python_dir / "setup.py").exists():
        result["success"] = False
        result["message"] = "Vendored PyFlink tree missing - thirdparty/flink-python/setup.py not found"
        result["command"] = "git checkout -- thirdparty/flink-python"
        result["flink_python_dir"] = str(flink_python_dir)
        return result

    result["flink_python_dir"] = str(flink_python_dir)

    # Check for shadowing pyflink directory in site-packages
    devenv_state = os.environ.get("DEVENV_STATE", Path(devenv_root) / ".devenv" / "state")
    site_packages = Path(devenv_state) / "venv" / "lib" / "python3.12" / "site-packages"
    shadowing_pyflink = site_packages / "pyflink"

    shadow_exists = False
    if shadowing_pyflink.exists():
        # Check if it's the problematic apache-flink-libraries directory
        # (contains bin/lib/opt but no __init__.py at top level)
        readme = shadowing_pyflink / "README.txt"
        if readme.exists() or not (shadowing_pyflink / "__init__.py").exists():
            shadow_exists = True
            result["shadowing_pyflink"] = str(shadowing_pyflink)

    if dry_run:
        result["success"] = True
        result["dry_run"] = True
        steps = []
        if shadow_exists:
            steps.append(f"Remove shadowing directory: {shadowing_pyflink}")
        steps.append("Run: uv sync (installs PyFlink from thirdparty/flink-python)")
        result["message"] = "Would install PyFlink from vendored thirdparty/flink-python"
        result["steps"] = steps
        return result

    try:
        # Step 1: Remove shadowing pyflink directory if present
        if shadow_exists:
            shutil.rmtree(shadowing_pyflink)
            result["removed_shadow"] = str(shadowing_pyflink)

        # Step 2: Run uv sync
        uv_proc = subprocess.run(
            ["uv", "sync"],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            cwd=devenv_root,
        )

        if uv_proc.returncode != 0:
            result["success"] = False
            result["message"] = "uv sync failed"
            result["error"] = uv_proc.stderr[-1000:] if len(uv_proc.stderr) > 1000 else uv_proc.stderr
            return result

        # Step 3: Verify PyFlink is now importable
        venv_python = site_packages.parent.parent.parent / "bin" / "python3"
        if venv_python.exists():
            verify_proc = subprocess.run(
                [str(venv_python), "-c", "import pyflink; print(pyflink.__version__)"],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if verify_proc.returncode == 0:
                result["success"] = True
                result["message"] = (
                    f"Installed PyFlink {verify_proc.stdout.strip()} from thirdparty/flink-python"
                )
                result["pyflink_version"] = verify_proc.stdout.strip()
            else:
                result["success"] = False
                result["message"] = "uv sync completed but PyFlink still not importable"
                result["verify_error"] = verify_proc.stderr[:500]
        else:
            # Can't verify but uv sync succeeded
            result["success"] = True
            result["message"] = "Installed PyFlink from thirdparty/flink-python (unable to verify)"

    except subprocess.TimeoutExpired:
        result["success"] = False
        result["message"] = "uv sync timed out after 5 minutes"
    except Exception as e:
        result["success"] = False
        result["message"] = f"Error installing PyFlink: {e}"

    return result


async def _fix_kubeconfig_stale(config: Any, dry_run: bool) -> dict[str, Any]:
    """Fix: Refresh user kubeconfig from system copy.

    Copies /etc/rancher/rke2/rke2.yaml to ~/.kube/rke2.yaml with correct
    ownership and permissions. Requires sudo.
    """
    from ..k8s.rke2 import refresh_kubeconfig, RKE2Config

    result: dict[str, Any] = {
        "failure_mode_id": "K8S_001",
        "action": "refresh_kubeconfig",
    }

    try:
        rke2_config = config.get_rke2_config()
    except (AttributeError, Exception):
        rke2_config = RKE2Config()

    refresh_result = refresh_kubeconfig(rke2_config, dry_run=dry_run)

    result["success"] = refresh_result.get("success", False)
    result["message"] = refresh_result.get("message", "")
    result["dry_run"] = dry_run
    if "command" in refresh_result:
        result["command"] = refresh_result["command"]

    return result
