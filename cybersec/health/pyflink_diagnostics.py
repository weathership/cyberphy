"""PyFlink diagnostic utilities.

Gathers diagnostic information when PyFlink jobs fail and provides
FMEA-based remediation recommendations.

Used by both CLI and MCP interfaces.
"""

import os
import sys
import platform
import subprocess
import glob
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from .catalog import get_failure_mode, get_category_modes


@dataclass
class DiagnosticCheck:
    """Result of a diagnostic check."""
    failure_mode_id: str
    detected: bool
    details: str
    severity: str  # "critical", "warning", "info"


async def _check_flink_job_status(flink_url: str) -> dict[str, Any]:
    """Check Flink cluster for job status issues.

    Detects:
    - FLINK_004: DataGen job finishes immediately (bounded source)
    - FLINK_005: Job stuck in CREATED/INITIALIZING state
    - PYFLINK_014: Iceberg JAR version mismatch (serialVersionUID error)

    Returns:
        Dict with job status analysis and detected issues.
    """
    import re

    result: dict[str, Any] = {
        "flink_url": flink_url,
        "cluster_reachable": False,
        "jobs_running": 0,
        "jobs_finished": 0,
        "jobs_failed": 0,
        "datagen_finished_immediately": False,
        "datagen_has_bounded_source": False,
        "job_stuck_initializing": False,
        "iceberg_version_mismatch": False,
        "details": [],
    }

    # First, check if the DataGen source file has bounded configuration
    # This catches the issue even when jobs aren't visible in history
    devenv_root = os.environ.get("DEVENV_ROOT", os.getcwd())
    datagen_file = Path(devenv_root) / "flink_jobs" / "cloudtrail_datagen.py"

    if datagen_file.exists():
        try:
            content = datagen_file.read_text()
            # Check for bounded row configuration
            if re.search(r"'fields\.event_id\.end'\s*=\s*'[^']+'", content):
                result["datagen_has_bounded_source"] = True
                result["details"].append(
                    "DataGen source has bounded rows (fields.event_id.end) - "
                    "job will finish after generating all events"
                )
        except Exception:
            pass

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Get cluster overview
            overview_resp = await client.get(f"{flink_url}/overview")
            if overview_resp.status_code != 200:
                result["details"].append(f"Flink cluster returned {overview_resp.status_code}")
                return result

            result["cluster_reachable"] = True
            overview = overview_resp.json()
            result["jobs_running"] = overview.get("jobs-running", 0)
            result["jobs_finished"] = overview.get("jobs-finished", 0)
            result["jobs_failed"] = overview.get("jobs-failed", 0)
            result["slots_total"] = overview.get("slots-total", 0)
            result["slots_available"] = overview.get("slots-available", 0)
            result["taskmanagers"] = overview.get("taskmanagers", 0)

            # Get detailed job list
            jobs_resp = await client.get(f"{flink_url}/jobs/overview")
            if jobs_resp.status_code == 200:
                jobs_data = jobs_resp.json()
                jobs = jobs_data.get("jobs", [])
                result["jobs"] = jobs

                # Analyze job patterns
                current_time_ms = int(time.time() * 1000)

                for job in jobs:
                    job_name = job.get("name", "")
                    job_state = job.get("state", "")
                    start_time = job.get("start-time", 0)
                    end_time = job.get("end-time", -1)
                    duration = job.get("duration", 0)

                    # Check for DataGen job that finished quickly
                    is_datagen = "datagen" in job_name.lower() or "cloudtrail" in job_name.lower()

                    if is_datagen and job_state == "FINISHED":
                        # Job finished - check if it was suspiciously quick
                        # If duration < 5 minutes and it's a streaming job, something's wrong
                        if duration > 0 and duration < 300000:  # < 5 minutes in ms
                            result["datagen_finished_immediately"] = True
                            result["details"].append(
                                f"DataGen job '{job_name}' finished in {duration/1000:.1f}s - "
                                "likely using bounded source (fields.event_id.end)"
                            )

                    # Check for jobs stuck in CREATED or INITIALIZING
                    if job_state in ("CREATED", "INITIALIZING"):
                        time_in_state = current_time_ms - start_time
                        if time_in_state > 60000:  # > 60 seconds
                            result["job_stuck_initializing"] = True
                            result["details"].append(
                                f"Job '{job_name}' stuck in {job_state} for {time_in_state/1000:.0f}s"
                            )

                # Check if we have finished jobs but no running jobs (DataGen completed)
                if result["jobs_running"] == 0 and result["jobs_finished"] > 0:
                    # Look for recent DataGen completions
                    for job in jobs:
                        job_name = job.get("name", "")
                        is_datagen = "datagen" in job_name.lower() or "cloudtrail" in job_name.lower()
                        if is_datagen and job.get("state") == "FINISHED":
                            end_time = job.get("end-time", 0)
                            # If finished in last 5 minutes, flag it
                            if end_time > 0 and (current_time_ms - end_time) < 300000:
                                if not result["datagen_finished_immediately"]:
                                    result["datagen_finished_immediately"] = True
                                    result["details"].append(
                                        f"DataGen job '{job_name}' recently completed - "
                                        "no running jobs found"
                                    )

                # Check for FAILED jobs and get their exceptions
                for job in jobs:
                    job_id = job.get("jid", "")
                    job_state = job.get("state", "")

                    if job_state == "FAILED" and job_id:
                        # Fetch job exceptions
                        try:
                            exc_resp = await client.get(f"{flink_url}/jobs/{job_id}/exceptions")
                            if exc_resp.status_code == 200:
                                exc_data = exc_resp.json()
                                root_exception = exc_data.get("root-exception", "")

                                # Check for Iceberg serialVersionUID mismatch
                                if "InvalidClassException" in root_exception and "serialVersionUID" in root_exception:
                                    if "org.apache.iceberg" in root_exception:
                                        result["iceberg_version_mismatch"] = True
                                        result["details"].append(
                                            "Iceberg JAR version mismatch detected: "
                                            "InvalidClassException with serialVersionUID conflict on org.apache.iceberg classes"
                                        )
                                        # Extract the class name for more detail
                                        import re
                                        class_match = re.search(r"InvalidClassException:\s*([\w.]+);", root_exception)
                                        if class_match:
                                            result["details"].append(
                                                f"Conflicting class: {class_match.group(1)}"
                                            )
                        except Exception:
                            pass  # Non-critical, continue checking other jobs

    except httpx.ConnectError:
        result["details"].append(f"Cannot connect to Flink at {flink_url}")
    except httpx.TimeoutException:
        result["details"].append(f"Timeout connecting to Flink at {flink_url}")
    except Exception as e:
        result["details"].append(f"Error checking Flink: {e}")

    return result


async def gather_pyflink_diagnostics() -> dict[str, Any]:
    """Gather diagnostic information for PyFlink job failures.

    Runs FMEA-based checks and returns detected issues with remediation steps.

    Returns:
        Diagnostic report with:
        - platform: System info
        - python_environment: Python paths and packages
        - flink_config: Flink settings
        - logs: Recent log entries
        - issues: Detected failure modes with remediation
        - recommendations: Prioritized action items
    """
    from ..bootstrap import BootstrapService

    diagnostics: dict[str, Any] = {
        "platform": {},
        "python_environment": {},
        "flink_config": {},
        "logs": {},
        "issues": [],
        "recommendations": [],
    }

    # === Gather diagnostic data ===

    # Platform info
    is_macos = platform.system() == "Darwin"
    diagnostics["platform"] = {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "is_macos": is_macos,
    }

    # Python environment
    diagnostics["python_environment"] = {
        "version": sys.version,
        "executable": sys.executable,
        "prefix": sys.prefix,
        "path": sys.path[:5],
        "pythonpath": os.environ.get("PYTHONPATH", "not set"),
    }

    # Check PyFlink
    pyflink_installed = False
    try:
        import pyflink
        diagnostics["python_environment"]["pyflink_version"] = getattr(pyflink, "__version__", "unknown")
        diagnostics["python_environment"]["pyflink_location"] = pyflink.__file__
        pyflink_installed = True
    except ImportError as e:
        diagnostics["python_environment"]["pyflink_error"] = str(e)

    # Check kafka-python
    kafka_installed = False
    try:
        import kafka
        diagnostics["python_environment"]["kafka_python"] = "installed"
        kafka_installed = True
    except ImportError:
        diagnostics["python_environment"]["kafka_python"] = "missing"

    # System python3
    try:
        result = subprocess.run(
            ["which", "python3"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        diagnostics["python_environment"]["system_python3"] = result.stdout.strip() or "not found"
    except Exception:
        diagnostics["python_environment"]["system_python3"] = "check failed"

    # Devenv python (macOS)
    devenv_python_exists = False
    if os.environ.get("DEVENV_ROOT"):
        devenv_python = os.environ.get("DEVENV_ROOT", "") + "/.devenv/profile/bin/python3"
        diagnostics["python_environment"]["devenv_python"] = devenv_python
        devenv_python_exists = Path(devenv_python).exists()
        diagnostics["python_environment"]["devenv_python_exists"] = devenv_python_exists

    # Flink configuration
    service = BootstrapService()
    config = service.get_config()
    flink_home = config.get_flink_home()

    # Check if FLINK_HOME env var is set (separate from bootstrap config)
    flink_home_env = os.environ.get("FLINK_HOME")
    flink_home_env_set = flink_home_env is not None and flink_home_env != ""

    flink_home_exists = flink_home.exists() if flink_home else False
    flink_binary_exists = False
    python_settings_configured = False

    diagnostics["flink_config"] = {
        "flink_home": str(flink_home) if flink_home else "not set",
        "flink_home_exists": flink_home_exists,
        "flink_home_env": flink_home_env or "not set",
        "flink_home_env_set": flink_home_env_set,
    }

    configured_python_path = None
    configured_python_exists = False
    flink_conf_mtime = None
    iceberg_aws_bundle_exists = False
    iceberg_flink_runtime_exists = False
    iceberg_submodule_initialized = False

    # Check if Iceberg submodule is initialized
    devenv_root = os.environ.get("DEVENV_ROOT", os.getcwd())
    iceberg_dir = Path(devenv_root) / "thirdparty" / "iceberg"
    iceberg_gradlew = iceberg_dir / "gradlew"
    iceberg_submodule_initialized = iceberg_gradlew.exists()
    diagnostics["flink_config"]["iceberg_submodule_initialized"] = iceberg_submodule_initialized

    if flink_home and flink_home_exists:
        flink_bin = flink_home / "bin" / "flink"
        flink_binary_exists = flink_bin.exists()
        diagnostics["flink_config"]["flink_binary_exists"] = flink_binary_exists

        # Check for Iceberg JARs in lib directory
        lib_dir = flink_home / "lib"
        if lib_dir.exists():
            iceberg_runtime_jars = list(lib_dir.glob("iceberg-flink-runtime-1.20-*.jar"))
            iceberg_aws_bundle_jars = list(lib_dir.glob("iceberg-aws-bundle-*.jar"))

            iceberg_flink_runtime_exists = bool(iceberg_runtime_jars)
            iceberg_aws_bundle_exists = bool(iceberg_aws_bundle_jars)

            diagnostics["flink_config"]["iceberg_jars"] = {
                "flink_runtime": [j.name for j in iceberg_runtime_jars] if iceberg_runtime_jars else "MISSING",
                "aws_bundle": [j.name for j in iceberg_aws_bundle_jars] if iceberg_aws_bundle_jars else "MISSING",
            }

        # Flink 1.20+ uses config.yaml — prefer the runtime overlay
        from cybersec.flink_paths import flink_conf_dir

        flink_conf = flink_conf_dir(flink_home) / "config.yaml"
        if flink_conf.exists():
            try:
                import yaml

                flink_conf_mtime = os.path.getmtime(flink_conf)
                diagnostics["flink_config"]["config_mtime"] = flink_conf_mtime

                content = flink_conf.read_text()
                config = yaml.safe_load(content) or {}
                python_config = config.get("python", {})

                # Extract Python settings from YAML structure
                python_settings = []
                if python_config.get("executable"):
                    python_settings.append(f"python.executable: {python_config['executable']}")
                    configured_python_path = python_config["executable"]
                client_config = python_config.get("client", {})
                if client_config.get("executable"):
                    python_settings.append(f"python.client.executable: {client_config['executable']}")
                    if not configured_python_path:
                        configured_python_path = client_config["executable"]

                diagnostics["flink_config"]["python_settings"] = python_settings or ["none configured"]
                python_settings_configured = len(python_settings) > 0

                if configured_python_path:
                    configured_python_exists = Path(configured_python_path).exists()
                    diagnostics["flink_config"]["configured_python_path"] = configured_python_path
                    diagnostics["flink_config"]["configured_python_exists"] = configured_python_exists

            except Exception as e:
                diagnostics["flink_config"]["config_error"] = str(e)

    # Check Flink process start times vs config mtime
    flink_process_stale = False
    taskmanager_start_time = None
    try:
        result = subprocess.run(
            ["pgrep", "-f", "TaskManager"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            tm_pid = result.stdout.strip().split()[0]
            # Get process start time
            stat_result = subprocess.run(
                ["ps", "-o", "lstart=", "-p", tm_pid],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if stat_result.returncode == 0:
                diagnostics["flink_config"]["taskmanager_pid"] = tm_pid
                diagnostics["flink_config"]["taskmanager_start"] = stat_result.stdout.strip()

                # Parse and compare times (approximate check)
                # If config was modified after TM started, cluster is stale
                if flink_conf_mtime:
                    # Use /proc on Linux for more precise timing
                    proc_stat = Path(f"/proc/{tm_pid}/stat")
                    if proc_stat.exists():
                        # Process start time from /proc
                        import time
                        boot_time = None
                        try:
                            with open("/proc/stat") as f:
                                for line in f:
                                    if line.startswith("btime"):
                                        boot_time = int(line.split()[1])
                                        break
                            with open(proc_stat) as f:
                                stat_fields = f.read().split()
                                # Field 22 is starttime in clock ticks since boot
                                starttime_ticks = int(stat_fields[21])
                                clk_tck = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
                                taskmanager_start_time = boot_time + (starttime_ticks / clk_tck)
                                diagnostics["flink_config"]["taskmanager_start_epoch"] = taskmanager_start_time

                                if flink_conf_mtime > taskmanager_start_time:
                                    flink_process_stale = True
                                    diagnostics["flink_config"]["process_stale"] = True
                                    diagnostics["flink_config"]["config_newer_than_process"] = True
                        except Exception:
                            pass
    except Exception as e:
        diagnostics["flink_config"]["process_check_error"] = str(e)

    # Logs
    submit_log_errors = []
    submit_log = Path("/tmp/cloudtrail_submit.log")
    if submit_log.exists():
        try:
            content = submit_log.read_text()
            lines = content.strip().split("\n")
            recent = lines[-30:] if len(lines) > 30 else lines
            submit_log_errors = [
                l for l in recent
                if "error" in l.lower() or "exception" in l.lower() or "failed" in l.lower()
            ]
            diagnostics["logs"]["submit_log"] = {
                "path": str(submit_log),
                "total_lines": len(lines),
                "recent_errors": submit_log_errors[-10:] if submit_log_errors else ["no errors found"],
                "last_10_lines": lines[-10:],
            }
        except Exception as e:
            diagnostics["logs"]["submit_log_error"] = str(e)
    else:
        diagnostics["logs"]["submit_log"] = "not found at /tmp/cloudtrail_submit.log"

    # TaskManager logs
    if flink_home and flink_home_exists:
        log_dir = flink_home / "log"
        if log_dir.exists():
            tm_logs = sorted(glob.glob(str(log_dir / "*taskmanager*.log")), key=os.path.getmtime, reverse=True)
            if tm_logs:
                try:
                    latest_tm = Path(tm_logs[0])
                    content = latest_tm.read_text()
                    lines = content.strip().split("\n")
                    python_errors = [
                        l for l in lines[-100:]
                        if "python" in l.lower() or "pyflink" in l.lower() or "PythonDriver" in l
                    ]
                    diagnostics["logs"]["taskmanager"] = {
                        "path": str(latest_tm),
                        "python_related_lines": python_errors[-20:] if python_errors else ["no python-related entries"],
                    }
                except Exception as e:
                    diagnostics["logs"]["taskmanager_error"] = str(e)

            jm_logs = sorted(glob.glob(str(log_dir / "*jobmanager*.log")), key=os.path.getmtime, reverse=True)
            if jm_logs:
                try:
                    latest_jm = Path(jm_logs[0])
                    content = latest_jm.read_text()
                    lines = content.strip().split("\n")
                    python_errors = [
                        l for l in lines[-100:]
                        if "python" in l.lower() or "pyflink" in l.lower() or "PythonDriver" in l
                    ]
                    diagnostics["logs"]["jobmanager"] = {
                        "path": str(latest_jm),
                        "python_related_lines": python_errors[-20:] if python_errors else ["no python-related entries"],
                    }
                except Exception as e:
                    diagnostics["logs"]["jobmanager_error"] = str(e)

    # === Run FMEA-based checks ===

    detected_issues: list[dict] = []
    recommendations: list[str] = []

    # PYFLINK_001: PyFlink Not Installed
    if not pyflink_installed:
        fm = get_failure_mode("PYFLINK_001")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_001",
                "name": fm.name,
                "severity": "critical",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
            })
            recommendations.extend(fm.remediation_steps)

    # PYFLINK_002: Python Path Mismatch (especially on macOS)
    if pyflink_installed and is_macos and not python_settings_configured:
        fm = get_failure_mode("PYFLINK_002")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_002",
                "name": fm.name,
                "severity": "critical" if is_macos else "warning",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
            })
            # Provide specific path for macOS
            if devenv_python_exists:
                devenv_path = diagnostics["python_environment"].get("devenv_python", "")
                recommendations.append(f"Add python section to $FLINK_HOME/conf/config.yaml:")
                recommendations.append(f"  python:")
                recommendations.append(f"    executable: {devenv_path}")
                recommendations.append(f"    client:")
                recommendations.append(f"      executable: {devenv_path}")
            else:
                recommendations.extend(fm.remediation_steps)

    # PYFLINK_003: kafka-python Missing
    if not kafka_installed:
        fm = get_failure_mode("PYFLINK_003")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_003",
                "name": fm.name,
                "severity": "warning",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
            })
            recommendations.extend(fm.remediation_steps)

    # PYFLINK_004: FLINK_HOME Not Set
    if not flink_home or not flink_home_exists:
        fm = get_failure_mode("PYFLINK_004")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_004",
                "name": fm.name,
                "severity": "critical",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
            })
            recommendations.extend(fm.remediation_steps)

    # PYFLINK_005: macOS Python Configuration
    if is_macos and flink_home_exists and not python_settings_configured:
        fm = get_failure_mode("PYFLINK_005")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_005",
                "name": fm.name,
                "severity": "critical",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
            })
            # Don't duplicate - PYFLINK_002 already added specific recommendations

    # PYFLINK_006: Job Submission Log Errors
    if submit_log_errors:
        fm = get_failure_mode("PYFLINK_006")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_006",
                "name": fm.name,
                "severity": "warning",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
                "details": submit_log_errors[:3],  # Include first 3 errors
            })
            recommendations.append("Review /tmp/cloudtrail_submit.log for error details")

    # PYFLINK_007: Config Written But Not Applied
    # Detect: config has python settings, but still getting "Python process exits with code: 1"
    python_exit_error = any("Python process exits with code: 1" in err for err in submit_log_errors)
    if python_settings_configured and python_exit_error:
        fm = get_failure_mode("PYFLINK_007")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_007",
                "name": fm.name,
                "severity": "critical",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
                "details": ["Config has Python settings but error persists - cluster likely not restarted"],
            })
            recommendations.insert(0, "Restart Flink cluster: devenv tasks run restart:clean")

    # PYFLINK_008: Flink Cluster Stale After Config Change
    if flink_process_stale:
        fm = get_failure_mode("PYFLINK_008")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_008",
                "name": fm.name,
                "severity": "critical",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
                "details": ["config.yaml modified after TaskManager started"],
            })
            recommendations.insert(0, "Restart Flink cluster to apply config: devenv tasks run restart:clean")

    # PYFLINK_009: Python Executable Not Found by Flink
    if configured_python_path and not configured_python_exists:
        fm = get_failure_mode("PYFLINK_009")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_009",
                "name": fm.name,
                "severity": "critical",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
                "details": [f"Configured path does not exist: {configured_python_path}"],
            })
            recommendations.insert(0, f"Fix Python path in config.yaml: {configured_python_path} not found")

    # PYFLINK_010: FLINK_HOME Not Exported
    if flink_home_exists and not flink_home_env_set:
        fm = get_failure_mode("PYFLINK_010")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_010",
                "name": fm.name,
                "severity": "info",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
                "details": [f"Bootstrap config has flink_home={flink_home}, but $FLINK_HOME not set in shell"],
            })
            recommendations.append(f"Export FLINK_HOME: export FLINK_HOME={flink_home}")

    # PYFLINK_011: Iceberg AWS Bundle Missing
    if flink_home_exists and not iceberg_aws_bundle_exists:
        fm = get_failure_mode("PYFLINK_011")
        if fm:
            rpn = fm.calculate_rpn()
            details = [f"iceberg-aws-bundle-*.jar not found in {flink_home}/lib/"]
            remediation = fm.remediation_steps.copy()

            # If submodule not initialized, add that as first step
            if not iceberg_submodule_initialized:
                details.insert(0, "Iceberg submodule not initialized")
                remediation.insert(0, "First run: git submodule update --init --recursive")

            detected_issues.append({
                "failure_mode_id": "PYFLINK_011",
                "name": fm.name,
                "severity": "critical",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": remediation,
                "details": details,
            })

            if not iceberg_submodule_initialized:
                recommendations.insert(0, "Run: git submodule update --init --recursive")
                recommendations.insert(1, "Then: cybersec bootstrap run (to build Iceberg JARs)")
            else:
                recommendations.insert(0, "Run: cybersec bootstrap run (to build and install Iceberg JARs)")

    # PYFLINK_012: Iceberg Flink Runtime Missing
    if flink_home_exists and not iceberg_flink_runtime_exists:
        fm = get_failure_mode("PYFLINK_012")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_012",
                "name": fm.name,
                "severity": "critical",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
                "details": [f"iceberg-flink-runtime-1.20-*.jar not found in {flink_home}/lib/"],
            })
            # Don't duplicate if PYFLINK_011 already added bootstrap recommendation
            if iceberg_aws_bundle_exists:
                recommendations.insert(0, "Run: cybersec bootstrap run (to build and install Iceberg JARs)")

    # === Flink Job Status Checks ===

    # Check Flink cluster and job status
    flink_url = config.flink_url or "http://localhost:8081"
    flink_job_status = await _check_flink_job_status(flink_url)
    diagnostics["flink_jobs"] = flink_job_status

    # FLINK_004: DataGen Job Finishes Immediately
    if flink_job_status.get("datagen_finished_immediately"):
        fm = get_failure_mode("FLINK_004")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "FLINK_004",
                "name": fm.name,
                "severity": "warning",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
                "details": flink_job_status.get("details", []),
            })
            recommendations.insert(0, "Fix DataGen source: remove bounded row limit or use unbounded mode")
            recommendations.insert(1, "Run: /health fix --apply")

    # FLINK_005: Job Submission Timeout
    if flink_job_status.get("job_stuck_initializing"):
        fm = get_failure_mode("FLINK_005")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "FLINK_005",
                "name": fm.name,
                "severity": "critical",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
                "details": flink_job_status.get("details", []),
            })
            recommendations.insert(0, "Check Flink cluster resources and logs")

    # PYFLINK_014: Iceberg JAR Version Mismatch
    if flink_job_status.get("iceberg_version_mismatch"):
        fm = get_failure_mode("PYFLINK_014")
        if fm:
            rpn = fm.calculate_rpn()
            detected_issues.append({
                "failure_mode_id": "PYFLINK_014",
                "name": fm.name,
                "severity": "critical",
                "symptom": fm.symptom,
                "rpn": rpn.rpn,
                "remediation": fm.remediation_steps,
                "details": flink_job_status.get("details", []),
            })
            recommendations.insert(0, "CRITICAL: Iceberg JAR version mismatch - rebuild JARs from source")
            recommendations.insert(1, "Run: /health fix --apply")

    # === Summary ===

    diagnostics["issues"] = detected_issues

    # Deduplicate and prioritize recommendations
    seen = set()
    unique_recommendations = []
    for rec in recommendations:
        if rec not in seen:
            seen.add(rec)
            unique_recommendations.append(rec)

    diagnostics["recommendations"] = unique_recommendations

    # Add status summary
    if detected_issues:
        critical_count = sum(1 for i in detected_issues if i["severity"] == "critical")
        warning_count = sum(1 for i in detected_issues if i["severity"] == "warning")
        diagnostics["status"] = {
            "healthy": False,
            "critical_issues": critical_count,
            "warning_issues": warning_count,
            "total_issues": len(detected_issues),
        }
    else:
        diagnostics["status"] = {
            "healthy": True,
            "critical_issues": 0,
            "warning_issues": 0,
            "total_issues": 0,
        }
        diagnostics["recommendations"].append("No issues detected. PyFlink environment appears healthy.")

    return diagnostics
