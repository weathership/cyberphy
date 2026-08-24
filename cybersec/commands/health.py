"""Health commands for unified command system.

Commands:
    /health                      - Run FMEA health diagnostics
    /health flink                - Check flink category
    /health fix                  - Dry-run all detected issues
    /health fix --apply          - Apply all fixes
    /health fix flink            - Dry-run flink category
    /health fix flink --apply    - Fix flink category
    /health diagnose <id>        - Diagnose specific failure mode
"""

from io import StringIO
from .parser import ParsedCommand, CommandResult
from .registry import register_command


async def cmd_health(cmd: ParsedCommand) -> CommandResult:
    """Run FMEA-based health diagnostics.

    Args:
        [category]  Optional: category to check (flink, rest-catalog, postgres, system, data)

    Options:
        --quick, -q           Run only critical checks
        --json, -j            Output as JSON
    """
    from ..bootstrap import BootstrapService
    from ..health.models import HealthContext
    from ..health.runner import run_health_check
    from ..health.catalog import CATEGORIES, CATEGORY_ALIASES

    service = BootstrapService()
    config = service.get_config()

    ctx = HealthContext(
        config=config,
        flink_url=config.flink_url or "http://localhost:8081",
        minio_endpoint=config.minio_endpoint or "http://localhost:9010",
        polaris_url=config.polaris_api_url or "http://localhost:8181",
        postgres_port=config.postgres_port or 5438,
        browser_port=config.iceberg_browser_port or 5050,
        nifi_url=config.nifi_url or "http://localhost:8450",
        nifi_otlp_port=config.nifi_otlp_port or 4319,
    )

    # Category can be argument or option (argument takes precedence)
    category = cmd.args[0] if cmd.args else cmd.options.get("category")
    if category:
        category = category.lower()
        # Resolve aliases
        category = CATEGORY_ALIASES.get(category, category)
        if category not in CATEGORIES:
            all_cats = ", ".join(sorted(CATEGORIES.keys()))
            return CommandResult(
                success=False,
                error=f"Unknown category: '{category}'. Available: {all_cats}",
            )
    quick = cmd.options.get("quick", False)

    report = await run_health_check(ctx, category=category, quick=quick)
    data = report.to_dict()

    # Format for human display
    formatted = _format_health_report(data)

    return CommandResult(
        success=True,
        data=data,
        formatted=formatted,
    )


async def cmd_health_pyflink(cmd: ParsedCommand) -> CommandResult:
    """Gather diagnostic information for PyFlink job failures.

    Use when PyFlink jobs fail with "Python process exits with code: 1".
    Shows diagnostics AND planned fixes (like terraform plan).
    Also runs conftest policy validation for comprehensive checks.

    Options:
        --json, -j  Output as JSON
    """
    from ..health.pyflink_diagnostics import gather_pyflink_diagnostics
    from ..health.fixes import apply_fixes
    from ..health.environment import run_conftest
    from ..config.runtime import gather_runtime_config, merge_runtime_config
    from ..config import load_config, hydrate_config
    from pathlib import Path
    import json

    # Gather FMEA-based diagnostics
    data = await gather_pyflink_diagnostics()

    # Run policy checks for additional validation
    try:
        # Load and hydrate config
        try:
            static_config = load_config("local")
            static_dict = hydrate_config(static_config)
        except FileNotFoundError:
            static_dict = {}

        runtime_config = await gather_runtime_config()
        merged = merge_runtime_config(static_dict, runtime_config)

        # Write config and run conftest
        config_path = Path("build/config.json")
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, "w") as f:
            json.dump(merged, f, indent=2)

        policy_result = run_conftest(config_path)
        data["policy_check"] = {
            "failures": policy_result.get("failures", []),
            "warnings": policy_result.get("warnings", []),
            "success": policy_result.get("success", True),
        }

        # Add policy failures as issues if not already detected by FMEA
        for failure in policy_result.get("failures", []):
            # Check if this issue is already in FMEA issues
            if not any(failure in str(issue) for issue in data.get("issues", [])):
                data.setdefault("issues", []).append({
                    "failure_mode_id": "POLICY",
                    "name": "Policy Violation",
                    "severity": "critical",
                    "symptom": failure,
                    "rpn": 0,
                    "remediation": [failure],
                })

    except Exception as e:
        data["policy_check"] = {"error": str(e)}

    # Compute planned fixes (dry-run) to show what would be changed
    issues = data.get("issues", [])
    if issues:
        planned_fixes = await apply_fixes(data, dry_run=True)
        data["planned_fixes"] = planned_fixes

    formatted = _format_pyflink_diagnostics(data)

    return CommandResult(
        success=True,
        data=data,
        formatted=formatted,
    )


async def cmd_health_fix_pyflink(cmd: ParsedCommand) -> CommandResult:
    """Fix detected PyFlink issues.

    DEPRECATED: Use `/health fix pyflink` instead. This function is kept
    for internal use but is no longer registered as a command.

    Runs diagnostics, identifies issues, and applies fixes.
    Default is dry-run. Use --apply to execute changes.

    Options:
        --apply     Apply fixes (default: dry-run preview)
        --json, -j  Output as JSON
    """
    from ..health.pyflink_diagnostics import gather_pyflink_diagnostics
    from ..health.fixes import apply_fixes

    dry_run = cmd.options.get("dry-run", False) or cmd.options.get("dry_run", False)

    # First gather diagnostics
    diagnostics = await gather_pyflink_diagnostics()
    issues = diagnostics.get("issues", [])

    if not issues:
        return CommandResult(
            success=True,
            data={"status": "healthy", "fixes_applied": []},
            formatted="✓ No issues detected. PyFlink environment is healthy.",
        )

    # Apply fixes
    fix_results = await apply_fixes(diagnostics, dry_run=dry_run)

    data = {
        "dry_run": dry_run,
        "issues_found": len(issues),
        "fixes": fix_results,
    }

    formatted = _format_pyflink_fixes(fix_results, dry_run)

    all_success = all(f.get("success", False) for f in fix_results)

    return CommandResult(
        success=all_success or dry_run,
        data=data,
        formatted=formatted,
    )


async def cmd_health_diagnose(cmd: ParsedCommand) -> CommandResult:
    """Get detailed diagnosis for a specific failure mode.

    Args:
        <id>  Failure mode ID (e.g., FLINK_001, ICE_002)

    Options:
        --json, -j  Output as JSON
    """
    from ..bootstrap import BootstrapService
    from ..health.models import HealthContext
    from ..health.runner import get_runner

    if not cmd.args:
        return CommandResult(
            success=False,
            error="Missing required argument: failure_mode_id (e.g., FLINK_001)",
        )

    failure_mode_id = cmd.args[0].upper()

    service = BootstrapService()
    config = service.get_config()

    ctx = HealthContext(
        config=config,
        flink_url=config.flink_url or "http://localhost:8081",
        minio_endpoint=config.minio_endpoint or "http://localhost:9010",
        polaris_url=config.polaris_api_url or "http://localhost:8181",
        postgres_port=config.postgres_port or 5438,
        browser_port=config.iceberg_browser_port or 5050,
        nifi_url=config.nifi_url or "http://localhost:8450",
        nifi_otlp_port=config.nifi_otlp_port or 4319,
    )

    runner = get_runner()
    data = await runner.diagnose(failure_mode_id, ctx)

    if data.get("error"):
        return CommandResult(
            success=False,
            error=data["error"],
        )

    formatted = _format_diagnosis(data, failure_mode_id)

    return CommandResult(
        success=True,
        data=data,
        formatted=formatted,
    )


async def cmd_health_fix(cmd: ParsedCommand) -> CommandResult:
    """Fix detected health issues.

    With no arguments, runs all checks and fixes all detected issues.
    With a category, fixes only issues in that category.
    With a failure mode ID, fixes only that specific issue.

    Args:
        [target]  Optional: category (flink, pyflink, rest-catalog, local-s3, postgres, system)
                  or failure mode ID (FLINK_001, INFRA_004)

    Options:
        --apply     Apply fixes (default: dry-run preview)
        --json, -j  Output as JSON
    """
    from ..bootstrap import BootstrapService
    from ..health.catalog import (
        get_failure_mode, get_category_mode_ids, get_all_categories,
        FAILURE_MODES, CATEGORIES, CATEGORY_ALIASES,
    )
    from ..health.models import HealthContext
    from ..health.runner import run_health_check
    from ..health.fixes import apply_fixes

    dry_run = not cmd.options.get("apply", False)
    target = cmd.args[0] if cmd.args else None

    # Set up health context for running checks
    service = BootstrapService()
    config = service.get_config()
    ctx = HealthContext(
        config=config,
        flink_url=config.flink_url or "http://localhost:8081",
        minio_endpoint=config.minio_endpoint or "http://localhost:9010",
        polaris_url=config.polaris_api_url or "http://localhost:8181",
        postgres_port=config.postgres_port or 5438,
        browser_port=config.iceberg_browser_port or 5050,
        nifi_url=config.nifi_url or "http://localhost:8450",
        nifi_otlp_port=config.nifi_otlp_port or 4319,
    )

    # Automatable failure modes (can be fixed without user intervention)
    #
    # DESIGN: Health fixes address ROOT CAUSES, not service restarts.
    # Service lifecycle is devenv/process-compose's job.
    #
    # NOT automatable (superficial restarts - use devenv instead):
    #   - FLINK_001: Flink not running -> devenv tasks run restart:clean
    #   - INFRA_001: PostgreSQL down -> devenv up postgres
    #   - INFRA_002: MinIO unhealthy -> devenv up minio
    #
    # NOT automatable (require user action):
    #   - PYFLINK_003: kafka-python missing -> uv add kafka-python && uv sync
    #   - PYFLINK_004: FLINK_HOME not set -> environment setup
    #   - PYFLINK_006: Log errors -> diagnostic review
    #   - PYFLINK_010: FLINK_HOME not exported -> shell config
    #   - PYFLINK_013: Submodules not initialized -> git submodule update
    #   - FLINK_005: Job stuck -> manual investigation
    #
    automatable_fixes = {
        # Root cause fixes (detect issues before services fail)
        "SYSTEM_001",  # Shared memory limits too low -> increase kern.sysv.*
        "INFRA_004",  # Orphaned shared memory segments -> ipcrm cleanup
        # Build/install fixes (submodule-aware)
        "PYFLINK_001",  # PyFlink not installed -> uv sync from thirdparty/flink-python
        "PYFLINK_011", "PYFLINK_012", "PYFLINK_014",  # Iceberg JARs -> build from submodule
        "NIFI_001",   # NiFi not installed -> build from submodule
        # Configuration fixes
        "PYFLINK_002", "PYFLINK_005", "PYFLINK_009",  # Python path in config.yaml
        "PYFLINK_007", "PYFLINK_008",  # Cluster restart for config changes
        "FLINK_004",  # DataGen bounded source fix
    }

    # Determine what to fix
    if target is None:
        # Fix-all mode: run health check, fix all detected issues
        report = await run_health_check(ctx)
        issues = report.to_dict().get("issues", [])
        mode = "all"
        target_desc = "all detected issues"
    elif target.upper() in FAILURE_MODES:
        # Specific failure mode ID
        failure_mode_id = target.upper()
        failure_mode = get_failure_mode(failure_mode_id)
        issues = [{
            "failure_mode_id": failure_mode_id,
            "name": failure_mode.name,
            "severity": "warning",
            "symptom": failure_mode.symptom,
            "rpn": failure_mode.calculate_rpn().rpn,
            "remediation": failure_mode.remediation_steps,
        }]
        mode = "single"
        target_desc = f"{failure_mode_id} ({failure_mode.name})"
    elif target.lower() in CATEGORIES or target.lower() in CATEGORY_ALIASES:
        # Category-based fix
        category = target.lower()
        mode_ids = get_category_mode_ids(category)
        if not mode_ids:
            return CommandResult(
                success=True,
                data={"category": category, "issues": []},
                formatted=f"Category '{category}' has no failure modes defined yet.",
            )
        # Run health check filtered to this category
        report = await run_health_check(ctx, category=category)
        issues = report.to_dict().get("issues", [])
        mode = "category"
        target_desc = f"category '{category}'"
    else:
        # Unknown target
        all_cats = ", ".join(sorted(CATEGORIES.keys()))
        return CommandResult(
            success=False,
            error=f"Unknown target: '{target}'. Use a category ({all_cats}) or failure mode ID (e.g., FLINK_001).",
        )

    # Filter to only automatable issues
    fixable_issues = [i for i in issues if i.get("failure_mode_id") in automatable_fixes]
    non_fixable_issues = [i for i in issues if i.get("failure_mode_id") not in automatable_fixes]

    if not issues:
        return CommandResult(
            success=True,
            data={"mode": mode, "target": target, "issues_found": 0, "fixes": []},
            formatted=f"No issues detected for {target_desc}.",
        )

    if not fixable_issues:
        # Only non-automatable issues found
        lines = [f"Found {len(issues)} issue(s) for {target_desc}, but none have automated fixes:"]
        for issue in non_fixable_issues:
            fm_id = issue.get("failure_mode_id", "?")
            name = issue.get("name", issue.get("message", "Unknown"))
            lines.append(f"  - [{fm_id}] {name}")
            remediation = issue.get("remediation")
            if remediation:
                if isinstance(remediation, list) and remediation:
                    lines.append(f"    → {remediation[0]}")
                elif isinstance(remediation, str):
                    lines.append(f"    → {remediation}")
        return CommandResult(
            success=True,
            data={"mode": mode, "issues": issues, "fixes": []},
            formatted="\n".join(lines),
        )

    # Build diagnostics dict for apply_fixes
    diagnostics = {
        "issues": fixable_issues,
        "python_environment": {
            "executable": __import__("sys").executable,
            "devenv_python": __import__("os").environ.get("DEVENV_ROOT", "") + "/.devenv/profile/bin/python3",
            "devenv_python_exists": True,
        },
    }

    # Apply fixes
    fix_results = await apply_fixes(diagnostics, dry_run=dry_run)

    data = {
        "mode": mode,
        "target": target,
        "dry_run": dry_run,
        "issues_found": len(issues),
        "fixable_issues": len(fixable_issues),
        "non_fixable_issues": len(non_fixable_issues),
        "fixes": fix_results,
    }

    formatted = _format_health_fixes(fix_results, dry_run, target_desc, non_fixable_issues)

    all_success = all(f.get("success", False) for f in fix_results) if fix_results else True

    return CommandResult(
        success=all_success or dry_run,
        data=data,
        formatted=formatted,
    )


def _format_single_fix(fix_result: dict, failure_mode, dry_run: bool) -> str:
    """Format a single fix result for human display."""
    lines = []
    fm_id = fix_result.get("failure_mode_id", "?")
    action = fix_result.get("action", "unknown")
    success = fix_result.get("success", False)
    message = fix_result.get("message", "")

    lines.append(f"Fix: {fm_id} - {failure_mode.name}")
    lines.append("=" * 50)
    lines.append("")

    if dry_run:
        lines.append("DRY RUN - Preview of changes:")
        lines.append("")

        if action == "update_datagen_source":
            lines.append(f"  File: {fix_result.get('file', 'unknown')}")
            changes = fix_result.get("changes", [])
            for change in changes:
                lines.append(f"  → {change}")

        elif action == "update_flink_config":
            lines.append(f"  Config: {fix_result.get('config_file', 'unknown')}")
            if fix_result.get("lines_to_add"):
                lines.append("  Add:")
                for line in fix_result["lines_to_add"]:
                    lines.append(f"    + {line}")

        elif action == "rebuild_iceberg_jars":
            lines.append(f"  {message}")
            if fix_result.get("existing_jars"):
                lines.append(f"  Existing JARs to remove: {len(fix_result['existing_jars'])}")
            if fix_result.get("steps"):
                lines.append("")
                lines.append("  Steps:")
                for i, step in enumerate(fix_result["steps"], 1):
                    lines.append(f"    {i}. {step}")

        elif action == "cleanup_shared_memory":
            lines.append(f"  {message}")
            if fix_result.get("segments"):
                lines.append(f"  Segments to clean: {len(fix_result['segments'])}")
                for seg in fix_result["segments"][:5]:  # Show first 5
                    lines.append(f"    - shmid {seg}")
                if len(fix_result["segments"]) > 5:
                    lines.append(f"    ... and {len(fix_result['segments']) - 5} more")

        else:
            lines.append(f"  {message}")

        lines.append("")
        lines.append("To apply:")
        lines.append(f"  /health fix --apply")

    else:
        icon = "✓" if success else "✗"
        lines.append(f"{icon} {message}")

        if fix_result.get("backup"):
            lines.append(f"  Backup: {fix_result['backup']}")

        if fix_result.get("removed_lines"):
            lines.append("  Removed:")
            for line in fix_result["removed_lines"]:
                lines.append(f"    - {line}")

        # Handle shared memory cleanup results
        if fix_result.get("removed"):
            lines.append(f"  Cleaned up segments: {len(fix_result['removed'])}")
        if fix_result.get("failed"):
            lines.append(f"  Failed to clean: {len(fix_result['failed'])}")
            for f in fix_result["failed"][:3]:
                lines.append(f"    - shmid {f['shmid']}: {f.get('error', 'unknown error')}")

        if fix_result.get("restart_required"):
            lines.append("")
            lines.append("⚠ Restart required to apply changes:")
            lines.append(f"  {fix_result.get('restart_command', 'devenv tasks run restart:clean')}")

        if fix_result.get("already_fixed"):
            lines.append("  (No changes needed - already in correct state)")

    return "\n".join(lines)


def _format_health_fixes(fix_results: list, dry_run: bool, target_desc: str, non_fixable: list) -> str:
    """Format health fix results for human display."""
    lines = []

    if dry_run:
        lines.append(f"Health Fix Preview ({target_desc})")
        lines.append("=" * 50)
        lines.append("")
        lines.append("DRY RUN - The following changes would be made:")
        lines.append("")
    else:
        lines.append(f"Health Fix Applied ({target_desc})")
        lines.append("=" * 50)
        lines.append("")

    # Show fix results
    for fix in fix_results:
        fm_id = fix.get("failure_mode_id", "?")
        action = fix.get("action", "unknown")
        success = fix.get("success", False)
        message = fix.get("message", "")

        if dry_run:
            if action == "install_package":
                lines.append(f"  + [{fm_id}] Would install: {fix.get('package', 'unknown')}")
            elif action == "update_flink_config":
                lines.append(f"  ~ [{fm_id}] Would update config.yaml")
            elif action == "rebuild_iceberg_jars":
                lines.append(f"  ~ [{fm_id}] Would rebuild Iceberg JARs")
            elif action == "cleanup_shared_memory":
                segs = fix.get("segments", [])
                lines.append(f"  ~ [{fm_id}] Would clean {len(segs)} shared memory segment(s)")
            else:
                lines.append(f"  ~ [{fm_id}] {message}")
        else:
            icon = "✓" if success else "✗"
            lines.append(f"  {icon} [{fm_id}] {message}")

    # Show non-fixable issues
    if non_fixable:
        lines.append("")
        lines.append("Manual intervention needed:")
        for issue in non_fixable:
            fm_id = issue.get("failure_mode_id", "?")
            name = issue.get("name", issue.get("message", "Unknown"))
            lines.append(f"  ! [{fm_id}] {name}")

    # Summary
    lines.append("")
    success_count = sum(1 for f in fix_results if f.get("success"))
    total = len(fix_results)

    if dry_run:
        lines.append(f"Preview: {total} fix(es) would be applied")
        lines.append("")
        lines.append("To apply:")
        lines.append("  /health fix --apply")
    else:
        lines.append(f"Applied: {success_count}/{total} fix(es)")
        if any(f.get("restart_required") for f in fix_results):
            lines.append("")
            lines.append("⚠ Restart required:")
            lines.append("  devenv tasks run restart:clean")

    return "\n".join(lines)


# === Formatting helpers ===

def _format_health_report(data: dict) -> str:
    """Format health report for human display."""
    lines = []
    status = data.get("status", "unknown")
    status_marker = {"healthy": "✓", "degraded": "⚠", "critical": "✗"}.get(status, "?")

    lines.append(f"Health Status: {status_marker} {status.upper()}")
    lines.append("")

    checks = data.get("checks", {})
    next_steps = []  # Collect hints for next steps

    for cat_name, cat_checks in checks.items():
        lines.append(f"{cat_name.upper()}:")
        for check in cat_checks:
            check_status = check.get("status", "unknown")
            icon = {"ok": "✓", "warning": "⚠", "critical": "✗", "error": "!", "skipped": "-"}.get(check_status, "?")
            lines.append(f"  {icon} {check.get('message', check.get('name', 'unknown'))}")
            if check.get("remediation"):
                lines.append(f"      → {check['remediation']}")

            # Collect next steps based on issues
            msg = check.get("message", "")
            if "Table does not exist" in msg or "Table not found" in msg:
                next_steps.append(("pyflink", "No Iceberg table - DataGen job may not have run"))
            elif "No running jobs" in msg:
                next_steps.append(("pyflink", "No Flink jobs running - check PyFlink configuration"))
            elif check_status in ("critical", "error"):
                if cat_name.lower() == "pyflink":
                    next_steps.append(("pyflink", check.get("message", "")))
                elif cat_name.lower() == "flink":
                    next_steps.append(("pyflink", check.get("message", "")))
        lines.append("")

    issues = data.get("issues", [])
    if issues:
        lines.append("Issues Detected:")
        for issue in issues:
            lines.append(f"  • [{issue.get('failure_mode_id', '?')}] {issue.get('message', 'unknown')}")
            lines.append(f"    RPN: {issue.get('rpn', 'N/A')} | {issue.get('remediation', 'No remediation')}")
        lines.append("")

    recommendations = data.get("recommendations", [])
    if recommendations:
        lines.append("Recommendations:")
        for rec in recommendations:
            lines.append(f"  • {rec}")
        lines.append("")

    # Add next steps guidance only if there are actual issues to fix
    if issues:
        lines.append("Next Steps:")
        lines.append("  Fix detected issues:")
        lines.append("    /health fix --apply")

    return "\n".join(lines)


def _format_pyflink_diagnostics(data: dict) -> str:
    """Format PyFlink diagnostics for human display."""
    lines = []
    lines.append("PyFlink Diagnostics")
    lines.append("=" * 40)

    # Platform
    platform = data.get("platform", {})
    lines.append("")
    lines.append("Platform:")
    lines.append(f"  System: {platform.get('system', 'unknown')}")
    lines.append(f"  Release: {platform.get('release', 'unknown')}")
    lines.append(f"  Machine: {platform.get('machine', 'unknown')}")
    if platform.get("is_macos"):
        lines.append("  ⚠ macOS detected - check Python path configuration")

    # Python environment
    py_env = data.get("python_environment", {})
    lines.append("")
    lines.append("Python Environment:")
    lines.append(f"  Version: {py_env.get('version', 'unknown').split()[0]}")
    lines.append(f"  Executable: {py_env.get('executable', 'unknown')}")
    lines.append(f"  System python3: {py_env.get('system_python3', 'unknown')}")
    lines.append(f"  PYTHONPATH: {py_env.get('pythonpath', 'not set')}")

    if py_env.get("pyflink_version"):
        lines.append(f"  ✓ PyFlink: {py_env.get('pyflink_version')}")
        lines.append(f"    Location: {py_env.get('pyflink_location', 'unknown')}")
    elif py_env.get("pyflink_error"):
        lines.append(f"  ✗ PyFlink: NOT INSTALLED")
        lines.append(f"    Error: {py_env.get('pyflink_error')}")

    kafka_status = py_env.get("kafka_python", "unknown")
    if kafka_status == "installed":
        lines.append(f"  ✓ kafka-python: installed")
    else:
        lines.append(f"  ✗ kafka-python: {kafka_status}")

    if py_env.get("devenv_python"):
        lines.append(f"  Devenv Python: {py_env.get('devenv_python')}")
        exists = py_env.get("devenv_python_exists", False)
        lines.append(f"    Exists: {'yes' if exists else 'no'}")

    # Flink config
    flink_cfg = data.get("flink_config", {})
    lines.append("")
    lines.append("Flink Configuration:")
    lines.append(f"  FLINK_HOME: {flink_cfg.get('flink_home', 'not set')}")
    lines.append(f"  Exists: {'yes' if flink_cfg.get('flink_home_exists') else 'no'}")
    lines.append(f"  Binary exists: {'yes' if flink_cfg.get('flink_binary_exists') else 'no'}")

    python_settings = flink_cfg.get("python_settings", [])
    if python_settings and python_settings != ["none configured"]:
        lines.append("  Python settings in config.yaml:")
        for setting in python_settings:
            lines.append(f"    {setting}")
    else:
        lines.append("  No Python settings in config.yaml")

    # Logs
    logs = data.get("logs", {})
    lines.append("")
    lines.append("Logs:")

    submit_log = logs.get("submit_log")
    if isinstance(submit_log, dict):
        lines.append(f"  Submit log: {submit_log.get('path', 'unknown')}")
        lines.append(f"    Total lines: {submit_log.get('total_lines', 0)}")
        errors = submit_log.get("recent_errors", [])
        if errors and errors != ["no errors found in recent lines"]:
            lines.append("    Recent errors:")
            for err in errors[-5:]:
                lines.append(f"      {err[:80]}...")
        else:
            lines.append("    ✓ No recent errors")
    else:
        lines.append(f"  Submit log: {submit_log}")

    tm_log = logs.get("taskmanager")
    if isinstance(tm_log, dict):
        lines.append(f"  TaskManager log: {tm_log.get('path', 'unknown')}")
        py_lines = tm_log.get("python_related_lines", [])
        if py_lines and py_lines != ["no python-related entries"]:
            lines.append("    Python-related entries:")
            for line in py_lines[-3:]:
                lines.append(f"      {line[:80]}...")

    jm_log = logs.get("jobmanager")
    if isinstance(jm_log, dict):
        lines.append(f"  JobManager log: {jm_log.get('path', 'unknown')}")
        py_lines = jm_log.get("python_related_lines", [])
        if py_lines and py_lines != ["no python-related entries"]:
            lines.append("    Python-related entries:")
            for line in py_lines[-3:]:
                lines.append(f"      {line[:80]}...")

    # Issues (FMEA-based)
    issues = data.get("issues", [])
    if issues:
        lines.append("")
        lines.append("=" * 40)
        lines.append("DETECTED ISSUES")
        lines.append("=" * 40)
        for issue in issues:
            severity = issue.get("severity", "unknown")
            severity_icon = {"critical": "✗", "warning": "⚠", "info": "ℹ"}.get(severity, "?")
            lines.append("")
            lines.append(f"{severity_icon} [{issue.get('failure_mode_id', '?')}] {issue.get('name', 'Unknown')}")
            lines.append(f"  Severity: {severity.upper()} (RPN: {issue.get('rpn', 'N/A')})")
            lines.append(f"  Symptom: {issue.get('symptom', 'N/A')}")
            remediation = issue.get("remediation", [])
            if remediation:
                lines.append("  Remediation:")
                for step in remediation:
                    lines.append(f"    → {step}")
            if issue.get("details"):
                lines.append("  Log errors:")
                for detail in issue.get("details", [])[:3]:
                    lines.append(f"    {detail[:70]}...")

    # Policy check results
    policy_check = data.get("policy_check", {})
    if policy_check:
        lines.append("")
        lines.append("=" * 40)
        lines.append("POLICY VALIDATION")
        lines.append("=" * 40)

        if policy_check.get("error"):
            lines.append(f"  ⚠ Policy check error: {policy_check['error']}")
        else:
            failures = policy_check.get("failures", [])
            warnings = policy_check.get("warnings", [])

            if failures:
                lines.append("  Failures:")
                for msg in failures:
                    lines.append(f"    ✗ {msg}")

            if warnings:
                lines.append("  Warnings:")
                for msg in warnings:
                    lines.append(f"    ⚠ {msg}")

            if not failures and not warnings:
                lines.append("  ✓ All policy checks passed")
            elif not failures:
                lines.append(f"  ✓ Passed with {len(warnings)} warning(s)")

    # Recommendations (prioritized action items)
    recommendations = data.get("recommendations", [])
    if recommendations:
        lines.append("")
        lines.append("=" * 40)
        lines.append("RECOMMENDED ACTIONS")
        lines.append("=" * 40)
        for i, rec in enumerate(recommendations, 1):
            lines.append(f"  {i}. {rec}")

    # Planned fixes (Terraform-style plan)
    planned_fixes = data.get("planned_fixes", [])
    if planned_fixes:
        lines.append("")
        lines.append("=" * 40)
        lines.append("PLANNED CHANGES")
        lines.append("=" * 40)
        lines.append("The following changes would be made by '/health fix pyflink':")
        lines.append("")

        for fix in planned_fixes:
            fm_id = fix.get("failure_mode_id", "?")
            action = fix.get("action", "unknown")
            message = fix.get("message", "")

            if action == "install_package":
                lines.append(f"  + [{fm_id}] Install package: {fix.get('package', 'unknown')}")
                lines.append(f"      Command: {fix.get('command', '')}")

            elif action == "update_flink_config":
                lines.append(f"  ~ [{fm_id}] Update config.yaml")
                lines.append(f"      Config: {fix.get('config_file', 'unknown')}")
                if fix.get("lines_to_add"):
                    lines.append("      Add:")
                    for line in fix["lines_to_add"]:
                        lines.append(f"        + {line}")

            elif action == "manual_required":
                lines.append(f"  ! [{fm_id}] Manual action required")
                lines.append(f"      {message}")

            elif action == "review_required":
                lines.append(f"  ? [{fm_id}] Review recommended")
                lines.append(f"      {fix.get('details', message)}")

            lines.append("")

        lines.append("To preview changes (dry-run, default):")
        lines.append("  cybersec --cmd \"/health fix pyflink\"")
        lines.append("")
        lines.append("To apply these changes:")
        lines.append("  cybersec --cmd \"/health fix pyflink --apply\"")

    # Status summary
    status = data.get("status", {})
    if status:
        lines.append("")
        if status.get("healthy"):
            lines.append("✓ PyFlink environment is healthy")
        else:
            lines.append(f"Status: {status.get('critical_issues', 0)} critical, {status.get('warning_issues', 0)} warnings")

    return "\n".join(lines)


def _format_diagnosis(data: dict, failure_mode_id: str) -> str:
    """Format diagnosis for human display."""
    lines = []
    lines.append(f"Diagnosis: {failure_mode_id}")
    lines.append("=" * 40)

    fm = data.get("failure_mode", {})
    lines.append("")
    lines.append(f"Failure Mode: {fm.get('name', 'unknown')}")
    lines.append(f"Symptom: {fm.get('symptom', 'unknown')}")
    lines.append(f"Category: {fm.get('category', 'unknown')}")

    rpn = data.get("rpn", {})
    lines.append("")
    lines.append("Risk Priority Number:")
    lines.append(f"  Severity: {rpn.get('severity', 'N/A')}")
    lines.append(f"  Occurrence: {rpn.get('occurrence', 'N/A')}")
    lines.append(f"  Detection: {rpn.get('detection', 'N/A')}")
    lines.append(f"  RPN Score: {rpn.get('rpn', 'N/A')}")
    lines.append(f"  Tier: {rpn.get('tier', 'N/A')}")

    check_result = data.get("check_result", {})
    status = check_result.get("status", "unknown")
    lines.append("")
    lines.append(f"Current Status: {status.upper()}")
    lines.append(f"  {check_result.get('message', 'No message')}")

    remediation = data.get("remediation_steps", [])
    if remediation:
        lines.append("")
        lines.append("Remediation Steps:")
        for i, step in enumerate(remediation, 1):
            lines.append(f"  {i}. {step}")

    return "\n".join(lines)


def _format_pyflink_fixes(fix_results: list, dry_run: bool) -> str:
    """Format PyFlink fix results for human display."""
    lines = []

    if dry_run:
        lines.append("PyFlink Fix (DRY RUN - Preview Only)")
        lines.append("=" * 40)
        lines.append("The following changes would be made:")
    else:
        lines.append("PyFlink Fix (Applying Changes)")
        lines.append("=" * 40)

    lines.append("")

    for fix in fix_results:
        fm_id = fix.get("failure_mode_id", "?")
        action = fix.get("action", "unknown")
        success = fix.get("success", False)
        message = fix.get("message", "")

        if dry_run:
            # Preview mode - show what would happen
            if action == "install_package":
                lines.append(f"  + [{fm_id}] Would install: {fix.get('package', 'unknown')}")
                lines.append(f"      Command: {fix.get('command', '')}")

            elif action == "update_flink_config":
                lines.append(f"  ~ [{fm_id}] Would update config.yaml")
                lines.append(f"      Config: {fix.get('config_file', 'unknown')}")
                if fix.get("lines_to_add"):
                    lines.append("      Add:")
                    for line in fix["lines_to_add"]:
                        lines.append(f"        + {line}")

            elif action == "manual_required":
                lines.append(f"  ! [{fm_id}] Manual action required")
                lines.append(f"      {message}")

            elif action == "review_required":
                lines.append(f"  ? [{fm_id}] Review recommended")
                lines.append(f"      {fix.get('details', message)}")

        else:
            # Execute mode - show results
            icon = "✓" if success else "✗"
            lines.append(f"{icon} [{fm_id}] {message}")

            if action == "update_flink_config":
                if fix.get("config_file"):
                    lines.append(f"    Config: {fix['config_file']}")
                if fix.get("backup"):
                    lines.append(f"    Backup: {fix['backup']}")
                if fix.get("restart_required"):
                    lines.append("    ⚠ Restart required:")
                    lines.append(f"      {fix.get('restart_command', '$FLINK_HOME/bin/stop-cluster.sh && $FLINK_HOME/bin/start-cluster.sh')}")

            elif action == "install_package":
                if fix.get("command"):
                    lines.append(f"    Command: {fix['command']}")
                if fix.get("error"):
                    lines.append(f"    Error: {fix['error']}")

            elif action == "manual_required":
                lines.append(f"    Details: {fix.get('details', '')}")

            elif action == "review_required":
                lines.append(f"    Details: {fix.get('details', '')}")

        lines.append("")

    # Summary
    success_count = sum(1 for f in fix_results if f.get("success"))
    total = len(fix_results)

    if dry_run:
        lines.append(f"Plan: {total} fix(es) would be applied")
        lines.append("")
        lines.append("To apply these changes:")
        lines.append("  cybersec --cmd \"/health fix pyflink\"")
    else:
        lines.append(f"Applied {success_count}/{total} fix(es)")
        if any(f.get("restart_required") for f in fix_results):
            lines.append("")
            lines.append("⚠ Flink cluster restart required to apply changes:")
            lines.append("  devenv tasks run restart:clean")

    return "\n".join(lines)


def _format_fix(data: dict, failure_mode_id: str) -> str:
    """Format fix result for human display."""
    lines = []
    lines.append(f"Fix: {failure_mode_id}")
    lines.append("=" * 40)

    lines.append("")
    lines.append(f"Escalation Tier: {data.get('tier', 'N/A')} (RPN: {data.get('tier_value', 'N/A')})")

    if data.get("requires_approval"):
        lines.append("⚠ This issue requires manual intervention.")
    elif data.get("dry_run"):
        lines.append("ℹ DRY RUN - showing what would be done")
    else:
        lines.append("✓ Executing remediation...")

    instructions = data.get("instructions", [])
    if instructions:
        lines.append("")
        lines.append("Remediation Instructions:")
        for i, step in enumerate(instructions, 1):
            lines.append(f"  {i}. {step}")

    return "\n".join(lines)


# === RETE-based explain and goal commands ===

async def cmd_health_explain(cmd: ParsedCommand) -> CommandResult:
    """Explain the reasoning behind a health goal's status.

    Uses backward chaining to show WHY a goal is proven, disproven, or unknown.
    Provides a step-by-step reasoning trace for explainability.

    Args:
        <goal_id>  Goal to explain (e.g., pyflink_ready, infrastructure_healthy)

    Options:
        --tree      Show proof tree visualization
        --gaps      Show gap analysis (what's missing)
        --what-if   Hypothetical: assume these checks pass (comma-separated)
        --json, -j  Output as JSON
    """
    from ..health.rete_runner import get_rete_runner
    from ..bootstrap import BootstrapService
    from ..health.models import HealthContext

    if not cmd.args:
        # List available goals
        runner = get_rete_runner()
        goals = runner.list_goals()

        lines = ["Available diagnostic goals:", ""]
        for g in goals:
            status_icon = {"proven": "✓", "disproven": "✗", "unknown": "?"}.get(g["status"], "?")
            lines.append(f"  {status_icon} {g['goal_id']}")
            lines.append(f"      {g['description']}")
            lines.append("")

        lines.append("Usage: /health explain <goal_id>")
        lines.append("Example: /health explain pyflink_ready")

        return CommandResult(
            success=True,
            data={"goals": goals},
            formatted="\n".join(lines),
        )

    goal_id = cmd.args[0].lower()

    # Initialize context and runner
    service = BootstrapService()
    config = service.get_config()

    ctx = HealthContext(
        config=config,
        flink_url=config.flink_url or "http://localhost:8081",
        minio_endpoint=config.minio_endpoint or "http://localhost:9010",
        polaris_url=config.polaris_api_url or "http://localhost:8181",
        postgres_port=config.postgres_port or 5438,
        browser_port=config.iceberg_browser_port or 5050,
        nifi_url=config.nifi_url or "http://localhost:8450",
        nifi_otlp_port=config.nifi_otlp_port or 4319,
    )

    runner = get_rete_runner()

    # Check infrastructure first to populate facts
    await runner._check_infrastructure(ctx)

    # Handle --what-if option (parser normalizes to what_if)
    what_if_checks = cmd.options.get("what_if") or cmd.options.get("what-if", "")
    if what_if_checks:
        check_ids = [c.strip().upper() for c in what_if_checks.split(",")]
        explanation = runner.what_if(goal_id, check_ids)
        data = {
            "goal_id": goal_id,
            "mode": "what_if",
            "hypothetical_checks": check_ids,
            "conclusion": explanation.conclusion.value,
            "summary": explanation.summary,
            "steps": [{"step": s.step_number, "action": s.action, "description": s.description, "result": s.result} for s in explanation.steps],
        }
        formatted = _format_explanation(explanation, what_if=True, check_ids=check_ids)
        return CommandResult(success=True, data=data, formatted=formatted)

    # Handle --gaps option
    if cmd.options.get("gaps"):
        gaps = runner.analyze_gaps(goal_id)
        data = gaps.to_dict()
        formatted = _format_gaps(gaps)
        return CommandResult(success=True, data=data, formatted=formatted)

    # Handle --tree option
    if cmd.options.get("tree"):
        tree = runner.get_proof_tree(goal_id)
        data = tree.to_dict()
        formatted = _format_proof_tree(tree)
        return CommandResult(success=True, data=data, formatted=formatted)

    # Default: show explanation
    explanation = runner.explain_goal(goal_id)
    data = {
        "goal_id": goal_id,
        "conclusion": explanation.conclusion.value,
        "summary": explanation.summary,
        "steps": [{"step": s.step_number, "action": s.action, "description": s.description, "result": s.result} for s in explanation.steps],
    }

    # Also include suggestion for next action
    suggestion = runner.suggest_next_check(goal_id)
    if suggestion:
        data["suggested_next"] = suggestion

    formatted = _format_explanation(explanation, suggestion=suggestion)

    return CommandResult(
        success=True,
        data=data,
        formatted=formatted,
    )


async def cmd_health_objective(cmd: ParsedCommand) -> CommandResult:
    """Run checks toward a specific diagnostic objective.

    Uses RETE inference to run only the checks needed to prove/disprove the objective.
    Skips checks whose dependencies aren't met.

    Args:
        <objective_id>  Objective to work toward (e.g., pyflink_ready, e2e_ready)

    Options:
        --max-checks <n>  Maximum checks to run (default: 20)
        --json, -j        Output as JSON
    """
    from ..health.rete_runner import get_rete_runner, ReteHealthResult
    from ..bootstrap import BootstrapService
    from ..health.models import HealthContext

    if not cmd.args:
        return CommandResult(
            success=False,
            error="Missing required argument: objective_id (e.g., pyflink_ready, e2e_ready)",
        )

    goal_id = cmd.args[0].lower()  # internally still called goal_id
    max_checks = int(cmd.options.get("max-checks", cmd.options.get("max_checks", 20)))

    service = BootstrapService()
    config = service.get_config()

    ctx = HealthContext(
        config=config,
        flink_url=config.flink_url or "http://localhost:8081",
        minio_endpoint=config.minio_endpoint or "http://localhost:9010",
        polaris_url=config.polaris_api_url or "http://localhost:8181",
        postgres_port=config.postgres_port or 5438,
        browser_port=config.iceberg_browser_port or 5050,
        nifi_url=config.nifi_url or "http://localhost:8450",
        nifi_otlp_port=config.nifi_otlp_port or 4319,
    )

    runner = get_rete_runner()
    result = await runner.run_for_goal(goal_id, ctx, max_checks=max_checks)

    data = {
        "goal_id": result.goal_id,
        "status": result.status.value,
        "checks_run": result.checks_run,
        "checks_skipped": result.checks_skipped,
        "skip_reasons": result.skip_reasons,
        "issues": result.issues,
    }

    formatted = _format_goal_result(result)

    return CommandResult(
        success=result.status.value != "disproven",
        data=data,
        formatted=formatted,
    )


def _format_explanation(explanation, what_if=False, check_ids=None, suggestion=None) -> str:
    """Format explanation for human display."""
    lines = []

    if what_if:
        lines.append("What-If Analysis")
        lines.append("=" * 50)
        lines.append(f"Hypothetical: Assuming checks pass: {', '.join(check_ids or [])}")
        lines.append("")

    lines.append(f"Goal: {explanation.goal_id}")
    lines.append("")

    status_icon = {"proven": "✓", "disproven": "✗", "unknown": "?"}.get(explanation.conclusion.value, "?")
    lines.append(f"Conclusion: {status_icon} {explanation.conclusion.value.upper()}")
    lines.append(f"Summary: {explanation.summary}")
    lines.append("")

    lines.append("Reasoning Trace:")
    lines.append("-" * 40)

    for step in explanation.steps:
        action_icon = {
            "start": "▶",
            "check": "◆",
            "conclude": "■",
            "infer": "→",
        }.get(step.action, "•")

        if step.result:
            result_icon = ""
            if "SATISFIED" in step.result:
                result_icon = "✓"
            elif "FAILED" in step.result:
                result_icon = "✗"
            elif "UNKNOWN" in step.result:
                result_icon = "?"
            lines.append(f"  {step.step_number}. {action_icon} {step.description}")
            lines.append(f"       {result_icon} {step.result}")
        else:
            lines.append(f"  {step.step_number}. {action_icon} {step.description}")

    if suggestion and suggestion.get("check_id"):
        lines.append("")
        lines.append("Suggested Next Action:")
        lines.append(f"  Run check: {suggestion['check_id']}")
        if suggestion.get("name"):
            lines.append(f"  ({suggestion['name']})")
        lines.append(f"  Cost: {suggestion.get('cost', 'N/A')}")
        lines.append(f"  Remaining unknowns: {suggestion.get('remaining_unknowns', 'N/A')}")

    return "\n".join(lines)


def _format_gaps(gaps) -> str:
    """Format gap analysis for human display."""
    lines = []
    lines.append(f"Gap Analysis: {gaps.goal_id}")
    lines.append("=" * 50)
    lines.append("")

    status_icon = {"proven": "✓", "disproven": "✗", "unknown": "?"}.get(gaps.status.value, "?")
    lines.append(f"Status: {status_icon} {gaps.status.value.upper()}")
    lines.append("")

    if gaps.missing_facts:
        lines.append(f"Missing Facts ({len(gaps.missing_facts)}):")
        for mf in gaps.missing_facts:
            lines.append(f"  • {mf}")
        lines.append("")

    if gaps.blocking_conditions:
        lines.append("Blocking Conditions:")
        for bc in gaps.blocking_conditions:
            reason = bc.get("reason", "")
            if bc.get("actual") is not None:
                lines.append(f"  ✗ {bc['pattern']} expected {bc['expected']}, got {bc['actual']}")
            else:
                lines.append(f"  ? {bc['pattern']} - {reason}")
        lines.append("")

    if gaps.acquisition_plan:
        lines.append("Acquisition Plan (by cost):")
        for i, acq in enumerate(gaps.acquisition_plan, 1):
            lines.append(f"  {i}. {acq['fact_pattern']} (cost: {acq['cost']})")
            lines.append(f"       {acq['action']}")

    return "\n".join(lines)


def _format_proof_tree(tree, indent=0) -> str:
    """Format proof tree for human display."""
    lines = []

    if indent == 0:
        lines.append("Proof Tree")
        lines.append("=" * 50)
        lines.append("")

    prefix = "  " * indent
    status_icon = {"proven": "✓", "disproven": "✗", "unknown": "?"}.get(tree.status.value, "?")

    if tree.node_type == "goal":
        lines.append(f"{prefix}{status_icon} {tree.node_id} ({tree.status.value})")
        lines.append(f"{prefix}   {tree.description}")
    else:
        actual = f" = {tree.actual_value}" if tree.actual_value is not None else ""
        lines.append(f"{prefix}├─ {status_icon} {tree.description}{actual}")

    for child in tree.children:
        lines.append(_format_proof_tree(child, indent + 1))

    return "\n".join(lines)


def _format_goal_result(result) -> str:
    """Format goal-directed health check result."""
    lines = []
    lines.append(f"Goal: {result.goal_id}")
    lines.append("=" * 50)
    lines.append("")

    status_icon = {"proven": "✓", "disproven": "✗", "unknown": "?"}.get(result.status.value, "?")
    lines.append(f"Status: {status_icon} {result.status.value.upper()}")
    lines.append("")

    if result.checks_run:
        lines.append(f"Checks Run ({len(result.checks_run)}):")
        for check in result.checks_run:
            lines.append(f"  ✓ {check}")
        lines.append("")

    if result.checks_skipped:
        lines.append(f"Checks Skipped ({len(result.checks_skipped)}):")
        for check in result.checks_skipped:
            reason = result.skip_reasons.get(check, "Dependency not met")
            lines.append(f"  ⏭ {check}: {reason}")
        lines.append("")

    if result.issues:
        lines.append("Issues Detected:")
        for issue in result.issues:
            lines.append(f"  ✗ [{issue['failure_mode_id']}] {issue.get('name', 'Unknown')}")
            lines.append(f"      {issue.get('message', '')}")
            lines.append(f"      RPN: {issue.get('rpn', 'N/A')}")
        lines.append("")

    if result.explanation:
        lines.append("Summary:")
        lines.append(f"  {result.explanation.summary}")

    return "\n".join(lines)


# === Register commands ===

def register_health_commands():
    """Register all health commands."""
    register_command(
        "health",
        cmd_health,
        description="Run FMEA-based health diagnostics",
        args=[
            {"name": "category", "required": False, "description": "Category (flink, rest-catalog, postgres, system, data)"},
        ],
        options=[
            {"name": "quick", "short": "q", "description": "Run only critical checks"},
            {"name": "json", "short": "j", "description": "Output as JSON"},
        ],
        examples=[
            "/health",
            "/health flink",
            "/health --quick",
        ],
    )

    # Note: /health pyflink removed - use /health flink instead (pyflink is alias)

    register_command(
        "health.diagnose",
        cmd_health_diagnose,
        description="Diagnose a specific failure mode",
        args=[
            {"name": "failure_mode_id", "required": True, "description": "Failure mode ID (e.g., FLINK_001)"},
        ],
        options=[
            {"name": "json", "short": "j", "description": "Output as JSON"},
        ],
        examples=[
            "/health diagnose FLINK_001",
            "/health diagnose ICE_002 --json",
        ],
    )

    register_command(
        "health.fix",
        cmd_health_fix,
        description="Fix detected health issues",
        args=[
            {"name": "target", "required": False, "description": "Category (flink, pyflink, rest-catalog) or failure mode ID"},
        ],
        options=[
            {"name": "apply", "description": "Apply fixes (default: dry-run preview)"},
            {"name": "json", "short": "j", "description": "Output as JSON"},
        ],
        examples=[
            "/health fix",
            "/health fix --apply",
            "/health fix pyflink",
            "/health fix system --apply",
        ],
    )

    register_command(
        "health.explain",
        cmd_health_explain,
        description="Explain reasoning behind a health objective's status",
        args=[
            {"name": "objective_id", "required": False, "description": "Objective to explain (omit to list all)"},
        ],
        options=[
            {"name": "tree", "description": "Show proof tree visualization"},
            {"name": "gaps", "description": "Show gap analysis (what's missing)"},
            {"name": "what-if", "description": "Hypothetical: assume these checks pass (comma-separated)"},
            {"name": "json", "short": "j", "description": "Output as JSON"},
        ],
        examples=[
            "/health explain",
            "/health explain pyflink_ready",
            "/health explain e2e_ready --tree",
            "/health explain pyflink_ready --gaps",
            "/health explain pyflink_ready --what-if PYFLINK_011,PYFLINK_012",
        ],
    )

    register_command(
        "health.objective",
        cmd_health_objective,
        description="Run checks toward a specific diagnostic objective",
        args=[
            {"name": "objective_id", "required": True, "description": "Objective to work toward (e.g., pyflink_ready)"},
        ],
        options=[
            {"name": "max-checks", "description": "Maximum checks to run (default: 20)"},
            {"name": "json", "short": "j", "description": "Output as JSON"},
        ],
        examples=[
            "/health objective pyflink_ready",
            "/health objective e2e_ready --max-checks 10",
        ],
    )
