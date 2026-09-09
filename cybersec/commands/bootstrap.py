"""Bootstrap commands for unified command system.

Commands:
    /bootstrap status   - Check service health
    /bootstrap info     - Show configuration
    /bootstrap verify   - Verify environment
    /bootstrap assess   - Quick assessment
    /bootstrap run      - Run bootstrap process
    /bootstrap settings - View/modify settings
"""

from .parser import ParsedCommand, CommandResult
from .registry import register_command


async def cmd_bootstrap_status(cmd: ParsedCommand) -> CommandResult:
    """Check current status of all services.

    Performs health checks on PostgreSQL, Polaris, Flink, MinIO,
    and Iceberg Browser.

    Options:
        --json, -j  Output as JSON
    """
    from ..bootstrap import BootstrapService

    service = BootstrapService()
    results = await service.check_all_services()

    all_healthy = all(r.get("healthy", False) for r in results)

    data = {
        "all_healthy": all_healthy,
        "services": results,
        "recommendation": None if all_healthy else "Run /bootstrap run to set up the environment",
    }

    formatted = _format_status(results, all_healthy)

    return CommandResult(
        success=True,
        data=data,
        formatted=formatted,
    )


async def cmd_bootstrap_info(cmd: ParsedCommand) -> CommandResult:
    """Show bootstrap system information and configuration.

    Options:
        --json, -j  Output as JSON
    """
    from ..bootstrap import BootstrapService

    service = BootstrapService()
    config = service.get_config()

    data = {
        "config_file": str(service.settings.config_path),
        "config_exists": service.settings.exists(),
        "bootstrap_completed": config.completed,
        "last_run": config.last_run,
        "paths": {
            "flink_home": str(config.get_flink_home()) if config.get_flink_home() else None,
            "flink_state": str(config.get_flink_state_dir()),
            "minio_data": str(config.get_minio_data_dir()),
            "log_dir": str(config.get_log_dir()),
        },
        "services": {
            "postgres": {"host": config.postgres_host, "port": config.postgres_port},
            "polaris": {"api_url": config.polaris_api_url, "admin_url": config.polaris_admin_url},
            "flink": {"url": config.flink_url},
            "minio": {"endpoint": config.minio_endpoint, "console": config.minio_console},
            "iceberg_browser": {"port": config.iceberg_browser_port},
            "nifi": {"url": config.nifi_url, "otlp_port": config.nifi_otlp_port},
        },
        "catalog": {
            "name": config.catalog_name,
            "warehouse": config.catalog_warehouse,
        },
    }

    formatted = _format_info(config, service.settings)

    return CommandResult(
        success=True,
        data=data,
        formatted=formatted,
    )


async def cmd_bootstrap_verify(cmd: ParsedCommand) -> CommandResult:
    """Verify the bootstrap configuration is correct.

    Checks required tools, Flink installation, service connectivity,
    and configuration files.

    Options:
        --json, -j  Output as JSON
    """
    from ..bootstrap import BootstrapService

    service = BootstrapService()
    result = await service.verify()

    all_passed = result.get("all_passed", False)
    checks = result.get("checks", [])

    data = {
        "all_passed": all_passed,
        "checks": checks,
        "summary": {
            "passed": sum(1 for c in checks if c["passed"]),
            "failed": sum(1 for c in checks if not c["passed"]),
            "total": len(checks),
        },
    }

    formatted = _format_verify(checks, all_passed)

    return CommandResult(
        success=all_passed,
        data=data,
        formatted=formatted,
    )


async def cmd_bootstrap_assess(cmd: ParsedCommand) -> CommandResult:
    """Quick environment assessment.

    Fast check of config file, required tools, Flink installation,
    and whether bootstrap is needed.

    Options:
        --json, -j  Output as JSON
    """
    from ..bootstrap import BootstrapService

    service = BootstrapService()
    result = await service.assess()

    data = {
        "ready": result.get("ready", False),
        "needs_bootstrap": result.get("needs_bootstrap", True),
        "config_exists": result.get("config_exists", False),
        "flink_installed": result.get("flink_installed", False),
        "flink_home": result.get("flink_home"),
        "nifi_installed": result.get("nifi_installed", False),
        "nifi_home": result.get("nifi_home"),
        "tools": result.get("tools", {}),
        "recommendation": (
            "Environment is ready" if result.get("ready")
            else "Run /bootstrap run to set up the environment"
        ),
    }

    formatted = _format_assess(result)

    return CommandResult(
        success=result.get("ready", False),
        data=data,
        formatted=formatted,
    )


async def cmd_bootstrap_run(cmd: ParsedCommand) -> CommandResult:
    """Run the bootstrap process to set up the environment.

    Options:
        --skip-flink         Skip Flink setup
        --flink-path <path>  Path to existing Flink installation
        --skip-nifi          Skip NiFi setup
        --nifi-path <path>   Path to existing NiFi installation
        --dry-run            Show what would be done
        --json, -j           Output as JSON
    """
    from ..bootstrap import BootstrapService, EventType
    from ..bootstrap.events import EventCollector

    service = BootstrapService()
    collector = EventCollector()

    skip_flink = cmd.options.get("skip_flink", False)
    flink_path = cmd.options.get("flink_path")
    skip_nifi = cmd.options.get("skip_nifi", False)
    nifi_path = cmd.options.get("nifi_path")
    dry_run = cmd.options.get("dry_run", False)

    events = []
    final_status = None
    prompt_needed = None

    async def prompt_handler(event):
        nonlocal prompt_needed
        if event.event_type == EventType.PROMPT_REQUIRED:
            prompt_needed = {
                "message": event.message,
                "options": [
                    {"key": o.key, "label": o.label, "description": o.description}
                    for o in event.prompt_options
                ],
            }
            return "3"  # Skip
        return None

    try:
        async for event in service.run(
            skip_flink=skip_flink,
            flink_path=flink_path,
            skip_nifi=skip_nifi,
            nifi_path=nifi_path,
            prompt_handler=prompt_handler,
            dry_run=dry_run,
        ):
            collector.collect(event)
            events.append({
                "type": event.event_type.value,
                "task_id": event.task_id,
                "message": event.message,
                "progress": event.progress,
            })

            if event.event_type == EventType.BOOTSTRAP_COMPLETED:
                final_status = "success"
            elif event.event_type == EventType.BOOTSTRAP_FAILED:
                final_status = "failed"

    except Exception as e:
        final_status = "error"
        events.append({"type": "error", "message": str(e)})

    data = {
        "status": final_status or "unknown",
        "events": events,
        "summary": collector.to_summary(),
    }

    if prompt_needed:
        data["prompt_needed"] = prompt_needed
        data["hint"] = "Flink setup requires input. Run with --flink-path or --skip-flink"

    formatted = _format_run(events, final_status, dry_run)

    return CommandResult(
        success=final_status == "success",
        data=data,
        formatted=formatted,
    )


async def cmd_bootstrap_settings(cmd: ParsedCommand) -> CommandResult:
    """View or modify bootstrap settings.

    Options:
        --set <key>=<value>  Set a specific value
        --reset              Reset to defaults
        --json, -j           Output as JSON
    """
    from ..bootstrap import BootstrapService, BootstrapConfig

    service = BootstrapService()

    if cmd.options.get("reset"):
        config = BootstrapConfig()
        service.settings.save(config)
        return CommandResult(
            success=True,
            data={"action": "reset", "message": "Settings reset to defaults"},
            message="Settings reset to defaults",
            formatted="✓ Settings reset to defaults",
        )

    set_value = cmd.options.get("set")
    if set_value:
        if "=" not in set_value:
            return CommandResult(
                success=False,
                error=f"Invalid format: {set_value} (expected key=value)",
            )
        key, value = set_value.split("=", 1)
        service.update_config(**{key: value})
        return CommandResult(
            success=True,
            data={"action": "updated", "key": key, "value": value},
            message=f"Updated {key} = {value}",
            formatted=f"✓ Updated {key} = {value}",
        )

    # Show settings
    config_dict = service.get_config_dict()
    formatted = _format_settings(config_dict)

    return CommandResult(
        success=True,
        data={"action": "show", "config": config_dict},
        formatted=formatted,
    )


# === Formatting helpers ===

def _format_status(results: list, all_healthy: bool) -> str:
    """Format status for human display."""
    lines = []
    lines.append("Service Status")
    lines.append("=" * 40)
    lines.append("")

    for result in results:
        is_healthy = result.get("healthy", False)
        icon = "✓" if is_healthy else "✗"
        status = "Healthy" if is_healthy else "Unhealthy"
        details = result.get("error", "") or result.get("message", "")
        if not details and is_healthy:
            details = "OK"

        lines.append(f"{icon} {result['service']:<20} {status:<10} {details}")

    lines.append("")
    if all_healthy:
        lines.append("✓ All services are healthy!")
    else:
        lines.append("⚠ Some services are not healthy. Run /bootstrap run to fix.")

    return "\n".join(lines)


def _format_info(config, settings) -> str:
    """Format info for human display."""
    lines = []
    lines.append("Cyberphy Bootstrap Configuration")
    lines.append("=" * 40)
    lines.append("")

    config_path = settings.config_path
    if config_path.exists():
        lines.append(f"Config file: {config_path}")
    else:
        lines.append(f"Config file: {config_path} (not created yet)")

    status = "Completed" if config.completed else "Not completed"
    lines.append(f"Bootstrap status: {status}")
    if config.last_run:
        lines.append(f"Last run: {config.last_run}")

    lines.append("")
    lines.append("Paths:")
    flink_home = config.get_flink_home()
    lines.append(f"  Flink Home: {flink_home if flink_home else 'Not configured'}")
    lines.append(f"  Flink State: {config.get_flink_state_dir()}")
    lines.append(f"  MinIO Data: {config.get_minio_data_dir()}")
    lines.append(f"  Log Dir: {config.get_log_dir()}")

    lines.append("")
    lines.append("Service Endpoints:")
    lines.append(f"  PostgreSQL: {config.postgres_host}:{config.postgres_port}")
    lines.append(f"  Polaris API: {config.polaris_api_url}")
    lines.append(f"  Flink: {config.flink_url}")
    lines.append(f"  MinIO: {config.minio_endpoint}")
    lines.append(f"  Iceberg Browser: http://localhost:{config.iceberg_browser_port}")
    lines.append(f"  NiFi: {config.nifi_url}")

    lines.append("")
    lines.append("Catalog:")
    lines.append(f"  Name: {config.catalog_name}")
    lines.append(f"  Warehouse: {config.catalog_warehouse}")

    return "\n".join(lines)


def _format_verify(checks: list, all_passed: bool) -> str:
    """Format verify for human display."""
    lines = []
    lines.append("Bootstrap Verification")
    lines.append("=" * 40)
    lines.append("")

    for check in checks:
        icon = "✓" if check["passed"] else "✗"
        lines.append(f"{icon} {check['name']}: {check.get('message', '')}")

    lines.append("")
    if all_passed:
        lines.append("✓ All checks passed! Environment is ready.")
    else:
        lines.append("⚠ Some checks failed. Run /bootstrap run to fix.")

    return "\n".join(lines)


def _format_assess(result: dict) -> str:
    """Format assess for human display."""
    lines = []
    lines.append("Environment Assessment")
    lines.append("=" * 40)
    lines.append("")

    ready = result.get("ready", False)
    lines.append(f"Ready: {'✓ Yes' if ready else '✗ No'}")
    lines.append(f"Config exists: {'✓' if result.get('config_exists') else '✗'}")
    lines.append(f"Flink installed: {'✓' if result.get('flink_installed') else '✗'}")
    if result.get("flink_home"):
        lines.append(f"  Path: {result['flink_home']}")
    lines.append(f"NiFi installed: {'✓' if result.get('nifi_installed') else '✗'}")

    tools = result.get("tools", {})
    if tools:
        lines.append("")
        lines.append("Required tools:")
        for tool, available in tools.items():
            icon = "✓" if available else "✗"
            lines.append(f"  {icon} {tool}")

    lines.append("")
    if ready:
        lines.append("✓ Environment is ready")
    else:
        lines.append("⚠ Run /bootstrap run to set up the environment")

    return "\n".join(lines)


def _format_run(events: list, status: str, dry_run: bool) -> str:
    """Format run for human display."""
    lines = []
    if dry_run:
        lines.append("Bootstrap (DRY RUN)")
    else:
        lines.append("Bootstrap")
    lines.append("=" * 40)
    lines.append("")

    for event in events:
        event_type = event.get("type", "")
        message = event.get("message", event.get("task_id", ""))

        if event_type == "task_completed":
            lines.append(f"✓ {message}")
        elif event_type == "task_failed":
            lines.append(f"✗ {message}")
        elif event_type == "task_skipped":
            lines.append(f"- {message}")
        elif event_type == "log_info":
            lines.append(f"  {message}")
        elif event_type == "log_warn":
            lines.append(f"⚠ {message}")
        elif event_type == "log_error":
            lines.append(f"✗ {message}")
        elif event_type == "bootstrap_completed":
            lines.append("")
            lines.append(f"✓ {message}")
        elif event_type == "bootstrap_failed":
            lines.append("")
            lines.append(f"✗ {message}")

    return "\n".join(lines)


def _format_settings(config_dict: dict) -> str:
    """Format settings for human display."""
    lines = []
    lines.append("Current Settings")
    lines.append("=" * 40)

    def format_dict(d: dict, indent: int = 0):
        prefix = "  " * indent
        for key, value in d.items():
            if isinstance(value, dict):
                lines.append(f"{prefix}{key}:")
                format_dict(value, indent + 1)
            else:
                lines.append(f"{prefix}{key}: {value}")

    format_dict(config_dict)
    return "\n".join(lines)


# === Register commands ===

def register_bootstrap_commands():
    """Register all bootstrap commands."""
    register_command(
        "bootstrap.status",
        cmd_bootstrap_status,
        description="Check service health",
        options=[
            {"name": "json", "short": "j", "description": "Output as JSON"},
        ],
        examples=["/bootstrap status", "/bootstrap status --json"],
    )

    register_command(
        "bootstrap.info",
        cmd_bootstrap_info,
        description="Show configuration",
        options=[
            {"name": "json", "short": "j", "description": "Output as JSON"},
        ],
        examples=["/bootstrap info"],
    )

    register_command(
        "bootstrap.verify",
        cmd_bootstrap_verify,
        description="Verify environment",
        options=[
            {"name": "json", "short": "j", "description": "Output as JSON"},
        ],
        examples=["/bootstrap verify"],
    )

    register_command(
        "bootstrap.assess",
        cmd_bootstrap_assess,
        description="Quick assessment",
        options=[
            {"name": "json", "short": "j", "description": "Output as JSON"},
        ],
        examples=["/bootstrap assess"],
    )

    register_command(
        "bootstrap.run",
        cmd_bootstrap_run,
        description="Run bootstrap process",
        options=[
            {"name": "skip_flink", "description": "Skip Flink setup"},
            {"name": "flink_path", "short": "f", "description": "Path to existing Flink"},
            {"name": "skip_nifi", "description": "Skip NiFi setup"},
            {"name": "nifi_path", "description": "Path to existing NiFi"},
            {"name": "dry_run", "description": "Show what would be done"},
            {"name": "json", "short": "j", "description": "Output as JSON"},
        ],
        examples=[
            "/bootstrap run",
            "/bootstrap run --flink-path ~/flink-1.20.1",
            "/bootstrap run --skip-flink --dry-run",
        ],
    )

    register_command(
        "bootstrap.settings",
        cmd_bootstrap_settings,
        description="View/modify settings",
        options=[
            {"name": "set", "description": "Set a value (key=value)"},
            {"name": "reset", "description": "Reset to defaults"},
            {"name": "json", "short": "j", "description": "Output as JSON"},
        ],
        examples=[
            "/bootstrap settings",
            "/bootstrap settings --set flink_home=/path/to/flink",
            "/bootstrap settings --reset",
        ],
    )
