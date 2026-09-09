"""MCP Server implementation using fastmcp.

Provides a single `cmd` tool that delegates to the unified command system,
ensuring full parity between MCP, CLI, and TUI interfaces.

Usage:
    cmd("/health pyflink")
    cmd("/bootstrap status --json")
    cmd("/health diagnose FLINK_001")

All commands follow the same syntax as CLI `--cmd` and TUI slash commands.
"""

from typing import Optional

from fastmcp import FastMCP

# Create MCP server
mcp = FastMCP(
    name="cyberphy",
    instructions="""Cyberphy Toolkit MCP Server

Execute commands using the `cmd` tool with slash-prefixed commands:

    cmd("/health")                  - Run FMEA health diagnostics
    cmd("/health flink")            - Check flink category
    cmd("/health fix")              - Dry-run all detected issues
    cmd("/health fix --apply")      - Apply all fixes
    cmd("/health fix flink")        - Dry-run flink category
    cmd("/health fix flink --apply") - Fix flink category
    cmd("/health diagnose FLINK_001") - Diagnose specific failure
    cmd("/bootstrap status")        - Check service health
    cmd("/bootstrap info")          - Show configuration
    cmd("/bootstrap verify")        - Verify environment
    cmd("/bootstrap assess")        - Quick assessment
    cmd("/bootstrap run")           - Run bootstrap process
    cmd("/bootstrap settings")      - View/modify settings

Add --json to any command for structured output:
    cmd("/health flink --json")

Available command groups:
- /health - FMEA-based health diagnostics
- /bootstrap - Environment setup and configuration
- /policy - Conftest policy validation
- /aws - AWS developer identity and target configuration
- /k8s - Kubernetes target preparation
""",
)


@mcp.tool()
async def cmd(command: str) -> dict:
    """Execute a unified command.

    Commands use slash-prefix syntax identical to CLI and TUI:
        /health                     - FMEA health diagnostics
        /health flink               - Check flink category
        /health fix                 - Dry-run all detected issues
        /health fix --apply         - Apply all fixes
        /health fix flink           - Dry-run flink category
        /health fix flink --apply   - Fix flink category
        /health diagnose <id>       - Diagnose failure mode
        /bootstrap status           - Service health
        /bootstrap info             - Configuration
        /bootstrap verify           - Verify environment
        /bootstrap assess           - Quick assessment
        /bootstrap run              - Run bootstrap
        /bootstrap settings         - View/modify settings

    Args:
        command: The command to execute (e.g., "/health flink --json")

    Returns:
        Command result with:
        - success: Whether command succeeded
        - data: Structured command output
        - message: Human-readable message
        - error: Error message if failed

    Examples:
        cmd("/health")
        cmd("/health flink")
        cmd("/health fix flink")
        cmd("/health fix --apply")
        cmd("/bootstrap status --json")
        cmd("/health diagnose FLINK_001")
    """
    from ..commands.setup import init_commands
    from ..commands import dispatch, parse_command, CommandError

    # Initialize command registry
    init_commands()

    # Parse and dispatch
    try:
        parsed = parse_command(command)
    except CommandError as e:
        return {
            "success": False,
            "error": f"Parse error: {e.message}",
        }

    result = await dispatch(parsed)
    return result.to_dict()


@mcp.tool()
async def help(command: Optional[str] = None) -> dict:
    """Get help for available commands.

    Args:
        command: Optional command to get help for (e.g., "health", "bootstrap")

    Returns:
        Available commands and their descriptions
    """
    from ..commands.setup import init_commands
    from ..commands.registry import list_commands, get_subcommands

    init_commands()

    if command:
        # Get subcommands for specific command
        subcommands = get_subcommands(command)
        if subcommands:
            return {
                "command": command,
                "subcommands": [
                    {
                        "name": sub.name,
                        "description": sub.description,
                        "examples": sub.examples,
                    }
                    for sub in subcommands
                ],
            }
        else:
            return {
                "error": f"Unknown command: {command}",
                "available": [c.name for c in list_commands() if "." not in c.name],
            }

    # List all top-level commands
    commands = list_commands()
    top_level = [c for c in commands if "." not in c.name]

    return {
        "commands": [
            {
                "name": f"/{c.name}",
                "description": c.description,
            }
            for c in top_level
        ],
        "usage": 'Use cmd("/command subcommand --options") to execute',
    }


# ============================================================================
# MCP Resources
# ============================================================================


@mcp.resource("bootstrap://config")
async def get_config_resource() -> str:
    """Get current bootstrap configuration as JSON."""
    import json
    from ..bootstrap import BootstrapService

    service = BootstrapService()
    return json.dumps(service.get_config_dict(), indent=2)


@mcp.resource("bootstrap://state")
async def get_state_resource() -> str:
    """Get current bootstrap state as JSON."""
    import json
    from ..bootstrap import BootstrapService

    service = BootstrapService()
    return json.dumps(service.state.to_dict(), indent=2)


# ============================================================================
# Server Entry Point
# ============================================================================


def run_server():
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    run_server()
