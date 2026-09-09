"""Infrastructure health checks.

Checks for:
- INFRA_001: PostgreSQL down
- INFRA_002: MinIO unhealthy
- INFRA_003: Polaris degraded
- INFRA_004: macOS Shared Memory Exhaustion
- INFRA_005: AWS Credentials Invalid
"""

import os
import socket
import time
from pathlib import Path
from typing import Any

import httpx

from ..models import CheckResult, HealthContext
from ..catalog import INFRA_001, INFRA_002, INFRA_003, INFRA_004, INFRA_005


async def check_postgres(ctx: HealthContext) -> CheckResult:
    """INFRA_001: Check PostgreSQL connectivity.

    Simple TCP connection test to the database port.
    """
    if not ctx.devenv_running:
        return CheckResult.skipped("devenv not running")

    start = time.monotonic()

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        result = sock.connect_ex(('localhost', ctx.postgres_port))
        sock.close()

        duration = int((time.monotonic() - start) * 1000)

        if result == 0:
            result_obj = CheckResult.ok(f"PostgreSQL OK (port {ctx.postgres_port})")
            result_obj.duration_ms = duration
            return result_obj
        else:
            rpn = INFRA_001.calculate_rpn()
            return CheckResult.critical(
                f"PostgreSQL not reachable on port {ctx.postgres_port}",
                failure_mode_id="INFRA_001",
                rpn=rpn,
                remediation="Start PostgreSQL: devenv up postgres",
                port=ctx.postgres_port,
                duration_ms=duration,
            )

    except socket.timeout:
        duration = int((time.monotonic() - start) * 1000)
        rpn = INFRA_001.calculate_rpn()
        return CheckResult.critical(
            "PostgreSQL connection timeout",
            failure_mode_id="INFRA_001",
            rpn=rpn,
            remediation="Check PostgreSQL is running",
            duration_ms=duration,
        )
    except Exception as e:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.error(f"Failed to check PostgreSQL: {e}", duration_ms=duration)


async def check_minio(ctx: HealthContext) -> CheckResult:
    """INFRA_002: Check local S3 (RustFS) health.

    Queries RustFS ``/health``, with legacy MinIO ``/minio/health/live`` fallback.
    Fact/check id remains ``minio`` for compatibility with rete rules and
    ``local-s3`` category aliases.
    """
    if not ctx.devenv_running:
        return CheckResult.skipped("devenv not running")

    start = time.monotonic()
    base = ctx.minio_endpoint.rstrip("/")
    paths = ("/health", "/minio/health/live")

    try:
        async with httpx.AsyncClient() as client:
            last_status = None
            for path in paths:
                try:
                    resp = await client.get(f"{base}{path}", timeout=5.0)
                    last_status = resp.status_code
                    if resp.status_code == 200:
                        duration = int((time.monotonic() - start) * 1000)
                        result = CheckResult.ok(f"Local S3 (RustFS) OK ({path})")
                        result.duration_ms = duration
                        return result
                except httpx.ConnectError:
                    raise
                except Exception:
                    continue

            duration = int((time.monotonic() - start) * 1000)
            rpn = INFRA_002.calculate_rpn()
            return CheckResult.critical(
                f"Local S3 unhealthy: {last_status}",
                failure_mode_id="INFRA_002",
                rpn=rpn,
                remediation="Start RustFS: devenv up (services.rustfs)",
                status_code=last_status,
                duration_ms=duration,
            )

    except httpx.ConnectError:
        duration = int((time.monotonic() - start) * 1000)
        rpn = INFRA_002.calculate_rpn()
        return CheckResult.critical(
            "Local S3 (RustFS) not reachable",
            failure_mode_id="INFRA_002",
            rpn=rpn,
            remediation="Start RustFS: devenv up -d (services.rustfs on :9010)",
            duration_ms=duration,
        )
    except Exception as e:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.error(f"Failed to check local S3: {e}", duration_ms=duration)


async def check_polaris(ctx: HealthContext) -> CheckResult:
    """INFRA_003: Check Polaris health.

    Queries the Polaris admin health endpoint and measures latency.
    """
    if not ctx.devenv_running:
        return CheckResult.skipped("devenv not running")

    start = time.monotonic()

    # Polaris admin port is typically 8182
    admin_url = ctx.polaris_url.replace(":8181", ":8182")

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{admin_url}/q/health/ready",
                timeout=5.0
            )

            duration = int((time.monotonic() - start) * 1000)

            if resp.status_code == 200:
                # Check for slow response
                if duration > 2000:  # 2 seconds is slow
                    rpn = INFRA_003.calculate_rpn()
                    return CheckResult.warning(
                        f"Polaris slow: {duration}ms response",
                        failure_mode_id="INFRA_003",
                        rpn=rpn,
                        remediation="Restart Polaris or check resources",
                        response_ms=duration,
                        duration_ms=duration,
                    )

                result = CheckResult.ok(f"Polaris OK ({duration}ms)")
                result.duration_ms = duration
                return result
            else:
                rpn = INFRA_003.calculate_rpn()
                return CheckResult.warning(
                    f"Polaris degraded: {resp.status_code}",
                    failure_mode_id="INFRA_003",
                    rpn=rpn,
                    remediation="Restart Polaris: devenv tasks run restart:polaris",
                    status_code=resp.status_code,
                    duration_ms=duration,
                )

    except httpx.ConnectError:
        duration = int((time.monotonic() - start) * 1000)
        rpn = INFRA_003.calculate_rpn()
        return CheckResult.critical(
            "Polaris not reachable",
            failure_mode_id="INFRA_003",
            rpn=rpn,
            remediation="Start Polaris: devenv up polaris",
            duration_ms=duration,
        )
    except Exception as e:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.error(f"Failed to check Polaris: {e}", duration_ms=duration)


async def check_shared_memory(ctx: HealthContext) -> CheckResult:
    """INFRA_004: Check for orphaned shared memory segments.

    On macOS, orphaned IPC shared memory segments from previous devenv crashes
    can accumulate and exhaust system limits, preventing PostgreSQL from starting.

    Proactively checks for orphaned segments owned by current user via ipcs -m.
    """
    import platform
    import subprocess

    start = time.monotonic()

    # Only relevant on macOS (Linux handles this differently)
    if platform.system() != "Darwin":
        return CheckResult.skipped("Shared memory check only applies to macOS")

    try:
        # Check for orphaned shared memory segments
        result = subprocess.run(
            ["ipcs", "-m"],
            capture_output=True,
            text=True,
            timeout=5,
        )

        if result.returncode != 0:
            duration = int((time.monotonic() - start) * 1000)
            return CheckResult.error(f"ipcs command failed: {result.stderr}", duration_ms=duration)

        # Parse ipcs output - count segments owned by current user
        # Format: T ID KEY MODE OWNER GROUP (header + data lines)
        lines = result.stdout.strip().split('\n')
        current_user = os.environ.get("USER", "")

        orphan_count = 0
        for line in lines:
            parts = line.split()
            # Skip header lines and empty lines
            if len(parts) >= 5 and parts[0] == 'm':
                owner = parts[4] if len(parts) > 4 else ""
                if owner == current_user:
                    orphan_count += 1

        duration = int((time.monotonic() - start) * 1000)

        # Any segments from current user are likely orphaned (PostgreSQL cleans up on normal exit)
        if orphan_count > 0:
            rpn = INFRA_004.calculate_rpn()
            return CheckResult.warning(
                f"Found {orphan_count} orphaned shared memory segment(s)",
                failure_mode_id="INFRA_004",
                rpn=rpn,
                remediation="Run: /health fix --apply",
                segment_count=orphan_count,
                duration_ms=duration,
            )

        result_obj = CheckResult.ok("No orphaned shared memory segments")
        result_obj.duration_ms = duration
        return result_obj

    except subprocess.TimeoutExpired:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.error("ipcs command timed out", duration_ms=duration)
    except FileNotFoundError:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.skipped("ipcs command not found")
    except Exception as e:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.error(f"Failed to check shared memory: {e}", duration_ms=duration)


async def check_aws_credentials(ctx: HealthContext) -> CheckResult:
    """INFRA_005: Check AWS credentials validity.

    Detects:
    - Missing AWS credentials
    - Expired session tokens
    - Stale environment variables blocking valid profile credentials
    """
    import json
    import subprocess

    start = time.monotonic()

    try:
        # Check if AWS CLI is available
        which_result = subprocess.run(
            ["which", "aws"],
            capture_output=True,
            timeout=5,
        )
        if which_result.returncode != 0:
            duration = int((time.monotonic() - start) * 1000)
            return CheckResult.skipped("AWS CLI not installed")

        # Check for environment variables
        env_key_set = bool(os.environ.get("AWS_ACCESS_KEY_ID"))
        env_secret_set = bool(os.environ.get("AWS_SECRET_ACCESS_KEY"))
        creds_file = Path.home() / ".aws" / "credentials"
        creds_file_exists = creds_file.exists()

        # Try default credential chain
        identity_proc = subprocess.run(
            ["aws", "sts", "get-caller-identity", "--output", "json"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        duration = int((time.monotonic() - start) * 1000)

        if identity_proc.returncode == 0:
            identity = json.loads(identity_proc.stdout)
            result = CheckResult.ok(
                f"AWS credentials OK (account: {identity.get('Account')}, "
                f"identity: {identity.get('Arn', 'unknown')})"
            )
            result.duration_ms = duration
            return result

        # Default chain failed - check if stale env vars are the issue
        if env_key_set and env_secret_set and creds_file_exists:
            # Try with explicit default profile
            profile_proc = subprocess.run(
                ["aws", "sts", "get-caller-identity", "--profile", "default", "--output", "json"],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if profile_proc.returncode == 0:
                # Stale env vars are blocking valid profile credentials
                rpn = INFRA_005.calculate_rpn()
                return CheckResult.critical(
                    "Stale AWS environment variables blocking valid profile credentials",
                    failure_mode_id="INFRA_005",
                    rpn=rpn,
                    remediation=(
                        "Fix with one of:\n"
                        "  1. unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY\n"
                        "  2. export AWS_PROFILE=default\n"
                        "  3. Add 'export AWS_PROFILE=default' to .envrc.local"
                    ),
                    duration_ms=duration,
                )

        # Credentials truly invalid
        error_msg = identity_proc.stderr.strip()
        rpn = INFRA_005.calculate_rpn()
        return CheckResult.critical(
            f"AWS credentials invalid: {error_msg[:100]}",
            failure_mode_id="INFRA_005",
            rpn=rpn,
            remediation=(
                "Configure AWS credentials:\n"
                "  1. aws configure (create new credentials)\n"
                "  2. aws sso login (if using SSO)\n"
                "  3. Check ~/.aws/credentials file"
            ),
            duration_ms=duration,
        )

    except subprocess.TimeoutExpired:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.error("AWS CLI timeout - check network", duration_ms=duration)
    except Exception as e:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.error(f"Failed to check AWS credentials: {e}", duration_ms=duration)


async def check_shared_memory_limits(ctx: HealthContext) -> CheckResult:
    """SYSTEM_001: Check macOS shared memory kernel limits.

    On macOS, the default kern.sysv.shmmax (4MB) is too low for PostgreSQL
    and other services that use shared memory. This check verifies limits
    are set high enough.

    Recommended values:
    - kern.sysv.shmmax = 1073741824 (1GB)
    - kern.sysv.shmall = 262144 (pages)
    - kern.sysv.shmmni = 256 (segments)
    """
    import platform
    import subprocess

    start = time.monotonic()
    system = platform.system()

    # Only relevant on macOS and Linux
    if system not in ("Darwin", "Linux"):
        return CheckResult.skipped(f"Shared memory limits check not applicable on {system}")

    try:
        # Platform-specific sysctl keys
        if system == "Darwin":
            sysctl_keys = ["kern.sysv.shmmax", "kern.sysv.shmall", "kern.sysv.shmmni"]
            prefix = "kern.sysv."
        else:  # Linux
            sysctl_keys = ["kernel.shmmax", "kernel.shmall", "kernel.shmmni"]
            prefix = "kernel."

        # Get current sysctl values
        result = subprocess.run(
            ["sysctl"] + sysctl_keys,
            capture_output=True,
            text=True,
            timeout=5,
        )

        if result.returncode != 0:
            duration = int((time.monotonic() - start) * 1000)
            return CheckResult.error(f"sysctl command failed: {result.stderr}", duration_ms=duration)

        # Parse output: "kern.sysv.shmmax: 4194304" (macOS) or "kernel.shmmax = 18446744073692774399" (Linux)
        values = {}
        for line in result.stdout.strip().split('\n'):
            # Handle both ": " (macOS) and " = " (Linux) separators
            if ':' in line or '=' in line:
                sep = ':' if ':' in line else '='
                key, val = line.split(sep, 1)
                key = key.strip().replace(prefix, '')
                values[key] = int(val.strip())

        shmmax = values.get('shmmax', 0)
        shmall = values.get('shmall', 0)
        shmmni = values.get('shmmni', 0)

        duration = int((time.monotonic() - start) * 1000)

        # Minimum recommended values
        MIN_SHMMAX = 1073741824  # 1GB
        MIN_SHMALL = 262144     # pages (1GB with 4KB pages)
        MIN_SHMMNI = 256        # segments

        issues = []
        if shmmax < MIN_SHMMAX:
            issues.append(f"shmmax={shmmax} (need {MIN_SHMMAX})")
        if shmall < MIN_SHMALL:
            issues.append(f"shmall={shmall} (need {MIN_SHMALL})")
        if shmmni < MIN_SHMMNI:
            issues.append(f"shmmni={shmmni} (need {MIN_SHMMNI})")

        if issues:
            from ..catalog import SYSTEM_001
            rpn = SYSTEM_001.calculate_rpn()
            return CheckResult.warning(
                f"Shared memory limits too low: {', '.join(issues)}",
                failure_mode_id="SYSTEM_001",
                rpn=rpn,
                remediation="Run: /health fix SYSTEM_001 --apply (requires sudo)",
                shmmax=shmmax,
                shmall=shmall,
                shmmni=shmmni,
                duration_ms=duration,
            )

        result_obj = CheckResult.ok(
            f"Shared memory limits OK (shmmax={shmmax}, shmall={shmall}, shmmni={shmmni})"
        )
        result_obj.duration_ms = duration
        return result_obj

    except subprocess.TimeoutExpired:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.error("sysctl command timed out", duration_ms=duration)
    except FileNotFoundError:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.skipped("sysctl command not found")
    except Exception as e:
        duration = int((time.monotonic() - start) * 1000)
        return CheckResult.error(f"Failed to check shared memory limits: {e}", duration_ms=duration)


# Registry of all infra checks
CHECKS: dict[str, Any] = {
    "INFRA_001": check_postgres,
    "INFRA_002": check_minio,
    "INFRA_003": check_polaris,
    "INFRA_004": check_shared_memory,
    "INFRA_005": check_aws_credentials,
    "SYSTEM_001": check_shared_memory_limits,
}
