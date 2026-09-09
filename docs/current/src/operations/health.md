# Health Diagnostics

The health system provides FMEA-based diagnostics and automated remediation for the Cyberphy Toolkit.

## Commands

Health diagnostics are available via CLI and MCP:

```bash
# CLI usage
cybersec health                    # All categories
cybersec health flink              # Flink category only
cybersec health --quick            # Critical checks only

# MCP usage (from Claude Code)
cmd("/health")
cmd("/health flink")
```

## Categories

Provider-agnostic naming for swappable components:

| Category | Description |
|----------|-------------|
| `flink` | Flink and PyFlink issues |
| `rest-catalog` | REST catalog (Polaris) |
| `local-s3` | Local S3 storage (MinIO) |
| `postgres` | PostgreSQL database |
| `system` | OS-level issues (shared memory, eBPF) |
| `infra` | Infrastructure (terraform/ansible) |
| `data` | Data quality checks |

Aliases: `iceberg` → `rest-catalog`, `pyflink` → `flink`

## Fix Mode

Health diagnostics support automatic remediation:

```bash
# Dry-run all issues
cybersec health fix

# Apply all fixes
cybersec health fix --apply

# Fix specific category
cybersec health fix flink --apply

# Fix specific failure mode
cybersec health fix FLINK_001 --apply
```

## Diagnose Mode

Get detailed information about a specific failure mode:

```bash
cybersec health diagnose FLINK_001
```

Output includes:
- Failure mode description
- Severity and impact
- Detection method
- Remediation steps
- Related documentation

## Example Output

```
$ cybersec health

FMEA Health Diagnostics
========================

Category: flink
  [PASS] Flink JobManager is running
  [PASS] TaskManagers registered (1 of 1)
  [WARN] FLINK_003: Checkpoint directory not configured

Category: rest-catalog
  [PASS] Polaris REST API responding
  [PASS] Catalog 'cybersec' exists

Category: local-s3
  [PASS] MinIO API accessible
  [PASS] Bucket 'cybersec' exists

Category: postgres
  [PASS] PostgreSQL accepting connections
  [PASS] Iceberg catalog tables exist

Summary: 7 passed, 1 warning, 0 failed
```

## Integration with @ops

The Operations Agent uses health diagnostics for environment validation:

```
@ops check the environment health
@ops troubleshoot why Flink isn't starting
```

See [Operations Guide](./overview.md) for more details.
