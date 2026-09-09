# Cyberphy System Heuristics

This collection contains diagnostic heuristics for the cybersec data pipeline. Each heuristic follows a four-component structure with FMEA integration:

1. **Symptom** - Brief observable failure description
2. **Cause** - Low-level explanation of why this occurs
3. **Observation** - Detection code and signals
4. **Solution** - Target state and remediation process

## FMEA Integration

Each heuristic includes an FMEA reference with Risk Priority Number:

```
RPN = Severity × Occurrence × Detection (1-1000)

Escalation Tiers:
- TIER_0 (RPN 1-100): Auto-remediate immediately
- TIER_1 (RPN 101-200): Auto-remediate with agent validation
- TIER_2 (RPN 201-400): Require user approval
- MANUAL (RPN 401-1000): Human intervention required
```

## Automation Levels

Each observation and solution is classified:

- **A** (Full Automation) - Agent handles autonomously
- **B** (Partial Automation) - Agent + human option
- **C** (Knowledge Transfer) - Agent provides awareness, human acts

## Categories

Provider-agnostic naming for swappable components:

- `flink/` - Flink and PyFlink issues (JobManager, TaskManager, Python env, JARs)
- `rest-catalog/` - REST catalog issues (Polaris)
- `local-s3/` - Local S3 storage (RustFS; replaces MinIO)
- `postgres/` - PostgreSQL database
- `system/` - OS-level issues (shared memory, eBPF)
- `infra/` - Infrastructure (terraform/ansible)
- `data/` - Data quality, staleness, accumulation

Aliases: `iceberg` → `rest-catalog`, `pyflink` → `flink`

## Usage

```bash
# Run health checks
/health                      # Run full health check
/health --quick              # Run critical checks only
/health --category pyflink   # Run checks for specific category

# Fix detected issues (fix-all mode is default)
/health fix                  # Dry-run all issues
/health fix --apply          # Apply all fixes

# Fix by category
/health fix pyflink          # Dry-run pyflink issues
/health fix system --apply   # Fix system issues

# Fix specific failure mode
/health fix INFRA_004 --apply

# Diagnose specific failure mode
/health diagnose FLINK_001
```

## Adding New Heuristics

1. Create a markdown file in the appropriate category directory
2. Follow the template format with FMEA reference
3. Add a corresponding failure mode to `cybersec/health/catalog.py`
4. Implement the check function in `cybersec/health/checks/<category>.py`
