# Troubleshooting

Common issues and solutions for the Cyberphy Toolkit.

## Quick Diagnostics

Run these commands to quickly identify issues:

```bash
# Full health check
cybersec health

# Service status
cybersec bootstrap status

# Specific component
cybersec health flink
cybersec health rest-catalog
cybersec health local-s3
```

## Service Issues

### PostgreSQL

**Problem**: PostgreSQL not accepting connections

```bash
# Check if running
pg_isready -h localhost -p 5438

# Check logs
journalctl -u devenv-postgres --since "5 minutes ago"

# Restart
devenv tasks run restart:clean && devenv up
```

**Problem**: "relation does not exist" errors

```bash
# Re-initialize catalog
devenv tasks run polaris:init
```

### Polaris

**Problem**: "Catalog 'cybersec' not found"

```bash
# Initialize catalog
devenv tasks run polaris:init

# Verify
devenv tasks run polaris:check
```

**Problem**: "not authorized for op LOAD_TABLE_WITH_READ_DELEGATION"

Missing TABLE_READ_DATA privilege:

```bash
devenv tasks run polaris:init
```

**Problem**: Polaris not responding

```bash
# Check health
curl http://localhost:8182/q/health/ready

# Restart
devenv tasks run restart:clean && devenv up
```

### MinIO

**Problem**: "Connection refused" on port 9010

```bash
# Check status
curl http://localhost:9010/minio/health/live

# Check logs
devenv up  # Watch output for MinIO errors
```

**Problem**: "Access Denied" when writing

Verify credentials:

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export S3_ENDPOINT=http://localhost:9010
```

### Flink

**Problem**: Flink not available

```bash
# Check if built
ls $DEVENV_STATE/flink-built 2>/dev/null || echo "Flink not built"

# Build Flink
cd thirdparty/flink
mvn clean install -DskipTests -Dfast
```

**Problem**: "No Flink jobmanager"

```bash
# Verify Flink home
cybersec bootstrap settings --show | grep flink

# Set Flink home
cybersec bootstrap settings --set flink_home=/path/to/flink
```

**Problem**: TaskManagers not registering

```bash
# Check Flink Web UI
open http://localhost:8081

# Check logs
$FLINK_HOME/log/flink-*-taskexecutor-*.log
```

## Iceberg Issues

**Problem**: "Table not found"

```bash
# List tables
uv run python -c "
from pyiceberg.catalog import load_catalog
catalog = load_catalog('cybersec')
print(list(catalog.list_tables('cybersec')))
"
```

**Problem**: "Failed to commit"

Snapshot conflict - retry the operation. If persistent:

```bash
# Check table metadata
uv run python iceberg_writer/cloudtrail_query.py status
```

**Problem**: "No data in table"

Verify pipeline is running:

```bash
# Check Flink jobs
curl http://localhost:8081/jobs/overview

# Check Iceberg Browser
open http://localhost:5050
```

## PyIceberg Compatibility

### PyArrow Version Mismatch

**Problem**: `TypeError: __cinit__() got an unexpected keyword argument 'store_decimal_as_integer'`

This occurs with certain combinations of pyarrow and pyiceberg-core versions.

**Workarounds**:
1. Use compatible version combinations (pyiceberg 0.10.0 with pyarrow 15.x)
2. Use Flink's native Iceberg connector (Java) instead of PyIceberg for writes
3. Use Spark for writing to Iceberg tables

### Table Partitioning Issues

**Problem**: Partitioning triggers errors with pyiceberg

**Workaround**: For testing, disable partitioning (`partition_spec = None`). For production, use the Java Flink pipeline which handles partitioning correctly.

### MinIO S3 Compatibility

PyIceberg works with MinIO via s3fs. Required configuration:

```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog(
    "cybersec",
    **{
        "uri": "postgresql://postgres@localhost:5438/cybersec",
        "s3.endpoint": "http://localhost:9010",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
    }
)
```

## K8s Issues (K3d/RKE2)

**Problem**: K3d cluster not starting

```bash
# Destroy and recreate
devenv tasks run k8s:destroy
devenv tasks run k8s:provision
```

**Problem**: Pods stuck in Pending

```bash
# Check events
kubectl describe pod <pod-name>

# Check resources
kubectl top nodes
kubectl describe node
```

**Problem**: Port-forward not working

```bash
# Kill existing forwards
pkill -f "kubectl port-forward"

# Restart
devenv tasks run k8s:forward
```

## Environment Issues

**Problem**: Python dependencies not found

```bash
# Reinstall
uv sync
```

**Problem**: "Command not found: cybersec"

```bash
# Install CLI
uv pip install -e .

# Or use directly
uv run python -m cybersec.cli.main <command>
```

**Problem**: Environment variables not set

```bash
# Check current settings
cybersec bootstrap info

# Source devenv environment
eval "$(devenv shell)"
```

## Clean Restart Procedure

When all else fails:

```bash
# 1. Stop everything
devenv tasks run restart:clean

# 2. Optional: Reset state (DESTRUCTIVE)
rm -rf $DEVENV_STATE/postgres
rm -rf $DEVENV_STATE/minio
rm -rf $DEVENV_STATE/flink

# 3. Start fresh
devenv up

# 4. Wait for services
sleep 30

# 5. Run bootstrap
cybersec bootstrap run

# 6. Verify
cybersec health
```

## Getting Help

If issues persist:

1. Check health diagnostics: `cybersec health diagnose <FAILURE_ID>`
2. Review logs: `devenv up` shows all service output
3. Ask the @ops agent: `@ops troubleshoot <issue>`
4. File an issue: https://github.com/anthropics/claude-code/issues
