# Testing & Verification

End-to-end testing procedures for the Cyberphy Toolkit.

## Quick Verification

Run the E2E test suite:

```bash
./test_complete_e2e.sh
# or
uv run python test_complete_pipeline.py
```

Expected output:
```
PIPELINE TEST PASSED
Results:
   - Flink generated: X JSON file(s)
   - Iceberg stored: Y records
Verify in MinIO:
   http://127.0.0.1:9011/browser/cybersec/iceberg/warehouse/cybersec/cloudtrail_events_test/
```

## Component Verification

### 1. Flink DataGen

Verify the data generator is producing events:

```bash
# Check Flink dashboard for running job
curl -s http://localhost:8081/jobs/overview | jq '.jobs[] | {id, state, name}'

# Expected: job in RUNNING state
```

What to verify:
- DataGen connector working
- Events generated at expected rate (100/sec for Java, 10/sec for Python)
- Output files written successfully

### 2. PyIceberg with PostgreSQL + MinIO

```bash
# Test catalog connection
uv run python -c "
from pyiceberg.catalog import load_catalog
catalog = load_catalog('cybersec')
print('Tables:', list(catalog.list_tables('default')))
"
```

What to verify:
- SQL catalog (PostgreSQL) connection working
- S3FS for MinIO storage working
- Table creation successful
- Schema definition working
- Parquet file writes to MinIO working

### 3. MinIO Storage

Verify data files in MinIO:

1. Open: http://127.0.0.1:9011/browser/cybersec/iceberg/warehouse/
2. Navigate to: `cybersec/cloudtrail_events/data/`
3. Confirm Parquet files with names like: `data-20260120-*.parquet`

```bash
# CLI verification
mc ls local/cybersec/iceberg/warehouse/cybersec/cloudtrail_events/
```

### 4. PostgreSQL Catalog

```bash
psql -h localhost -p 5438 -d cybersec -c "
  SELECT table_namespace, table_name, metadata_location
  FROM iceberg.iceberg_tables
  WHERE table_name = 'cloudtrail_events';
"
```

## Full E2E Checklist

Complete E2E requires all services healthy:

| Service | Port | Verification |
|---------|------|--------------|
| PostgreSQL | 5438 | `pg_isready -h localhost -p 5438` |
| Polaris REST API | 8181 | `curl http://localhost:8181/api/catalog/v1/config` |
| Polaris Admin | 8182 | `curl http://localhost:8182/q/health/ready` |
| MinIO | 9010 | `curl http://localhost:9010/minio/health/live` |
| Iceberg Browser | 5050 | `curl http://localhost:5050/` |
| Flink JobManager | 8081 | `curl http://localhost:8081/overview` |
| Flink TaskManager | - | Check Flink UI for registered TMs |
| OTEL Collector | 4317/4318/8889 | `curl http://localhost:8889/metrics` |
| Prometheus | 9090 | `curl http://localhost:9090/api/v1/status/runtimeinfo` |
| NiFi | 8450 | `curl http://localhost:8450/nifi-api/system-diagnostics` |

## Test Scripts

| Script | Purpose |
|--------|---------|
| `test_complete_pipeline.py` | Full E2E test |
| `test_iceberg_setup.py` | Basic Iceberg connectivity |
| `test_flink_datagen_iceberg.py` | PyFlink with Iceberg catalog (requires JARs) |

## Known Issues & Workarounds

### PyIceberg table.append() Compatibility

**Problem**: Version incompatibility between pyarrow 16.1.0 and pyiceberg-core 0.6.0

```
TypeError: __cinit__() got an unexpected keyword argument 'store_decimal_as_integer'
```

**Workaround**: Direct Parquet file writing bypasses `table.append()`. Files are written to MinIO successfully, but Iceberg metadata may not update (table.scan() shows 0 records).

**Production solution**:
1. Upgrade to compatible versions when available
2. Use Flink's native Iceberg connector (requires flink-connector-iceberg JAR)
3. Use Spark for writing to Iceberg tables

### Table Partitioning

**Problem**: Partitioning triggers problematic code paths in PyIceberg

**Solution**: Disable partitioning (`partition_spec = None`) for PyIceberg tests. The Java pipeline handles partitioning correctly.

## Production Write Options

### Option 1: Spark for Iceberg Writes

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.catalog.cybersec", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.cybersec.type", "jdbc") \
    .config("spark.sql.catalog.cybersec.uri", "jdbc:postgresql://localhost:5438/cybersec") \
    .getOrCreate()

# Read Flink output and write to Iceberg
df = spark.read.json("/tmp/cloudtrail_events")
df.writeTo("cybersec.cloudtrail_events").createOrReplace()
```

### Option 2: Flink with Iceberg Connector

Add to Flink classpath:
```
flink-sql-connector-iceberg-1.18.jar
```

The Java pipeline (`CloudTrailDataGenIcebergJob`) uses this approach and handles all metadata correctly.

## Dependencies

Current tested versions:

```toml
[project.dependencies]
pyarrow = ">=15.0.0"
pandas = ">=2.2.0"
psycopg2-binary = ">=2.9.9"
boto3 = ">=1.34.0"
pyiceberg[s3fs,sql-postgres,pyiceberg-core] = ">=0.10.0"
apache-flink = ">=2.2.0"
```

Actual versions in use:
- PyArrow: 16.1.0
- PyIceberg: 0.10.0
- PyIceberg-Core: 0.6.0
- Apache Flink (Python): 2.2.0

## Verification Summary

Core functionality verified:
- Flink DataGen generates CloudTrail events
- PyIceberg connects to PostgreSQL catalog
- PyIceberg connects to MinIO via S3FS
- Parquet files are written to MinIO
- Java pipeline writes with full metadata support

**Recommendation**: For production workloads, use the Java Flink pipeline or Spark for proper Iceberg metadata management.
