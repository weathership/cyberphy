# Configuration

Complete configuration reference for the Cyberphy Toolkit.

## Environment Variables

### Required

| Variable | Default | Description |
|----------|---------|-------------|
| `POLARIS_API_URL` | `http://localhost:8181` | Polaris REST API endpoint |
| `POLARIS_ADMIN_URL` | `http://localhost:8182` | Polaris Admin API endpoint |
| `ICEBERG_WAREHOUSE` | `s3://cybersec/iceberg/warehouse` | Iceberg warehouse location |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | MinIO/S3 access key |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | MinIO/S3 secret key |
| `S3_ENDPOINT` | `http://localhost:9010` | MinIO endpoint |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `JAVA_DATAGEN_RPS` | `100` | Rows per second for Java DataGen |
| `FLINK_HOME` | (auto-detected) | Flink installation directory |
| `FLINK_STATE_DIR` | `$DEVENV_STATE/flink` | Flink state directory |
| `LOG_LEVEL` | `INFO` | Logging level |
| `PYTHONUNBUFFERED` | `1` | Disable Python output buffering |

## Iceberg Table Configuration

### Catalog

- Type: REST (Polaris)
- Endpoint: `http://localhost:8181/api/catalog`
- Warehouse: `cybersec`

### Warehouse

- Location: `s3://cybersec/iceberg/warehouse`
- Storage: MinIO (localhost:9010)
- Format: Parquet
- File I/O: FsspecFileIO (s3fs backend)

### Table Schema

The CloudTrail events table (`cybersec.default.cloudtrail_events`) has the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | STRING | Unique event identifier |
| `event_version` | STRING | CloudTrail event version |
| `event_timestamp` | TIMESTAMP | When the event occurred |
| `event_source` | STRING | AWS service (e.g., s3.amazonaws.com) |
| `event_name` | STRING | API action name (e.g., GetObject) |
| `aws_region` | STRING | AWS region |
| `source_ip` | STRING | Source IP address |
| `user_agent` | STRING | Client user agent |
| `user_type` | STRING | IAM user type |
| `user_arn` | STRING | IAM user ARN |
| `account_id` | STRING | AWS account ID |
| `read_only` | BOOLEAN | Whether the operation is read-only |
| `event_type` | STRING | Event type (e.g., AwsApiCall) |
| `processing_time` | TIMESTAMP | When the event was processed |

### Sample Event

```json
{
  "event_version": "1.08",
  "event_timestamp": "2026-01-20T10:30:45Z",
  "event_source": "s3.amazonaws.com",
  "event_name": "GetObject",
  "aws_region": "us-east-1",
  "source_ip": "192.168.1.100",
  "user_agent": "aws-cli/2.13.0",
  "user_type": "IAMUser",
  "user_arn": "arn:aws:iam::123456789012:user/testuser1",
  "account_id": "123456789012",
  "event_id": "12345678-1234-1234-1234-123456789012",
  "read_only": true,
  "event_type": "AwsApiCall",
  "processing_time": "2026-01-20T10:30:46.123456Z"
}
```

### Partitioning

- Partition by: `event_day` (day transform on `event_timestamp`) and `region`
- Benefits:
  - Efficient time-range queries
  - Partition pruning
  - Easy data lifecycle management

### Sorting

Primary sort order:
1. `event_timestamp` (ascending)
2. `event_id` (ascending)

Benefits:
- Faster time-based queries
- Better compression
- Efficient range scans

### Sample Partition Layout

```
s3://cybersec/iceberg/warehouse/
  cybersec/
    default/
      cloudtrail_events/
        metadata/
          v1.metadata.json
          snap-123456789.avro
        data/
          event_day=2026-01-20/region=us-east-1/
            00000-0-abc123.parquet
            00001-0-def456.parquet
          event_day=2026-01-21/region=us-west-2/
            00000-0-ghi789.parquet
```

## Flink Configuration

### Java DataGen Job (Default)

```yaml
rows-per-second: 100  # configurable via JAVA_DATAGEN_RPS
checkpoint-interval: 60000  # 60 seconds
```

### Python DataGen Job (Optional)

```yaml
rows-per-second: 10
fields:
  event_id:
    kind: sequence
    start: 1
    end: 1000000
```

### Resources

```yaml
taskmanager.numberOfTaskSlots: 4
parallelism.default: 2
execution.checkpointing.interval: 60000  # 60 seconds
jobmanager.memory.process.size: 2g
taskmanager.memory.process.size: 4g
```

Adjust via devenv.nix or environment variables.

### Rate Tuning

```bash
# Increase Java DataGen rate
export JAVA_DATAGEN_RPS=1000  # 1000 rows/sec
devenv up
```

## Query Examples

### Python API

```python
from iceberg_writer.cloudtrail_query import CloudTrailQuery

query = CloudTrailQuery(
    catalog_uri="postgresql://postgres@localhost:5438/cybersec",
    warehouse_path="s3://cybersec/iceberg/warehouse"
)

# Recent events
events = query.query_recent_events(hours=24, limit=1000)

# Query by event name
console_logins = query.query_by_event_name("ConsoleLogin")

# Events from specific IP
events = query.query_by_source_ip("192.168.1.100")

# Statistics
stats = query.get_event_statistics(hours=24)
# {
#   "total_events": 86400,
#   "unique_event_names": 16,
#   "unique_accounts": 50,
#   "unique_ips": 234,
#   "unique_regions": 4,
#   "read_only_events": 45000
# }
```

### DuckDB Integration

```python
import duckdb

con = duckdb.connect()
con.execute("""
    INSTALL iceberg;
    LOAD iceberg;

    SELECT event_name, COUNT(*) as count
    FROM iceberg_scan('s3://cybersec/iceberg/warehouse/cybersec/cloudtrail_events')
    WHERE event_timestamp > NOW() - INTERVAL '1 hour'
    GROUP BY event_name
    ORDER BY count DESC
""")
```

### Apache Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.cybersec", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.cybersec.type", "jdbc") \
    .config("spark.sql.catalog.cybersec.uri", "jdbc:postgresql://localhost:5438/cybersec") \
    .getOrCreate()

df = spark.table("cybersec.cloudtrail_events")
df.filter("event_name = 'ConsoleLogin'").show()
```

## Monitoring

### PostgreSQL Catalog Queries

```sql
-- List all tables
SELECT * FROM iceberg.catalog_tables;

-- Table metadata
SELECT
  table_name,
  metadata_location,
  previous_metadata_location
FROM iceberg.catalog_tables
WHERE catalog_name = 'cybersec';
```

### Polaris REST API

```bash
# Check catalog configuration
curl http://localhost:8181/api/catalog/v1/config

# List namespaces
curl -u admin:admin http://localhost:8181/api/catalog/v1/namespaces

# List tables
curl -u admin:admin http://localhost:8181/api/catalog/v1/namespaces/default/tables
```

### Parquet File Statistics

```python
import pyarrow.parquet as pq

parquet_file = pq.ParquetFile('path/to/file.parquet')
print(parquet_file.metadata)
print(parquet_file.schema)

# Row group statistics
for i in range(parquet_file.num_row_groups):
    rg = parquet_file.metadata.row_group(i)
    print(f"Row group {i}: {rg.num_rows} rows")
```

## Data Lifecycle Management

### Iceberg Maintenance

```python
from datetime import datetime, timedelta

# Expire old snapshots
table.expire_snapshots(
    older_than=datetime.now() - timedelta(days=30)
)

# Remove orphan files
table.remove_orphans()

# Rewrite manifest files
table.rewrite_manifests()
```

### PostgreSQL Cleanup (using pg_cron)

```sql
-- Schedule weekly cleanup
SELECT cron.schedule(
  'iceberg-cleanup',
  '0 2 * * 0',  -- Every Sunday at 2 AM
  $$
    DELETE FROM iceberg.catalog_tables
    WHERE metadata_location IS NULL
      AND updated_at < NOW() - INTERVAL '90 days'
  $$
);
```

## Scaling Considerations

### Horizontal Scaling

- Increase Flink TaskManager slots
- Run multiple DataGen jobs
- Partition by additional columns

### Vertical Scaling

- Increase TaskManager memory
- Larger batch sizes for Iceberg writes
- More CPU cores for Flink

### Storage Scaling

- MinIO can be clustered for HA
- PostgreSQL can use replication
- Iceberg supports multiple table formats

## Security Considerations

This is a development setup. For production:

1. **Enable authentication** on all services
2. **Use TLS/SSL** for communication
3. **Rotate credentials** regularly
4. **Implement proper IAM** for S3/MinIO access
5. **Enable Flink security** features
6. **Use secrets management** for credentials
