# Replication Strategy

## Overview

Flink streaming replication provides continuous, incremental sync from AWS S3 Table Buckets to on-prem storage (HDFS or Ozone).

## Architecture

```d2
direction: right

AWS: {
  label: "AWS Source"

  s3_tables: {
    label: "S3 Table Bucket\n(cloudtrail_events)"
    shape: cylinder
  }
}

Flink: {
  label: "Flink Replication Job"

  source: "Iceberg\nStreaming Source"
  transform: "Schema\nMapping"
  sink: "Iceberg\nSink"

  source -> transform -> sink
}

OnPrem: {
  label: "On-Prem Target"

  rest_catalog: "Iceberg REST Catalog"

  storage: {
    label: "HDFS or Ozone"
    shape: cylinder
  }

  rest_catalog -> storage
}

AWS.s3_tables -> Flink.source: "Stream read\n(incremental)"
Flink.sink -> OnPrem.storage: "Write"
Flink.sink -> OnPrem.rest_catalog: "Commit"
```

## Flink Replication Job

### Catalog Configuration

```sql
-- AWS Source Catalog (S3 Tables)
CREATE CATALOG aws_catalog WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
  'warehouse' = 'arn:aws:s3tables:us-east-1:123456789012:bucket/cybersec-cloudtrail'
);

-- On-Prem Target Catalog (Iceberg REST + HDFS/Ozone)
CREATE CATALOG onprem_catalog WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'uri' = 'https://iceberg-rest.onprem.example.com',
  'warehouse' = 'ofs://ozone1/iceberg/warehouse'  -- Ozone
  -- OR: 'warehouse' = 'hdfs://namenode:8020/iceberg/warehouse'  -- HDFS
);
```

### Streaming Replication Query

```sql
-- Continuous incremental replication from S3 to on-prem
INSERT INTO onprem_catalog.cybersec.cloudtrail_events
SELECT
  event_id,
  event_time,
  event_source,
  event_name,
  event_type,
  aws_region,
  source_ip_address,
  user_agent,
  user_identity,
  request_parameters,
  response_elements,
  error_code,
  error_message,
  resources,
  geo_country,
  geo_city,
  asn_org,
  ingested_at,
  raw_event
FROM aws_catalog.cybersec.cloudtrail_events
/*+ OPTIONS(
  'streaming' = 'true',
  'monitor-interval' = '60s'
) */;
```

### PyFlink Replication Job

```python
# flink_jobs/iceberg_replicator.py
"""Flink job to replicate Iceberg tables from S3 to on-prem."""

from pyflink.table import EnvironmentSettings, TableEnvironment

def create_replication_job():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Configure checkpointing for exactly-once
    t_env.get_config().set("execution.checkpointing.interval", "120s")
    t_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")

    # AWS Source Catalog (S3 Tables)
    t_env.execute_sql("""
        CREATE CATALOG aws_catalog WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
            'warehouse' = 'arn:aws:s3tables:us-east-1:123456789012:bucket/cybersec-cloudtrail'
        )
    """)

    # On-Prem Target Catalog (Iceberg REST + Ozone or HDFS)
    t_env.execute_sql("""
        CREATE CATALOG onprem_catalog WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
            'uri' = 'https://iceberg-rest.onprem.example.com',
            'warehouse' = 'ofs://ozone1/iceberg/warehouse'
        )
    """)

    # Start streaming replication
    t_env.execute_sql("""
        INSERT INTO onprem_catalog.cybersec.cloudtrail_events
        SELECT * FROM aws_catalog.cybersec.cloudtrail_events
        /*+ OPTIONS('streaming'='true', 'monitor-interval'='60s') */
    """)

if __name__ == "__main__":
    create_replication_job()
```

## On-Prem Storage Options

### HDFS

Traditional Hadoop storage, well-supported:

```sql
'warehouse' = 'hdfs://namenode:8020/iceberg/warehouse'
```

### Ozone

Cloud-native object store for Cloudera, S3-compatible:

```sql
'warehouse' = 'ofs://ozone1/iceberg/warehouse'
-- OR using S3 gateway:
'warehouse' = 's3a://iceberg-bucket/warehouse'
```

### Comparison

| Feature | HDFS | Ozone |
|---------|------|-------|
| Protocol | hdfs:// | ofs://, s3a:// |
| Scalability | Limited by NameNode | Petabyte scale |
| S3 compatibility | No | Yes (S3 Gateway) |
| Erasure coding | Yes | Yes |
| Recommended for | Existing clusters | New deployments |

## Monitoring

### Replication Metrics

| Metric | Alert Threshold | Description |
|--------|-----------------|-------------|
| `replication_lag_seconds` | > 900 (15 min) | Time since last successful sync |
| `flink_checkpoint_duration` | > 60s | Checkpoint taking too long |
| `flink_records_lag` | > 10,000 | Records pending replication |
| `iceberg_commits_failed` | > 0 | Failed commits to target |

### Lag Dashboard Query

```sql
-- Compare event counts between AWS and on-prem
WITH aws_counts AS (
  SELECT
    date_trunc('hour', event_time) as event_hour,
    count(*) as aws_count
  FROM aws_catalog.cybersec.cloudtrail_events
  WHERE event_time >= current_timestamp - interval '24' hour
  GROUP BY 1
),
onprem_counts AS (
  SELECT
    date_trunc('hour', event_time) as event_hour,
    count(*) as onprem_count
  FROM onprem_catalog.cybersec.cloudtrail_events
  WHERE event_time >= current_timestamp - interval '24' hour
  GROUP BY 1
)
SELECT
  COALESCE(a.event_hour, o.event_hour) as event_hour,
  COALESCE(a.aws_count, 0) as aws_count,
  COALESCE(o.onprem_count, 0) as onprem_count,
  COALESCE(a.aws_count, 0) - COALESCE(o.onprem_count, 0) as diff
FROM aws_counts a
FULL OUTER JOIN onprem_counts o ON a.event_hour = o.event_hour
ORDER BY event_hour DESC;
```

## Conflict Resolution

### Write Conflict Handling

Since AWS is the source of truth for new data:

1. **On-prem is read-only** for replicated tables
2. **Local enrichments** written to separate tables
3. **Merge on read** via Impala views if needed

```sql
-- View that merges AWS data with local enrichments
CREATE VIEW cybersec.cloudtrail_enriched AS
SELECT
  ct.*,
  e.threat_score,
  e.ioc_match
FROM cybersec.cloudtrail_events ct
LEFT JOIN cyberphy.local_enrichments e
  ON ct.event_id = e.event_id;
```

## Fallback: Batch Replication

For initial load or catch-up after extended outage:

```python
# batch_replication.py
"""One-time batch copy for initial sync or catch-up."""

from pyflink.table import EnvironmentSettings, TableEnvironment

def batch_replicate(start_date: str, end_date: str):
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    # ... catalog setup ...

    t_env.execute_sql(f"""
        INSERT INTO onprem_catalog.cybersec.cloudtrail_events
        SELECT * FROM aws_catalog.cybersec.cloudtrail_events
        WHERE event_time >= TIMESTAMP '{start_date}'
          AND event_time < TIMESTAMP '{end_date}'
    """)
```
