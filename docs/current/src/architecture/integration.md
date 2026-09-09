# Complex Systems Integration

The Cyberphy Toolkit integrates multiple complex systems that must work together reliably. This chapter covers integration patterns, failure modes, and operational considerations.

## Integration Philosophy

### Layered Independence

Each layer operates independently with well-defined interfaces:

```d2
direction: down

Workload: {
  label: "Workload Layer"
  pipelines: "Flink Jobs"
  analytics: "Dask/Jupyter"
  ml: "ML Inference"
}

Platform: {
  label: "Platform Layer"
  k8s: "Kubernetes\n(RKE2/K3d)"
  storage: "Object Storage\n(S3/MinIO)"
  catalog: "Iceberg Catalog\n(Polaris)"
}

Infrastructure: {
  label: "Infrastructure Layer"
  compute: "Compute\n(EC2/Bare Metal)"
  network: "Network\n(VPC/Physical)"
  observe: "Observability\n(OTEL/Prometheus)"
}

Workload -> Platform
Platform -> Infrastructure
```

### Failure Isolation

Components are designed to fail independently:

| Component | Failure Impact | Recovery |
|-----------|----------------|----------|
| Flink TaskManager | Job retries on other TMs | Automatic |
| Polaris | No new table ops | Restart service |
| MinIO | Storage unavailable | Restart or failover |
| PostgreSQL | Catalog metadata unavailable | Restart with WAL |

## Cross-System Data Flow

### Event Pipeline

```d2
direction: right

Source: "CloudTrail\nS3"
Flink: "Flink\nCluster"
Kafka: "Kafka\n(optional)"
Iceberg: "Iceberg\nTable"
Query: "Query\nEngines"

Source -> Flink: "S3 Source"
Flink -> Kafka: "Parsed events"
Kafka -> Flink: "Enrich"
Flink -> Iceberg: "Iceberg Sink"
Iceberg -> Query: "Read"
```

### Metadata Flow

```d2
direction: right

Client: "Flink/Dask\nClient"
Polaris: "Polaris\nREST API"
Postgres: "PostgreSQL\nCatalog"
S3: "S3/MinIO\nMetadata"

Client -> Polaris: "List tables"
Polaris -> Postgres: "Query"
Polaris -> S3: "Read manifests"
S3 -> Polaris: "Manifest data"
Polaris -> Client: "Table metadata"
```

## Operational Boundaries

### Environment Transitions

| From | To | Transition Method |
|------|----|-------------------|
| Laptop | Workstation | Export manifests, apply to RKE2 |
| Workstation | AWS | Terraform, replicate data |
| K3d | RKE2 | Update storage class, apply |

### Data Boundaries

- **Iceberg tables**: Cross-environment via catalog federation
- **Metrics**: Exported via OTEL, aggregated in Prometheus
- **Logs**: Centralized via Flink or NiFi
- **Traces**: End-to-end visibility via OTEL

## Health Integration

The health system validates cross-component integration:

```bash
# Check all integration points
cybersec health

# Specific integration checks
cybersec health flink      # Flink ↔ Iceberg
cybersec health rest-catalog  # Polaris ↔ PostgreSQL
cybersec health local-s3   # MinIO ↔ Iceberg
```

## Failure Mode Analysis

### FMEA Categories

| ID | Failure Mode | Detection | Mitigation |
|----|--------------|-----------|------------|
| FLINK_001 | JobManager crash | Health check fails | Auto-restart via K8s |
| ICEBERG_001 | Snapshot conflict | Commit fails | Retry with backoff |
| POLARIS_001 | Catalog unavailable | API timeout | Circuit breaker |
| MINIO_001 | Storage full | Metrics alert | Expand or archive |

### Recovery Patterns

1. **Automatic Recovery**: K8s restarts failed pods
2. **Circuit Breaker**: Prevent cascade failures
3. **Graceful Degradation**: Continue with reduced functionality
4. **Manual Intervention**: Documented runbooks

## Best Practices

### Configuration Management

- Use `config.toml` for local settings
- Environment variables for secrets
- Helm values for K8s deployments

### Monitoring

- Prometheus for metrics
- OTEL for traces
- Structured logging with correlation IDs

### Testing Integration

```gherkin
Scenario: End-to-end pipeline integration
  Given all services are healthy
  When I submit events to the datagen job
  Then events should flow through Flink
  And events should land in Iceberg
  And queries should return the events
```

See [Benchmarking Scenarios](../workloads/benchmarking/scenarios.md) for detailed test cases.
