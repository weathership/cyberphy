# Local Flink Environment

Running Flink jobs locally for development and testing.

## Flink Provisioning

### Option A: Build from Source (Recommended)

```bash
cd thirdparty/flink
git submodule update --init --recursive
mvn clean install -DskipTests -Dfast
```

This builds Flink 1.20.1 with Iceberg 1.9.0 compatibility.

### Option B: Use Existing Installation

```bash
cybersec bootstrap settings --set flink_home=/path/to/flink-1.20.1
```

## Running Jobs

### Python (PyFlink)

```bash
# DataGen job
uv run python flink_jobs/cloudtrail_datagen.py

# Processing job
uv run python flink_jobs/cloudtrail_processor.py
```

### Java (Cyber Toolkit)

```bash
cd flink-cyber
mvn clean package -DskipTests

# Submit to local cluster
$FLINK_HOME/bin/flink run \
  -c com.cloudera.cyber.parser.ParserJob \
  parser-chains-flink/target/parser-chains-flink-*.jar \
  --config config/parser-chain.yaml
```

## Local Cluster Mode

For multi-job testing, start a local cluster:

```bash
# Start cluster
$FLINK_HOME/bin/start-cluster.sh

# Access Web UI
open http://localhost:8081

# Stop cluster
$FLINK_HOME/bin/stop-cluster.sh
```

## Checkpointing

For local development, use filesystem checkpointing:

```python
env.get_checkpoint_config().set_checkpoint_storage_dir(
    "file:///tmp/flink-checkpoints"
)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FLINK_HOME` | repo-relative dist via `scripts/flink-env.sh` | Flink installation path — never a host-absolute default |
| `FLINK_CONF_DIR` | `$DEVENV_STATE/flink/conf` | Runtime overlay (python.executable). Keep this out of Maven `target/` |
| `FLINK_STATE_DIR` | `$DEVENV_STATE/flink` | Checkpoints / tmp — not under the dist |
| `ICEBERG_CATALOG_URI` | `postgresql://...` | Catalog connection |
| `S3_ENDPOINT` | `http://localhost:9010` | MinIO endpoint |

Submit scripts (`submit_iceberg_job.sh`) and PyFlink jobs resolve these through `scripts/flink-env.sh` / `cybersec.flink_paths`. Do not set `pipeline.jars=file:///abs/path` — put connectors in `$FLINK_HOME/lib/` instead.

## Related Scenarios

See [Scenarios](./scenarios.md) for testable workflows.
