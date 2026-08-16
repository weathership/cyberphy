# Building Flink 1.20.1 from Source for Iceberg Integration

## Version Configuration

The cybersec project is now configured to use:
- **Flink 1.20.1** (Apache, not Cloudera) - Compatible with Iceberg's `TableFactory` interface
- **Iceberg 1.9.0** - Latest with `flink-runtime-1.20` support  
- **Java 8** for cybersec code compilation
- **Java 11** for building Flink from source

## Setup Instructions

### 1. Add Flink as Git Submodule

```bash
# From the repository root (path-independent)

# Add Flink 1.20.1 as submodule (already recorded in .gitmodules as thirdparty/flink)
git submodule update --init --recursive thirdparty/flink
```

### 2. Build Flink

```bash
cd flink

# Use Java 11 for building (required for Flink 1.20.x)
export JAVA_HOME=/nix/store/.../jdk11/lib/openjdk  # Or use system Java 11
# OR in devenv: export JAVA_HOME="${pkgs.jdk11}/lib/openjdk"

# Build Flink (takes 10-15 minutes)
mvn clean install -DskipTests -Dfast -Dscala-2.12

cd ..
```

### 3. Update devenv.nix to Use Custom Flink

Replace the Flink processes in `devenv.nix` with:

```nix
processes = {
  flink-jobmanager = {
    exec = ''
      export FLINK_HOME="$PWD/flink/flink-dist/target/flink-1.20.1-bin/flink-1.20.1"
      export FLINK_STATE_DIR="$DEVENV_STATE/flink"
      mkdir -p "$FLINK_STATE_DIR"/{logs,checkpoints,savepoints}
      
      exec $FLINK_HOME/bin/jobmanager.sh start-foreground \
        -D jobmanager.rpc.address=localhost \
        -D rest.port=8081 \
        -D state.checkpoints.dir=file://$FLINK_STATE_DIR/checkpoints \
        -D state.savepoints.dir=file://$FLINK_STATE_DIR/savepoints
    '';
    process-compose = {
      readiness_probe = {
        http_get = {
          host = "localhost";
          port = 8081;
          path = "/overview";
        };
        initial_delay_seconds = 5;
        period_seconds = 2;
        failure_threshold = 30;
      };
    };
  };

  flink-taskmanager = {
    exec = ''
      export FLINK_HOME="$PWD/flink/flink-dist/target/flink-1.20.1-bin/flink-1.20.1"
      export FLINK_STATE_DIR="$DEVENV_STATE/flink"
      mkdir -p "$FLINK_STATE_DIR"/{logs,tmp}
      
      exec $FLINK_HOME/bin/taskmanager.sh start-foreground \
        -D jobmanager.rpc.address=localhost \
        -D taskmanager.numberOfTaskSlots=4 \
        -D taskmanager.tmp.dirs=$FLINK_STATE_DIR/tmp
    '';
    process-compose = {
      depends_on = {
        flink-jobmanager = {
          condition = "process_healthy";
        };
      };
    };
  };
};
```

### 4. Start Services and Submit Job

```bash
# Start all services (PostgreSQL, MinIO, Flink)
devenv up

# In another terminal, submit the Iceberg job (from the repo root)
./submit_iceberg_job.sh
```

## What Changed in cybersec

### 1. POM Version Updates

**`flink-cyber/pom.xml`**:
```xml
<!-- Changed from: <flink.version>1.20.1-csa1.16.0.0</flink.version> -->
<flink.version>1.20.1</flink.version>

<!-- Upgraded from: <iceberg.version>1.7.0</iceberg.version> -->
<iceberg.version>1.9.0</iceberg.version>
```

### 2. Removed Cloudera-Specific Encryption

**`flink-cyber/flink-common/src/main/java/com/cloudera/cyber/flink/Utils.java`**:

Removed dependency on `org.apache.flink.util.encrypttool.EncryptTool` (Cloudera-specific).
The `decrypt()` method now returns input as-is. In production, implement proper encryption here.

```java
public static String decrypt(String input) {
    Preconditions.checkNotNull(input, "key is null");
    // TODO: Implement encryption/decryption for Apache Flink
    return input;
}
```

## Why This Works

**The Problem**:
- nixpkgs Flink 2.1.0 uses the new `CatalogFactory` interface
- Iceberg 1.7.0-1.9.0 still use the old `TableFactory` interface
- These are incompatible

**The Solution**:
- Build Apache Flink 1.20.1 from source (last version using `TableFactory`)
- Use Iceberg 1.9.0 (latest with Flink 1.20 runtime)
- Both use the same `TableFactory` interface ✅

## Testing

Once running, verify:

```bash
# Check Flink Web UI
open http://localhost:8081

# Check job is running
curl http://localhost:8081/jobs

# Query PostgreSQL Iceberg catalog
psql -h localhost -p 5438 -U ryanhill -d iceberg -c "SELECT * FROM iceberg_tables;"

# Check MinIO for Iceberg data
mc ls minio/cybersec/iceberg/warehouse/cybersec/cloudtrail_events/
```

## Files Modified

- ✅ `flink-cyber/pom.xml` - Flink 1.20.1, Iceberg 1.9.0
- ✅ `flink-cyber/flink-common/pom.xml` - Iceberg dependencies
- ✅ `flink-cyber/flink-common/src/main/java/com/cloudera/cyber/flink/Utils.java` - Removed EncryptTool
- ✅ `flink-cyber/flink-common/target/flink-common-2.4.0-iceberg.jar` - Rebuilt with new versions

## Next Steps

1. Add Flink 1.20.1 as git submodule
2. Build Flink from source
3. Update devenv.nix Flink paths
4. Test end-to-end: DataGen → Iceberg → MinIO
