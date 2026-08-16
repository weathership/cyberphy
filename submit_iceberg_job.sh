#!/usr/bin/env bash
#
# Submit CloudTrail DataGen to Iceberg streaming job to Flink cluster
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/flink-env.sh
source "${SCRIPT_DIR}/scripts/flink-env.sh"

JAR_PATH="${JAR_PATH:-${FLINK_COMMON_JAR:-}}"
if [ -z "$JAR_PATH" ] || [ ! -f "$JAR_PATH" ]; then
  echo "flink-common JAR not found. Build it with:" >&2
  echo "  cd flink-cyber && mvn clean install -DskipTests -pl flink-common -am" >&2
  exit 1
fi

if [ ! -x "$FLINK_BIN" ]; then
  echo "Flink not found at $FLINK_HOME" >&2
  echo "Set FLINK_HOME or run: devenv tasks run restart:clean" >&2
  exit 1
fi

echo "═══════════════════════════════════════════════════════════════"
echo "  CloudTrail DataGen → Iceberg Streaming Job Submission"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "📦 JAR: $(basename "$JAR_PATH")"
echo "🎯 Main Class: com.cloudera.cyber.flink.iceberg.CloudTrailDataGenIcebergJob"
echo "🔧 Configuration:"
echo "   - FLINK_HOME: $FLINK_HOME"
echo "   - PostgreSQL: localhost:5438/iceberg"
echo "   - MinIO: http://localhost:9010"
echo "   - Event Rate: 5 events/second"
echo ""

"$FLINK_BIN" run \
  -d \
  "$JAR_PATH" \
  --postgres.host localhost \
  --postgres.port 5438 \
  --postgres.db iceberg \
  --postgres.user "${USER}" \
  --minio.endpoint http://localhost:9010 \
  --minio.access-key minioadmin \
  --minio.secret-key minioadmin \
  --rows-per-second 5

echo ""
echo "✅ Job submitted successfully!"
echo "📊 Monitor at: http://localhost:8081"
echo ""
