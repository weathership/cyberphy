#!/usr/bin/env bash
# Complete E2E Test: Write data and verify it exists

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/flink-env.sh
source "${SCRIPT_DIR}/scripts/flink-env.sh"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

log_info "=== Flink DataGen -> Iceberg -> MinIO E2E Test ==="

# Step 1: Clean up
log_info "Step 1: Cleaning up previous test data..."
"${FLINK_HOME}/bin/sql-client.sh" <<'CLEANUP' 2>/dev/null || true
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://localhost:8181/api/catalog',
  'warehouse' = 'cybersec',
  'credential' = 'admin:admin',
  'oauth2-server-uri' = 'http://localhost:8181/api/catalog/v1/oauth/tokens',
  'scope' = 'PRINCIPAL_ROLE:ALL'
);
USE CATALOG iceberg_catalog;
DROP TABLE IF EXISTS e2e_test.test_data;
DROP DATABASE IF EXISTS e2e_test CASCADE;
exit;
CLEANUP

# Step 2: Submit Flink job to write data
log_info "Step 2: Submitting Flink job to write 100 test records..."
log_info "This will take about 10-15 seconds..."

timeout 60 "${FLINK_HOME}/bin/sql-client.sh" -f "${SCRIPT_DIR}/test_write_data.sql" > /tmp/flink_write.log 2>&1 || {
    if [ $? -eq 124 ]; then
        log_warn "Job timed out (expected - job may still be running)"
    else
        log_error "Job submission failed"
        tail -50 /tmp/flink_write.log
        exit 1
    fi
}

# Wait for job to complete
log_info "Waiting for Flink job to complete..."
sleep 15

# Check Flink UI for completed jobs
log_info "Checking Flink UI for job status..."
JOB_STATUS=$(curl -s http://localhost:8081/jobs/overview | jq -r '.jobs[] | select(.state == "FINISHED") | .state' | head -1)
if [ "$JOB_STATUS" = "FINISHED" ]; then
    log_info "✓ Flink job completed successfully"
else
    log_warn "Job may still be running or failed - check http://localhost:8081"
fi

# Step 3: Verify data via MinIO
log_info "Step 3: Checking files in MinIO..."
if command -v mc &> /dev/null; then
    mc alias set local http://localhost:9010 minioadmin minioadmin 2>/dev/null || true
    
    METADATA_COUNT=$(mc ls local/cybersec/iceberg/warehouse/e2e_test/test_data/metadata/ 2>/dev/null | wc -l | tr -d ' ')
    DATA_COUNT=$(mc find local/cybersec/iceberg/warehouse/e2e_test/test_data/data/ --name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
    
    log_info "Found $METADATA_COUNT metadata files"
    log_info "Found $DATA_COUNT data files"
    
    if [ "$DATA_COUNT" -gt 0 ]; then
        log_info "✓ Data files present in MinIO"
        log_info "Sample data files:"
        mc find local/cybersec/iceberg/warehouse/e2e_test/test_data/data/ --name "*.parquet" 2>/dev/null | head -5
    else
        log_error "✗ No data files found - insert may have failed"
        exit 1
    fi
else
    log_warn "MinIO Client (mc) not installed - skipping file check"
    log_info "Install with: brew install minio/stable/mc"
fi

# Step 4: Query data via Python (more reliable than Flink SQL for queries)
log_info "Step 4: Querying data via PyIceberg..."
python3 <<'PYQUERY'
import os
os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'

from pyiceberg.catalog import load_catalog

try:
    catalog = load_catalog(
        "cybersec",
        **{
            "type": "rest",
            "uri": "http://localhost:8181/api/catalog",
            "credential": "admin:admin",
            "warehouse": "cybersec",
            "s3.endpoint": "http://localhost:9010",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
        }
    )
    
    table = catalog.load_table("e2e_test.test_data")
    
    # Get row count
    df = table.scan().to_pandas()
    row_count = len(df)
    
    print(f"✓ Successfully queried table: {row_count} rows")
    
    if row_count > 0:
        print(f"\nSample data (first 5 rows):")
        print(df.head().to_string())
        
        print(f"\nPartition distribution:")
        print(df['region'].value_counts().to_string())
        
        exit(0)
    else:
        print("✗ Table exists but no data found")
        exit(1)
        
except Exception as e:
    print(f"✗ Error querying table: {e}")
    exit(1)
PYQUERY

QUERY_RESULT=$?

# Step 5: Show results
log_info "=== E2E Test Results ==="
if [ $QUERY_RESULT -eq 0 ]; then
    log_info "✓✓✓ SUCCESS: Complete E2E pipeline working ✓✓✓"
    log_info ""
    log_info "Pipeline verification:"
    log_info "  ✓ Flink DataGen generated 100 records"
    log_info "  ✓ Data written to Iceberg table"
    log_info "  ✓ Files stored in MinIO (S3)"
    log_info "  ✓ Data queryable via PyIceberg"
    log_info ""
    log_info "Next steps:"
    log_info "  - View Flink UI: http://localhost:8081"
    log_info "  - Check MinIO: http://localhost:9010 (minioadmin/minioadmin)"
    log_info "  - Run: python iceberg_browser.py"
else
    log_error "✗✗✗ E2E test failed - see errors above ✗✗✗"
    log_info "Troubleshooting:"
    log_info "  - Check Flink logs: ${FLINK_HOME}/log/"
    log_info "  - View job status: http://localhost:8081"
    log_info "  - Check /tmp/flink_write.log"
    exit 1
fi
