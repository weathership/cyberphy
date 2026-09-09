#!/usr/bin/env bash
# Setup Polaris catalog and warehouse using Management API
#
# Defaults match cyberphy + RustFS local lab:
#   catalog: cyberphy
#   warehouse: s3://cyberphy/iceberg/warehouse
#   S3 endpoint: http://localhost:9010 (RustFS)
#   creds: admin/admin
#
# Override via env: POLARIS_CATALOG_NAME, S3_ENDPOINT, S3_BUCKET,
#   S3_ACCESS_KEY / S3_SECRET_KEY (or MINIO_* / RUSTFS_* / AWS_*).
set -euo pipefail

CATALOG_NAME="${POLARIS_CATALOG_NAME:-cyberphy}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9010}"
S3_BUCKET="${S3_BUCKET:-cyberphy}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-${RUSTFS_ACCESS_KEY:-${MINIO_ACCESS_KEY:-${AWS_ACCESS_KEY_ID:-admin}}}}"
S3_SECRET_KEY="${S3_SECRET_KEY:-${RUSTFS_SECRET_KEY:-${MINIO_SECRET_KEY:-${AWS_SECRET_ACCESS_KEY:-admin}}}}"
WAREHOUSE="${POLARIS_WAREHOUSE:-s3://${S3_BUCKET}/iceberg/warehouse}"

echo "=== Setting up Polaris Catalog and Warehouse (cyberphy / RustFS) ==="
echo "  catalog:   $CATALOG_NAME"
echo "  warehouse: $WAREHOUSE"
echo "  s3:        $S3_ENDPOINT  (bucket=$S3_BUCKET region=$S3_REGION)"

# --- Wait for RustFS (S3) ---
echo "Waiting for RustFS/S3 at $S3_ENDPOINT ..."
for i in $(seq 1 30); do
  if curl -s -f "${S3_ENDPOINT}/health" >/dev/null 2>&1 \
     || curl -s -f "${S3_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
    echo "RustFS/S3 is ready"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: S3 endpoint not reachable at $S3_ENDPOINT"
    exit 1
  fi
  echo "Waiting for S3... attempt $i/30"
  sleep 2
done

# Ensure warehouse bucket exists (idempotent)
echo "Ensuring S3 bucket '$S3_BUCKET' exists..."
python3 - <<PY
import os, sys
try:
    import boto3
    from botocore.client import Config
except ImportError:
    print("boto3 not available; relying on pre-created bucket dirs")
    sys.exit(0)

endpoint = os.environ.get("S3_ENDPOINT", "$S3_ENDPOINT")
key = os.environ.get("S3_ACCESS_KEY", "$S3_ACCESS_KEY")
secret = os.environ.get("S3_SECRET_KEY", "$S3_SECRET_KEY")
bucket = os.environ.get("S3_BUCKET", "$S3_BUCKET")
region = os.environ.get("S3_REGION", "$S3_REGION")

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=key,
    aws_secret_access_key=secret,
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    region_name=region,
)
try:
    s3.head_bucket(Bucket=bucket)
    print(f"Bucket already exists: {bucket}")
except Exception:
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"Created bucket: {bucket}")
    except Exception as e:
        # Directory-style buckets (mkdir on rustfs data dir) may already work
        print(f"create_bucket note: {e}")
PY

# --- Wait for Polaris ---
echo "Waiting for Polaris Management API..."
for i in $(seq 1 30); do
  if curl -s -f "http://localhost:8182/q/health/ready" >/dev/null 2>&1; then
    echo "Polaris is ready!"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Polaris not ready"
    exit 1
  fi
  echo "Waiting... attempt $i/30"
  sleep 2
done

# Get OAuth token
echo "Getting OAuth token..."
TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=admin&client_secret=admin&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])" 2>/dev/null || true)

if [ -z "${TOKEN:-}" ]; then
  echo "Warning: Could not get OAuth token. Trying with Basic auth..."
  AUTH_HEADER="Authorization: Basic YWRtaW46YWRtaW4="
else
  echo "Got OAuth token"
  AUTH_HEADER="Authorization: Bearer $TOKEN"
fi

# Skip if catalog already exists
if [ -n "${TOKEN:-}" ]; then
  EXISTING=$(curl -s -H "Authorization: Bearer $TOKEN" \
    http://localhost:8181/api/management/v1/catalogs 2>/dev/null || true)
  if echo "$EXISTING" | grep -q "\"name\":\"${CATALOG_NAME}\""; then
    echo "Catalog '${CATALOG_NAME}' already exists — skipping create"
    exit 0
  fi
fi

echo ""
echo "Creating catalog '${CATALOG_NAME}' with RustFS S3 storage..."
# Note: pathStyleAccess + endpoint for local RustFS; STS unavailable on local S3
HTTP_CODE=$(curl -s -o /tmp/polaris-catalog-create.json -w "%{http_code}" \
  -X POST "http://localhost:8181/api/management/v1/catalogs" \
  -H "$AUTH_HEADER" \
  -H "Content-Type: application/json" \
  -d "{
    \"catalog\": {
      \"name\": \"${CATALOG_NAME}\",
      \"type\": \"INTERNAL\",
      \"storageConfigInfo\": {
        \"storageType\": \"S3\",
        \"endpoint\": \"${S3_ENDPOINT}\",
        \"pathStyleAccess\": true,
        \"region\": \"${S3_REGION}\",
        \"stsUnavailable\": true,
        \"allowedLocations\": [
          \"s3://${S3_BUCKET}\",
          \"${WAREHOUSE}\"
        ]
      },
      \"properties\": {
        \"default-base-location\": \"${WAREHOUSE}\"
      }
    }
  }")

echo "Create catalog HTTP $HTTP_CODE"
cat /tmp/polaris-catalog-create.json 2>/dev/null || true
echo ""

if [ "$HTTP_CODE" != "201" ] && [ "$HTTP_CODE" != "200" ]; then
  # 409 = already exists is OK
  if [ "$HTTP_CODE" = "409" ] || grep -qi 'already exists\|Conflict' /tmp/polaris-catalog-create.json 2>/dev/null; then
    echo "Catalog already present (HTTP $HTTP_CODE)"
  else
    echo "ERROR: catalog create failed"
    exit 1
  fi
fi

echo ""
echo "=== Granting Permissions ==="

if [ -z "${TOKEN:-}" ]; then
  TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=client_credentials&client_id=admin&client_secret=admin&scope=PRINCIPAL_ROLE:ALL" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])" 2>/dev/null || true)
fi

if [ -z "${TOKEN:-}" ]; then
  echo "Warning: Could not get OAuth token. Skipping grant configuration."
else
  echo "Creating catalog role 'data_access' in ${CATALOG_NAME}..."
  curl -s -X POST "http://localhost:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "catalogRole": {
        "name": "data_access",
        "properties": {}
      }
    }' >/dev/null || true

  echo "Granting privileges to data_access..."
  for privilege in CATALOG_MANAGE_CONTENT CATALOG_MANAGE_ACCESS TABLE_READ_DATA TABLE_WRITE_DATA NAMESPACE_FULL_METADATA TABLE_FULL_METADATA; do
    curl -s -X PUT "http://localhost:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/data_access/grants" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{
        \"grant\": {
          \"type\": \"catalog\",
          \"privilege\": \"${privilege}\"
        }
      }" >/dev/null || true
  done

  echo "Assigning data_access role to service_admin..."
  curl -s -X PUT "http://localhost:8181/api/management/v1/principal-roles/service_admin/catalog-roles/${CATALOG_NAME}" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "catalogRole": {
        "name": "data_access"
      }
    }' >/dev/null || true
fi

echo ""
echo "=== Setup Complete ==="
echo "Catalog: $CATALOG_NAME"
echo "Base location: $WAREHOUSE"
echo "S3 (RustFS): $S3_ENDPOINT"
echo ""
echo "Verify:"
echo "  curl -s -H \"Authorization: Bearer \$TOKEN\" http://localhost:8181/api/management/v1/catalogs"
echo "  devenv tasks run polaris:check"
