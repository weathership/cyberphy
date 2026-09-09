"""
Iceberg Browser - Web UI for CloudTrail Events

A simple Flask web application to browse and search CloudTrail events stored in Iceberg.
Supports real-time updates via Server-Sent Events (SSE).
"""

from flask import Flask, render_template, jsonify, request, Response, stream_with_context
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import time
import json
import threading
from urllib.parse import urlparse
import subprocess

app = Flask(__name__)


# ============================================================================
# Dynamic Host Detection for External Links
# ============================================================================
# When accessed via FQDN (e.g., Cloudflare WARP), adjust service links accordingly.

# Service port mappings - maps service name to its port
SERVICE_PORTS = {
    "flink": 8081,
    # RustFS (local S3); minio_* keys retained as template aliases
    "rustfs_console": 9011,
    "rustfs_api": 9010,
    "minio_console": 9011,
    "minio_api": 9010,
    "polaris_api": 8181,
    "polaris_admin": 8182,
    "nifi": 8450,
    "prometheus": 9090,
    "iceberg_browser": 5050,
    "otel_grpc": 4317,
    "otel_http": 4318,
    "otel_metrics": 8889,
    # K8s stack (NodePort services from Zarf local deploy)
    "viz": 30506,           # OTEL Navigator (Panel-Viz)
    "dask": 30087,          # Dask Dashboard
    "jupyterhub": 30080,    # JupyterHub
    "k8s_dashboard": 10443, # Kubernetes Dashboard (HTTPS)
}

# Server-side health probe paths (host always 127.0.0.1 — hub proxies for UI dots)
# Each entry: list of (path, accept_codes). First success wins.
SERVICE_HEALTH_PROBES = {
    "flink": [("/", {200, 301, 302})],
    "rustfs_console": [("/rustfs/console/", {200, 301, 302, 401, 403})],
    "rustfs_api": [("/health", {200}), ("/minio/health/live", {200})],
    "minio_console": [("/rustfs/console/", {200, 301, 302, 401, 403})],
    "minio_api": [("/health", {200}), ("/minio/health/live", {200})],
    "nifi": [("/nifi/", {200, 301, 302, 401, 403})],
    "prometheus": [("/-/healthy", {200}), ("/", {200})],
    "polaris_admin": [("/q/health/ready", {200})],
    # Catalog root often 404 without a path; any HTTP response means the listener is up
    "polaris_api": [("/api/catalog/v1/config", {200, 401, 403}), ("/", {200, 401, 403, 404})],
    "viz": [("/", {200, 301, 302, 401, 403})],
    "dask": [("/health", {200}), ("/", {200, 301, 302})],
    "jupyterhub": [("/hub/login", {200, 301, 302, 401, 403}), ("/", {200, 301, 302, 401, 403})],
    "k8s_dashboard": [("/", {200, 301, 302, 401, 403})],
}


def get_base_host():
    """
    Detect if we're being accessed via FQDN or localhost.
    Returns the base hostname without port.
    """
    # Check X-Forwarded-Host first (for reverse proxies like Cloudflare)
    forwarded_host = request.headers.get("X-Forwarded-Host")
    if forwarded_host:
        # Extract just the hostname (might be "host:port")
        return forwarded_host.split(":")[0]

    # Fall back to Host header
    host = request.headers.get("Host", "localhost:5050")
    return host.split(":")[0]


def get_service_url(service_name: str, path: str = "") -> str:
    """
    Generate a URL for a service based on the current request context.

    If accessed via localhost, returns localhost URLs.
    If accessed via FQDN, returns FQDN URLs with appropriate ports.

    Args:
        service_name: Name of the service (e.g., "flink", "minio_console")
        path: Optional path to append (e.g., "/api/health")

    Returns:
        Full URL string like "http://localhost:8081" or "http://myhost.example.com:8081"
    """
    base_host = get_base_host()
    port = SERVICE_PORTS.get(service_name, 80)

    # K8s Dashboard is always HTTPS
    if service_name == "k8s_dashboard":
        proto = "https"
    else:
        proto = request.headers.get("X-Forwarded-Proto", "http")

    # Build the URL
    if port == 80:
        url = f"{proto}://{base_host}"
    else:
        url = f"{proto}://{base_host}:{port}"

    if path:
        url = f"{url}{path}"

    return url


def get_all_service_urls() -> dict:
    """
    Get URLs for all services, adjusted for the current request context.

    Returns:
        Dict mapping service names to their full URLs
    """
    rustfs_console = get_service_url("rustfs_console", "/rustfs/console/")
    rustfs_api = get_service_url("rustfs_api")
    return {
        "flink": get_service_url("flink"),
        "flink_ui": get_service_url("flink", "/#/overview"),
        # RustFS console UI (release binary embeds assets at /rustfs/console/)
        "rustfs_console": rustfs_console,
        "rustfs_api": rustfs_api,
        "minio_console": rustfs_console,  # template alias
        "minio_api": rustfs_api,          # template alias
        "polaris_api": get_service_url("polaris_api"),
        "polaris_admin": get_service_url("polaris_admin"),
        "nifi": get_service_url("nifi", "/nifi"),
        "prometheus": get_service_url("prometheus"),
        "iceberg_browser": get_service_url("iceberg_browser"),
        "viz": get_service_url("viz"),
        "dask": get_service_url("dask"),
        "jupyterhub": get_service_url("jupyterhub"),
        "k8s_dashboard": get_service_url("k8s_dashboard"),
    }


@app.context_processor
def inject_service_urls():
    """Make service URLs available to all templates."""
    return {
        "service_urls": get_all_service_urls(),
        "get_service_url": get_service_url,
    }

# Iceberg catalog configuration - using REST catalog to connect to Polaris.
# Defaults match devenv RustFS (admin/admin) + cyberphy catalog; env overrides.
def _catalog_config() -> dict:
    return {
        "type": "rest",
        "uri": os.environ.get("POLARIS_URI", "http://localhost:8181/api/catalog"),
        "credential": os.environ.get("POLARIS_CREDENTIAL", "admin:admin"),
        "scope": "PRINCIPAL_ROLE:ALL",
        "warehouse": os.environ.get("POLARIS_CATALOG_NAME", "cyberphy"),
        "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:9010"),
        "s3.region": os.environ.get("S3_REGION", "us-east-1"),
        "s3.path-style-access": "true",
        "s3.access-key-id": os.environ.get(
            "AWS_ACCESS_KEY_ID",
            os.environ.get("RUSTFS_ACCESS_KEY", os.environ.get("MINIO_ACCESS_KEY", "admin")),
        ),
        "s3.secret-access-key": os.environ.get(
            "AWS_SECRET_ACCESS_KEY",
            os.environ.get("RUSTFS_SECRET_KEY", os.environ.get("MINIO_SECRET_KEY", "admin")),
        ),
    }


CATALOG_CONFIG = _catalog_config()

# Global catalog instance (singleton to avoid re-initialization)
_catalog = None


def get_catalog():
    """Get Iceberg catalog instance"""
    global _catalog, CATALOG_CONFIG
    if _catalog is None:
        CATALOG_CONFIG = _catalog_config()
        warehouse = CATALOG_CONFIG.get("warehouse", "cyberphy")
        _catalog = load_catalog(warehouse, **CATALOG_CONFIG)
    return _catalog


def get_table():
    """Load the preferred CloudTrail table from the catalog.

    Preference order matches the live datagen path:
      1. cyberphy.cloudtrail_events  (Polaris warehouse default)
      2. cybersec.cloudtrail_events  (legacy catalog name)
      3. default.cloudtrail_events
      4. first table found in any namespace

    Distinguishes missing-table from broken warehouse (e.g. S3 bucket gone /
    stale metadata) so API handlers can surface a useful error.
    """
    try:
        catalog = get_catalog()
        candidates = [
            "cyberphy.cloudtrail_events",
            "cybersec.cloudtrail_events",
            "default.cloudtrail_events",
        ]
        last_err = None
        for ident in candidates:
            try:
                return catalog.load_table(ident)
            except NoSuchTableError as e:
                last_err = e
                continue
            except Exception as e:
                # Table is registered but unreadable (missing S3 object, bad bucket…)
                last_err = e
                print(f"Error loading table {ident}: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
                # Keep trying other candidates; if none load, fall through.
                continue

        namespaces = catalog.list_namespaces()
        for namespace in namespaces:
            tables = catalog.list_tables(namespace)
            for table_id in tables:
                try:
                    return catalog.load_table(table_id)
                except Exception as e:
                    last_err = e
                    print(f"Error loading table {table_id}: {type(e).__name__}: {e}")
                    continue

        if last_err is not None:
            print(f"get_table: no loadable table (last error: {last_err})")
        return None
    except Exception as e:
        print(f"Error loading table: {e}")
        import traceback
        traceback.print_exc()
        return None


@app.route("/")
def index():
    """Main page"""
    return render_template("index.html")


@app.route("/api/service-urls")
def api_service_urls():
    """
    Get service URLs adjusted for the current request context.

    When accessed via localhost, returns localhost URLs.
    When accessed via FQDN (e.g., through Cloudflare WARP), returns FQDN URLs.

    Returns:
        JSON object with service names mapped to their URLs
    """
    return jsonify({
        "base_host": get_base_host(),
        "services": get_all_service_urls(),
        "ports": SERVICE_PORTS,
    })


@app.route("/api/services/health")
def api_services_health():
    """Server-side probes for hub service-link status indicators.

    Probes localhost ports so the browser does not hit CORS/NodePort issues.
    Accepts 2xx/3xx/401/403 as "up" (auth walls still mean the service is live).
    """
    import urllib.error
    import urllib.request

    results = {}
    for name, path_specs in SERVICE_HEALTH_PROBES.items():
        port = SERVICE_PORTS.get(name)
        if not port:
            results[name] = {"up": False, "code": 0, "error": "unknown port"}
            continue
        scheme = "https" if name == "k8s_dashboard" else "http"
        up = False
        code = 0
        err = None
        for path, accept in path_specs:
            url = f"{scheme}://127.0.0.1:{port}{path}"
            try:
                req = urllib.request.Request(url, method="GET")
                open_kw = {"timeout": 2}
                # Dashboard may use self-signed cert
                if scheme == "https":
                    import ssl
                    open_kw["context"] = ssl._create_unverified_context()
                with urllib.request.urlopen(req, **open_kw) as resp:
                    code = getattr(resp, "status", 200) or 200
                    if code in accept:
                        up = True
                        break
            except urllib.error.HTTPError as e:
                code = e.code
                if code in accept:
                    up = True
                    break
                err = f"HTTP {code}"
            except Exception as e:
                err = type(e).__name__
                code = 0
        results[name] = {"up": up, "code": code, "error": None if up else err}

    # Aliases so data-service="minio_console" and "rustfs_console" both work
    if "rustfs_console" in results and "minio_console" in results:
        results["minio_console"] = results["rustfs_console"]
    if "rustfs_api" in results and "minio_api" in results:
        results["minio_api"] = results["rustfs_api"]

    return jsonify({
        "services": results,
        "ts": datetime.utcnow().isoformat() + "Z",
    })


@app.route("/api/tables")
def list_tables():
    """List all tables in the catalog"""
    try:
        catalog = get_catalog()
        namespaces = catalog.list_namespaces()
        
        tables = []
        for namespace in namespaces:
            namespace_str = ".".join(namespace)
            table_list = catalog.list_tables(namespace_str)
            for table_id in table_list:
                tables.append({
                    "namespace": namespace_str,
                    "name": table_id[1] if isinstance(table_id, tuple) else str(table_id),
                    "full_name": f"{namespace_str}.{table_id[1] if isinstance(table_id, tuple) else str(table_id)}"
                })
        
        return jsonify({"tables": tables})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/schema")
def get_schema():
    """Get table schema"""
    try:
        table = get_table()
        if not table:
            return jsonify({"error": "Table not found"}), 404
        
        schema = table.schema()
        fields = []
        for field in schema.fields:
            fields.append({
                "id": field.field_id,
                "name": field.name,
                "type": str(field.field_type),
                "required": field.required,
            })
        
        return jsonify({
            "schema_id": schema.schema_id,
            "fields": fields
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stats")
def get_stats():
    """Get table statistics"""
    try:
        table = get_table()
        if not table:
            return jsonify({"error": "Table not found"}), 404
        
        # Get metadata
        metadata = table.metadata
        snapshots = metadata.snapshots
        
        # Get actual table identifier - handle tuple format
        try:
            if isinstance(table.identifier, tuple):
                table_name = ".".join(table.identifier)
            else:
                table_name = str(table.identifier)
        except:
            table_name = "unknown"
        
        stats = {
            "table_name": table_name,
            "format_version": metadata.format_version,
            "location": metadata.location,
            "snapshot_count": len(snapshots),
            "current_snapshot_id": metadata.current_snapshot_id,
        }
        
        if snapshots:
            current_snapshot = metadata.snapshot_by_id(metadata.current_snapshot_id)
            if current_snapshot:
                stats["last_updated"] = datetime.fromtimestamp(
                    current_snapshot.timestamp_ms / 1000
                ).isoformat()
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/events")
def get_events():
    """Query CloudTrail events with optional filters"""
    try:
        table = get_table()
        if not table:
            return jsonify({"error": "Table not found"}), 404
        
        # Parse query parameters
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
        event_name = request.args.get("event_name")
        user_identity = request.args.get("user_identity")
        source_ip = request.args.get("source_ip")
        region = request.args.get("region")
        event_source = request.args.get("event_source")
        
        # Cap scan size to keep memory bounded on large tables.
        MAX_SCAN_ROWS = 10000
        scan = table.scan(limit=MAX_SCAN_ROWS)

        # Flink + PyIceberg writers can diverge on optional types (e.g. read_only as
        # string vs boolean). Prefer a full to_pandas(); on schema-promotion failure,
        # fall back to column-selected Arrow projection without the conflict-prone
        # fields, then coerce remaining columns.
        try:
            df = scan.to_pandas()
        except Exception as scan_err:
            print(f"/api/events scan.to_pandas failed ({scan_err}); using resilient path")
            field_names = [f.name for f in table.schema().fields]
            # Drop booleans that commonly conflict with Flink string-encoded flags
            safe_fields = [
                n for n in field_names
                if n not in ("read_only",)  # re-derived below if needed
            ]
            try:
                arrow = table.scan(limit=MAX_SCAN_ROWS, selected_fields=tuple(safe_fields)).to_arrow()
                df = arrow.to_pandas()
            except Exception as e2:
                # Last resort: raw file sample via metadata only
                return jsonify({
                    "error": f"Cannot read table rows: {scan_err}; fallback also failed: {e2}",
                    "hint": "Stale or mixed-schema data files — recreate table or wait for "
                            "datagen to rewrite with a consistent schema",
                }), 500
            if "read_only" not in df.columns:
                df["read_only"] = None

        # Parse event_data JSON first if it exists (needed for filtering)
        if "event_data" in df.columns:
            def parse_event_data(row):
                try:
                    if row and isinstance(row, str):
                        return json.loads(row)
                    return {}
                except:
                    return {}

            parsed = df["event_data"].apply(parse_event_data)

            # Extract common fields for filtering
            df["event_name"] = parsed.apply(lambda x: x.get("eventName", ""))
            df["event_source"] = parsed.apply(lambda x: x.get("eventSource", ""))
            df["region"] = parsed.apply(lambda x: x.get("awsRegion", ""))
            df["source_ip_address"] = parsed.apply(lambda x: x.get("sourceIPAddress", ""))
            df["user_identity"] = parsed.apply(lambda x:
                x.get("userIdentity", {}).get("userName", "") or
                x.get("userIdentity", {}).get("arn", "").split("/")[-1] if x.get("userIdentity") else ""
            )

        # Apply filters (now works with extracted fields)
        if event_name:
            df = df[df["event_name"].str.contains(event_name, case=False, na=False)]
        if event_source:
            df = df[df["event_source"].str.contains(event_source, case=False, na=False)]
        if user_identity:
            df = df[df["user_identity"].str.contains(user_identity, case=False, na=False)]
        if source_ip:
            df = df[df["source_ip_address"].str.contains(source_ip, case=False, na=False)]
        if region:
            df = df[df["region"].str.contains(region, case=False, na=False)]

        # Get total count before pagination
        total_count = len(df)

        # Sort by timestamp descending (check multiple possible column names)
        for ts_col in ["event_time", "event_timestamp", "eventTime", "processing_time"]:
            if ts_col in df.columns:
                df = df.sort_values(ts_col, ascending=False)
                break

        # Apply pagination
        df = df.iloc[offset:offset + limit]
        
        # Convert to records
        events = df.to_dict(orient="records")

        # Process events: expand event_data JSON and handle types
        processed_events = []
        for event in events:
            # Parse and merge full event_data JSON for frontend
            if "event_data" in event and event["event_data"]:
                try:
                    event_data_str = event["event_data"]
                    if isinstance(event_data_str, str):
                        event_data = json.loads(event_data_str)
                        # Extract additional fields not already extracted
                        if "errorCode" in event_data:
                            event["error_code"] = event_data["errorCode"]
                        if "errorMessage" in event_data:
                            event["error_message"] = event_data["errorMessage"]
                        # Merge all fields from event_data
                        event.update(event_data)
                except Exception as e:
                    print(f"Error parsing event_data: {e}")
            
            # Convert any datetime objects to ISO format and handle numpy types
            for key, value in list(event.items()):
                if isinstance(value, pd.Timestamp):
                    event[key] = value.isoformat()
                elif isinstance(value, np.ndarray):
                    # Convert numpy arrays to Python lists
                    event[key] = value.tolist() if value.size > 0 else None
                elif isinstance(value, dict):
                    # Keep dicts as-is (they're JSON serializable)
                    pass
                elif isinstance(value, (list, tuple)):
                    # Keep lists/tuples as-is
                    pass
                elif value is None:
                    pass
                elif not isinstance(value, (str, int, float, bool)):
                    # For other non-standard types, check if scalar NA
                    try:
                        if pd.isna(value):
                            event[key] = None
                    except (TypeError, ValueError):
                        # pd.isna fails on arrays - leave value as-is
                        pass

            processed_events.append(event)
            
        events = processed_events
        
        return jsonify({
            "events": events,
            "total": total_count,
            "limit": limit,
            "offset": offset,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/event/<event_id>")
def get_event_detail(event_id):
    """Get detailed information for a specific event"""
    try:
        table = get_table()
        if not table:
            return jsonify({"error": "Table not found"}), 404

        # Use a reasonable limit - event IDs should be unique so we just need to find it
        # Note: Ideally we'd use row_filter but PyIceberg doesn't support complex filters well
        scan = table.scan(limit=50000)
        df = scan.to_pandas()

        # Find event by ID - check both possible column names
        event_df = None
        if "event_id" in df.columns:
            event_df = df[df["event_id"] == event_id]
        elif "eventID" in df.columns:
            event_df = df[df["eventID"] == event_id]
        else:
            # List available columns for debugging
            return jsonify({"error": f"No event_id column found. Available columns: {list(df.columns)}"}), 500

        if event_df.empty:
            return jsonify({"error": "Event not found"}), 404
        
        event = event_df.iloc[0].to_dict()
        
        # Parse event_data JSON if it exists
        if "event_data" in event and event["event_data"]:
            try:
                event_data_str = event["event_data"]
                if isinstance(event_data_str, str):
                    event_data = json.loads(event_data_str)
                    
                    # Flatten common fields for frontend
                    if "eventName" in event_data:
                        event["event_name"] = event_data["eventName"]
                    if "userIdentity" in event_data:
                        userIdentity = event_data["userIdentity"]
                        if isinstance(userIdentity, dict):
                            if "userName" in userIdentity:
                                event["user_identity"] = userIdentity["userName"]
                            elif "arn" in userIdentity:
                                event["user_identity"] = userIdentity["arn"].split("/")[-1]
                    if "sourceIPAddress" in event_data:
                        event["source_ip_address"] = event_data["sourceIPAddress"]
                    if "awsRegion" in event_data:
                        event["region"] = event_data["awsRegion"]
                        
                    # Merge the rest
                    event.update(event_data)
            except Exception as e:
                print(f"Error parsing event_data: {e}")
        
        # Convert datetime objects and handle numpy types
        for key, value in list(event.items()):
            if isinstance(value, pd.Timestamp):
                event[key] = value.isoformat()
            elif isinstance(value, np.ndarray):
                # Convert numpy arrays to Python lists
                event[key] = value.tolist() if value.size > 0 else None
            elif isinstance(value, dict):
                # Keep dicts as-is (they're JSON serializable)
                pass
            elif isinstance(value, (list, tuple)):
                # Keep lists/tuples as-is
                pass
            elif value is None:
                pass
            elif not isinstance(value, (str, int, float, bool)):
                # For other non-standard types, check if scalar NA
                try:
                    if pd.isna(value):
                        event[key] = None
                except (TypeError, ValueError):
                    # pd.isna fails on arrays - leave value as-is
                    pass

        return jsonify(event)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/summary")
def get_summary():
    """Get summary statistics from snapshot metadata (fast, no full scan)"""
    try:
        table = get_table()
        if not table:
            return jsonify({"error": "Table not found"}), 404

        metadata = table.metadata
        snapshots = metadata.snapshots

        if not snapshots:
            return jsonify({"total_events": 0})

        # Get counts from latest snapshot summary (no scan needed!)
        latest = snapshots[-1]
        summary_props = latest.summary.additional_properties if hasattr(latest.summary, 'additional_properties') else {}

        total_records = int(summary_props.get('total-records', 0))

        summary = {
            "total_events": total_records,
        }

        # Get time range from first and last snapshots
        if len(snapshots) > 0:
            first_snapshot = snapshots[0]
            summary["earliest_event"] = datetime.fromtimestamp(first_snapshot.timestamp_ms / 1000).isoformat()
            summary["latest_event"] = datetime.fromtimestamp(latest.timestamp_ms / 1000).isoformat()

        return jsonify(summary)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ============================================================================
# Mock Visualization Inference Service
# ============================================================================
# Simulates an ML/LLM agent that recommends visualizations for event cards.
# Future: Replace with real inference service (local LLM, API call, etc.)
#
# Widget Taxonomy (best practices):
# - TIME SERIES: sparkline, horizon (layered bands)
# - PROPORTION: ring (donut), bullet (vs target)
# - STATUS: threat_score, deviation (from baseline)
# - CATEGORICAL: mini_bar, heatrow
# - METADATA: geo_badge, error_pulse, trend_arrow
#
# Anti-patterns we avoid: pie charts, 3D, dual Y-axis

VIZ_TYPES = {
    # Time series
    "sparkline": "Area chart for event timing distribution",
    "horizon": "Layered bands for dense time series, anomaly detection",
    # Proportion
    "ring": "Part-of-whole with center KPI (max 5-6 segments)",
    "bullet": "Actual vs target with qualitative ranges",
    # Status
    "threat_score": "Security risk indicator (0-100 scale)",
    "deviation": "Diverging bar from baseline for anomalies",
    # Categorical
    "mini_bar": "Horizontal bars for top-N comparison",
    "heatrow": "Sequential intensity for patterns",
    # Metadata
    "geo_badge": "Geographic region indicator",
    "error_pulse": "Animated error status",
    "trend_arrow": "Direction indicator (up/down/flat)",
    "none": "No visualization needed",
}


def mock_viz_inference(event_group: dict) -> dict:
    """
    Mock inference function that returns visualization recommendations.

    In production, this would call an ML model or LLM to analyze the event
    and recommend appropriate visualizations. The model would consider:
    - Data shape (time series, categorical, proportional)
    - Context (security event, metric, status)
    - User intent (monitoring, investigation, reporting)
    - Visual salience (what needs attention)

    Args:
        event_group: A grouped event dict with count, events list, etc.

    Returns:
        Dict with viz_type, confidence, params, and reason
    """
    import random

    count = event_group.get("count", 1)
    event_name = event_group.get("event_name", "")
    has_error = bool(event_group.get("error_code"))
    region = event_group.get("region", "")
    events = event_group.get("events", [])

    # === PATTERN: High volume grouped events ===
    # Best viz: sparkline (shows timing distribution) or horizon (shows intensity)
    if count >= 5:
        # Use horizon for very high volume (shows layered intensity)
        if count >= 10:
            return {
                "viz_type": "horizon",
                "confidence": 0.92,
                "params": {"bands": 3},
                "reason": f"High density pattern ({count} events)",
            }
        return {
            "viz_type": "sparkline",
            "confidence": 0.9,
            "reason": f"Event timing distribution ({count} events)",
        }

    # === PATTERN: Error events ===
    # Best viz: threat_score with severity, or deviation from normal
    if has_error:
        threat_keywords = ["Delete", "Terminate", "Remove", "Revoke", "Detach"]
        base_score = 55 if any(k in event_name for k in threat_keywords) else 25
        score = min(95, base_score + (count * 8))
        return {
            "viz_type": "threat_score",
            "confidence": 0.88,
            "score": score,
            "reason": f"Error: {event_group.get('error_code')}",
        }

    # === PATTERN: Sensitive/privileged operations ===
    # Best viz: threat_score (risk awareness) or ring (% of sensitive ops)
    sensitive_ops = {
        "CreateAccessKey": 50, "DeleteAccessKey": 45,
        "AttachUserPolicy": 55, "DetachUserPolicy": 40,
        "ConsoleLogin": 35, "AssumeRole": 30,
        "PutBucketPolicy": 50, "DeleteBucket": 60,
        "CreateUser": 40, "DeleteUser": 55,
        "TerminateInstances": 65, "RunInstances": 25,
    }
    if event_name in sensitive_ops:
        base_score = sensitive_ops[event_name]
        score = min(90, base_score + (count * 5))
        return {
            "viz_type": "threat_score",
            "confidence": 0.82,
            "score": score,
            "reason": f"Sensitive: {event_name}",
        }

    # === PATTERN: Read-only operations with volume ===
    # Best viz: ring showing proportion of total, or trend arrow
    read_ops = ["GetObject", "DescribeInstances", "ListBuckets", "GetUser"]
    if event_name in read_ops and count >= 2:
        # Show as proportion of activity
        return {
            "viz_type": "ring",
            "confidence": 0.7,
            "params": {"value": count, "max": max(10, count * 2), "label": "reads"},
            "reason": f"Read pattern: {event_name}",
        }

    # === PATTERN: Regional activity ===
    # Best viz: geo_badge for single region, heatrow for multi-region
    if region and count >= 2:
        return {
            "viz_type": "geo_badge",
            "confidence": 0.65,
            "region": region,
            "reason": "Regional pattern",
        }

    # === PATTERN: Standard single event ===
    # Usually no viz needed, but occasionally show trend
    if count == 1 and random.random() < 0.15:
        # Occasionally show a trend arrow for variety
        directions = ["up", "flat", "down"]
        return {
            "viz_type": "trend_arrow",
            "confidence": 0.5,
            "params": {"direction": random.choice(directions), "magnitude": "normal"},
            "reason": "Activity trend",
        }

    # Default: no visualization
    return {
        "viz_type": "none",
        "confidence": 0.5,
        "reason": "Standard event",
    }


@app.route("/api/viz/infer", methods=["POST"])
def viz_infer():
    """
    Inference endpoint for card visualizations.

    Accepts a list of event groups and returns visualization recommendations.
    Future: This endpoint signature stays the same when swapping to real inference.
    """
    try:
        data = request.get_json()
        event_groups = data.get("events", [])

        recommendations = []
        for group in event_groups:
            rec = mock_viz_inference(group)
            rec["event_id"] = group.get("event_id") or group.get("eventID") or group.get("id")
            recommendations.append(rec)

        return jsonify({
            "recommendations": recommendations,
            "model": "mock-rules-v1",  # Future: "llama-3-8b", "gpt-4-turbo", etc.
            "latency_ms": 5,  # Mock latency
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/fsn/aggregate")
def fsn_aggregate():
    """
    Pivot-style aggregation for FSN 3D visualization.

    Returns grid data: region × event_source with metrics.
    Supports partition pruning via time_start/time_end filters.
    """
    try:
        table = get_table()
        if not table:
            return jsonify({"error": "Table not found"}), 404

        time_start = request.args.get("time_start")
        time_end = request.args.get("time_end")

        # Scan table with limit to prevent memory issues on large tables
        # For FSN visualization, we sample recent data to show patterns
        FSN_SCAN_LIMIT = 50000  # Enough to show meaningful patterns
        scan = table.scan(limit=FSN_SCAN_LIMIT)
        df = scan.to_pandas()

        if df.empty:
            return jsonify({
                "data": [],
                "x_axis": "aws_region",
                "z_axis": "event_source",
                "regions": [],
                "services": []
            })

        # Parse event_data JSON if it exists (raw table may only have event_data column)
        if "event_data" in df.columns:
            parsed_fields = []
            for _, row in df.iterrows():
                try:
                    if row["event_data"] and isinstance(row["event_data"], str):
                        event = json.loads(row["event_data"])
                        parsed_fields.append({
                            "awsRegion": event.get("awsRegion", ""),
                            "eventSource": event.get("eventSource", ""),
                            "eventName": event.get("eventName", ""),
                            "errorCode": event.get("errorCode")
                        })
                    else:
                        parsed_fields.append({})
                except:
                    parsed_fields.append({})

            if parsed_fields:
                parsed_df = pd.DataFrame(parsed_fields)
                for col in parsed_df.columns:
                    if col not in df.columns:
                        df[col] = parsed_df[col]

        # Apply time filters if provided
        ts_col = None
        for col in ["event_timestamp", "event_time", "processing_time"]:
            if col in df.columns:
                ts_col = col
                break

        if ts_col and time_start:
            df = df[df[ts_col] >= pd.to_datetime(time_start)]
        if ts_col and time_end:
            df = df[df[ts_col] <= pd.to_datetime(time_end)]

        # Determine region column (check both snake_case and camelCase)
        region_col = None
        for col in ["aws_region", "awsRegion", "region"]:
            if col in df.columns:
                region_col = col
                break

        # Determine service column
        service_col = None
        for col in ["event_source", "eventSource"]:
            if col in df.columns:
                service_col = col
                break

        # Determine event name column for fallback
        event_name_col = None
        for col in ["event_name", "eventName"]:
            if col in df.columns:
                event_name_col = col
                break

        if not region_col or not service_col:
            # Fallback to event_name grouping if no region/service columns
            if event_name_col:
                pivot = df.groupby(event_name_col).agg(
                    count=(event_name_col, "count")
                ).reset_index()
                pivot = pivot.rename(columns={event_name_col: "event_name"})
                pivot["error_rate"] = 0
                return jsonify({
                    "data": pivot.to_dict(orient="records"),
                    "x_axis": "event_name",
                    "z_axis": None,
                    "fallback": True
                })
            return jsonify({"error": "Required columns not found", "columns": list(df.columns)}), 400

        # Aggregate by region and event_source
        agg_dict = {
            "count": (region_col, "count"),
        }

        # Check for error column
        error_col = None
        for col in ["error_code", "errorCode"]:
            if col in df.columns:
                error_col = col
                break

        pivot = df.groupby([region_col, service_col]).agg(**agg_dict).reset_index()

        # Calculate error rate if we have error data
        if error_col:
            error_counts = df.groupby([region_col, service_col])[error_col].apply(
                lambda x: x.notna().sum()
            ).reset_index(name="error_count")
            pivot = pivot.merge(error_counts, on=[region_col, service_col], how="left")
            pivot["error_rate"] = pivot["error_count"] / pivot["count"]
            pivot["error_rate"] = pivot["error_rate"].fillna(0)
        else:
            pivot["error_rate"] = 0

        # Get unique values for axis labels
        regions = sorted(df[region_col].dropna().unique().tolist())
        services = sorted(df[service_col].dropna().unique().tolist())

        # Rename columns for consistency
        pivot = pivot.rename(columns={region_col: "aws_region", service_col: "event_source"})

        return jsonify({
            "data": pivot.to_dict(orient="records"),
            "x_axis": "aws_region",
            "z_axis": "event_source",
            "regions": regions,
            "services": services
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/fsn/drilldown")
def fsn_drilldown():
    """
    Get event_names within a region+service cell for drill-down.
    """
    try:
        table = get_table()
        if not table:
            return jsonify({"error": "Table not found"}), 404

        region = request.args.get("region")
        service = request.args.get("service")

        if not region or not service:
            return jsonify({"error": "region and service parameters required"}), 400

        # Scan with limit to prevent memory issues
        DRILLDOWN_LIMIT = 50000
        scan = table.scan(limit=DRILLDOWN_LIMIT)
        df = scan.to_pandas()

        # Parse event_data JSON if needed
        if "event_data" in df.columns:
            parsed_fields = []
            for _, row in df.iterrows():
                try:
                    if row["event_data"] and isinstance(row["event_data"], str):
                        event = json.loads(row["event_data"])
                        parsed_fields.append({
                            "awsRegion": event.get("awsRegion", ""),
                            "eventSource": event.get("eventSource", ""),
                            "eventName": event.get("eventName", "")
                        })
                    else:
                        parsed_fields.append({})
                except:
                    parsed_fields.append({})

            if parsed_fields:
                parsed_df = pd.DataFrame(parsed_fields)
                for col in parsed_df.columns:
                    if col not in df.columns:
                        df[col] = parsed_df[col]

        # Determine column names (check both snake_case and camelCase)
        region_col = None
        for col in ["aws_region", "awsRegion", "region"]:
            if col in df.columns:
                region_col = col
                break

        service_col = None
        for col in ["event_source", "eventSource"]:
            if col in df.columns:
                service_col = col
                break

        event_name_col = None
        for col in ["event_name", "eventName"]:
            if col in df.columns:
                event_name_col = col
                break

        if not region_col or not service_col:
            return jsonify({"error": "Required columns not found"}), 400

        # Filter to selected region and service
        filtered = df[(df[region_col] == region) & (df[service_col] == service)]

        if filtered.empty:
            return jsonify({
                "events": [],
                "region": region,
                "service": service,
                "total": 0
            })

        # Aggregate by event_name
        if event_name_col and event_name_col in filtered.columns:
            events = filtered.groupby(event_name_col).agg(
                count=(event_name_col, "count")
            ).reset_index()
            events = events.rename(columns={event_name_col: "event_name"})
            events = events.to_dict(orient="records")
        else:
            events = [{"event_name": "Unknown", "count": len(filtered)}]

        return jsonify({
            "events": events,
            "region": region,
            "service": service,
            "total": len(filtered)
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# Store metrics for change detection and rate calculation
_last_snapshot_id = None
_last_event_count = None  # None = not initialized yet
_event_timestamps = []  # Track event times for rate calculation


def get_table_changes():
    """Check if table has changed and calculate events per minute"""
    global _last_snapshot_id, _last_event_count, _event_timestamps

    try:
        table = get_table()
        if not table:
            return None

        metadata = table.metadata
        current_snapshot_id = metadata.current_snapshot_id

        # Get current count from snapshot metadata (no scan needed!)
        current_count = 0
        if metadata.snapshots:
            latest = metadata.snapshots[-1]
            summary_props = latest.summary.additional_properties if hasattr(latest.summary, 'additional_properties') else {}
            current_count = int(summary_props.get('total-records', 0))

        # First call - initialize baseline (don't count existing events as "new")
        if _last_event_count is None:
            _last_event_count = current_count
            _last_snapshot_id = current_snapshot_id
            return {
                "total_events": current_count,
                "events_per_minute": 0,
                "timestamp": datetime.now().isoformat()
            }

        # Calculate new events since last check
        new_events = current_count - _last_event_count

        # Update tracking
        if new_events > 0:
            now = time.time()
            # Add timestamp for each new event
            _event_timestamps.extend([now] * new_events)
            _last_event_count = current_count
            _last_snapshot_id = current_snapshot_id

        # Calculate events per minute (last 60 seconds)
        now = time.time()
        cutoff = now - 60
        _event_timestamps[:] = [ts for ts in _event_timestamps if ts > cutoff]
        events_per_minute = len(_event_timestamps)

        return {
            "total_events": current_count,
            "events_per_minute": events_per_minute,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"error": str(e)}


@app.route("/api/stream")
def stream_updates():
    """Server-Sent Events endpoint for real-time updates"""
    def generate():
        # Send initial connection message
        yield f"data: {json.dumps({'type': 'connected', 'message': 'Connected to live updates'})}\n\n"
        
        while True:
            try:
                # Check for changes every 2 seconds
                changes = get_table_changes()
                
                if changes:
                    # Send metrics update (total events and events per minute)
                    yield f"data: {json.dumps({'type': 'metrics', 'data': changes})}\n\n"
                
                time.sleep(2)
            except GeneratorExit:
                break
            except Exception as e:
                error_data = {'type': 'error', 'message': str(e)}
                yield f"data: {json.dumps(error_data)}\n\n"
                time.sleep(5)
    
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive'
        }
    )



@app.route("/polaris")
def polaris_page():
    """Polaris Insights page"""
    return render_template("polaris.html")


# ============================================================================
# Bootstrap / Settings Routes
# ============================================================================


@app.route("/settings")
def settings_page():
    """Bootstrap settings page"""
    return render_template("settings.html")


# ============================================================================
# K8s Dashboard Token API
# ============================================================================


@app.route("/api/k8s-dashboard-token")
def k8s_dashboard_token():
    """Generate a fresh K8s dashboard bearer token."""
    try:
        # Resolve kubeconfig: .env override > ~/.kube/rke2.yaml > KUBECONFIG env > devenv default
        kubeconfig = None
        env_file = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.isfile(env_file):
            with open(env_file) as f:
                for line in f:
                    if line.startswith("KUBECONFIG="):
                        candidate = line.strip().split("=", 1)[1]
                        if os.path.isfile(candidate):
                            kubeconfig = candidate
                            break
        if not kubeconfig:
            for candidate in [
                os.path.expanduser("~/.kube/rke2.yaml"),
                os.environ.get("KUBECONFIG", ""),
            ]:
                if candidate and os.path.isfile(candidate):
                    kubeconfig = candidate
                    break

        env = dict(os.environ)
        if kubeconfig:
            env["KUBECONFIG"] = kubeconfig

        result = subprocess.run(
            ["kubectl", "-n", "kubernetes-dashboard", "create", "token", "admin-user"],
            capture_output=True, text=True, timeout=10, env=env,
        )
        if result.returncode == 0 and result.stdout.strip():
            return jsonify({"token": result.stdout.strip()})
        return jsonify({"error": "Could not generate token. Is the dashboard deployed?"}), 503
    except FileNotFoundError:
        return jsonify({"error": "kubectl not found"}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================================
# Theme API
# ============================================================================


@app.route("/api/themes")
def api_themes():
    """List available themes."""
    try:
        from cybersec.themes import list_themes
        return jsonify(list_themes())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/themes/<theme_id>")
def api_theme(theme_id):
    """Get theme CSS variables and THREE.js colors."""
    try:
        from cybersec.themes import get_theme
        return jsonify(get_theme(theme_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ui/theme", methods=["GET", "POST"])
def api_ui_theme():
    """Get or set current UI theme preference."""
    try:
        from cybersec.bootstrap import BootstrapService
        service = BootstrapService()

        if request.method == "POST":
            data = request.json or {}
            theme_id = data.get("theme", "nord")
            service.update_config(ui_theme=theme_id)
            return jsonify({"theme": theme_id})

        config = service.get_config()
        return jsonify({"theme": config.ui_theme})
    except Exception as e:
        return jsonify({"error": str(e), "theme": "nord"}), 500


@app.route("/api/fsn/settings", methods=["GET", "POST"])
def api_fsn_settings():
    """Get or set FSN visualization settings."""
    try:
        from cybersec.bootstrap import BootstrapService
        service = BootstrapService()

        if request.method == "POST":
            data = request.json or {}
            updates = {}

            if "default_mode" in data:
                updates["fsn_default_mode"] = data["default_mode"]
            if "remember_mode" in data:
                updates["fsn_remember_mode"] = data["remember_mode"]
            if "iceberg_auto_refresh" in data:
                updates["fsn_iceberg_auto_refresh"] = data["iceberg_auto_refresh"]
            if "iceberg_refresh_interval" in data:
                updates["fsn_iceberg_refresh_interval"] = data["iceberg_refresh_interval"]

            if updates:
                service.update_config(**updates)

            config = service.get_config()
            return jsonify({
                "default_mode": config.fsn_default_mode,
                "remember_mode": config.fsn_remember_mode,
                "iceberg_auto_refresh": config.fsn_iceberg_auto_refresh,
                "iceberg_refresh_interval": config.fsn_iceberg_refresh_interval,
            })

        config = service.get_config()
        return jsonify({
            "default_mode": config.fsn_default_mode,
            "remember_mode": config.fsn_remember_mode,
            "iceberg_auto_refresh": config.fsn_iceberg_auto_refresh,
            "iceberg_refresh_interval": config.fsn_iceberg_refresh_interval,
        })
    except Exception as e:
        return jsonify({
            "error": str(e),
            "default_mode": "cloudtrail",
            "remember_mode": True,
            "iceberg_auto_refresh": False,
            "iceberg_refresh_interval": 60,
        }), 500


@app.route("/api/bootstrap/info")
def bootstrap_info():
    """Get bootstrap configuration and status"""
    try:
        from cybersec.bootstrap import BootstrapService
        service = BootstrapService()
        config = service.get_config()

        return jsonify({
            "config_file": str(service.settings.config_path),
            "config_exists": service.settings.exists(),
            "bootstrap_completed": config.completed,
            "last_run": config.last_run,
            "paths": {
                "flink_home": str(config.get_flink_home()) if config.get_flink_home() else None,
                "flink_state": str(config.get_flink_state_dir()),
                "minio_data": str(config.get_minio_data_dir()),
                "rustfs_data": str(config.get_minio_data_dir()),
                "log_dir": str(config.get_log_dir()),
            },
            "services": {
                "postgres": {"host": config.postgres_host, "port": config.postgres_port},
                "polaris": {"api_url": config.polaris_api_url, "admin_url": config.polaris_admin_url},
                "flink": {"url": config.flink_url},
                "rustfs": {"endpoint": config.minio_endpoint, "console": config.minio_console},
                "minio": {"endpoint": config.minio_endpoint, "console": config.minio_console},  # alias
                "iceberg_browser": {"port": config.iceberg_browser_port},
            },
            "catalog": {
                "name": config.catalog_name,
                "warehouse": config.catalog_warehouse,
            },
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/bootstrap/status")
def bootstrap_status():
    """Get service health status"""
    import asyncio
    try:
        from cybersec.bootstrap import BootstrapService
        service = BootstrapService()

        # Run async health checks
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            results = loop.run_until_complete(service.check_all_services())
        finally:
            loop.close()

        all_healthy = all(r.get("healthy", False) for r in results)

        return jsonify({
            "all_healthy": all_healthy,
            "services": results,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/bootstrap/settings", methods=["GET", "POST"])
def bootstrap_settings():
    """Get or update bootstrap settings"""
    try:
        from cybersec.bootstrap import BootstrapService, BootstrapConfig
        service = BootstrapService()

        if request.method == "POST":
            data = request.get_json() or {}

            if data.get("reset"):
                config = BootstrapConfig()
                service.settings.save(config)
                return jsonify({"action": "reset", "message": "Settings reset to defaults"})

            if data.get("updates"):
                service.update_config(**data["updates"])
                return jsonify({
                    "action": "updated",
                    "message": f"Updated {len(data['updates'])} setting(s)",
                })

        return jsonify({"config": service.get_config_dict()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/bootstrap/verify")
def bootstrap_verify():
    """Verify bootstrap configuration"""
    import asyncio
    try:
        from cybersec.bootstrap import BootstrapService
        service = BootstrapService()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(service.verify())
        finally:
            loop.close()

        return jsonify({
            "all_passed": result.get("all_passed", False),
            "checks": result.get("checks", []),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/bootstrap/assess")
def bootstrap_assess():
    """Quick assessment for startup check"""
    import asyncio
    try:
        from cybersec.bootstrap import BootstrapService
        service = BootstrapService()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(service.assess())
        finally:
            loop.close()

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/bootstrap/run", methods=["POST"])
def bootstrap_run():
    """Execute bootstrap (returns SSE stream)"""
    import asyncio
    from cybersec.bootstrap import BootstrapService, EventType

    data = request.get_json() or {}
    skip_flink = data.get("skip_flink", False)
    flink_path = data.get("flink_path")
    dry_run = data.get("dry_run", False)

    def generate():
        service = BootstrapService()

        async def run_bootstrap():
            async for event in service.run(
                skip_flink=skip_flink,
                flink_path=flink_path,
                dry_run=dry_run,
            ):
                event_data = {
                    "type": event.event_type.value,
                    "task_id": event.task_id,
                    "message": event.message,
                    "progress": event.progress,
                }

                # Include prompt options if present
                if event.prompt_options:
                    event_data["prompt_options"] = [
                        {"key": o.key, "label": o.label, "description": o.description, "default": o.default}
                        for o in event.prompt_options
                    ]
                    event_data["prompt_allow_custom"] = event.prompt_allow_custom

                yield f"data: {json.dumps(event_data)}\n\n"

        # Run the async generator
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # Convert async generator to sync
            async def collect_events():
                events = []
                async for event in service.run(
                    skip_flink=skip_flink,
                    flink_path=flink_path,
                    dry_run=dry_run,
                ):
                    events.append(event)
                return events

            events = loop.run_until_complete(collect_events())
            for event in events:
                event_data = {
                    "type": event.event_type.value,
                    "task_id": event.task_id,
                    "message": event.message,
                    "progress": event.progress,
                }
                if event.prompt_options:
                    event_data["prompt_options"] = [
                        {"key": o.key, "label": o.label, "description": o.description, "default": o.default}
                        for o in event.prompt_options
                    ]
                    event_data["prompt_allow_custom"] = event.prompt_allow_custom
                yield f"data: {json.dumps(event_data)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        finally:
            loop.close()

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive'
        }
    )


@app.route("/api/catalog-info")
def get_catalog_info():
    """Get catalog configuration and status"""
    try:
        catalog = get_catalog()
        
        # Get structure
        structure = {}
        namespaces = catalog.list_namespaces()
        total_tables = 0
        
        for namespace in namespaces:
            namespace_str = ".".join(namespace)
            tables = catalog.list_tables(namespace_str)
            table_names = [t[1] if isinstance(t, tuple) else str(t) for t in tables]
            structure[namespace_str] = table_names
            total_tables += len(tables)
            
        # Check health (internal probe to localhost:8182)
        import urllib.request
        health_status = "DOWN"
        try:
            with urllib.request.urlopen("http://localhost:8182/q/health", timeout=2) as response:
                if response.getcode() == 200:
                    health_data = json.loads(response.read())
                    health_status = health_data.get("status", "UNKNOWN")
        except Exception as e:
            print(f"Health check failed: {e}")
            
        return jsonify({
            "config": {
                "uri": CATALOG_CONFIG["uri"],
                "warehouse": CATALOG_CONFIG["warehouse"],
                "scope": CATALOG_CONFIG["scope"],
                "s3_endpoint": CATALOG_CONFIG["s3.endpoint"],
                "properties": catalog.properties
            },
            "status": health_status,
            "structure": structure,
            "total_tables": total_tables
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/catalog/namespace/<path:namespace>")
def get_namespace_details(namespace):
    """Get namespace properties"""
    try:
        catalog = get_catalog()
        # Create namespace tuple/string
        ns_parts = namespace.split(".")
        if len(ns_parts) == 1:
            ns_idf = ns_parts[0]
        else:
            ns_idf = tuple(ns_parts)
            
        props = catalog.load_namespace_properties(ns_idf)
        return jsonify({"namespace": namespace, "properties": props})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================================
# FSN Iceberg Optimization Mode APIs
# ============================================================================
# These endpoints support the "Iceberg Optimization" FSN mode which visualizes
# table health, partition distribution, file fragmentation, and RETE recommendations.


@app.route("/api/fsn/iceberg/tables")
def fsn_iceberg_tables():
    """Get all tables with optimization metrics for FSN visualization.

    Returns table-level health scores for catalog overview (Level 0).
    """
    try:
        catalog = get_catalog()
        namespaces = catalog.list_namespaces()

        tables = []
        summary = {"critical": 0, "warnings": 0, "healthy": 0}

        for namespace in namespaces:
            namespace_str = ".".join(namespace)
            table_list = catalog.list_tables(namespace_str)

            for table_id in table_list:
                table_name = table_id[1] if isinstance(table_id, tuple) else str(table_id)
                full_name = f"{namespace_str}.{table_name}"

                try:
                    table = catalog.load_table(full_name)
                    metrics = _gather_table_optimization_metrics(table, full_name)
                    tables.append(metrics)

                    # Update summary counts
                    if metrics["health_score"] < 0.4:
                        summary["critical"] += 1
                    elif metrics["health_score"] < 0.7:
                        summary["warnings"] += 1
                    else:
                        summary["healthy"] += 1
                except Exception as e:
                    print(f"Error loading table {full_name}: {e}")
                    continue

        return jsonify({
            "tables": tables,
            "summary": summary,
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/fsn/iceberg/partitions/<path:table_name>")
def fsn_iceberg_partitions(table_name):
    """Get per-partition metrics for drill-down visualization (Level 1).

    X-axis = partition values (dates/hours)
    Y-axis = file count per partition
    Color = health score
    """
    try:
        catalog = get_catalog()
        table = catalog.load_table(table_name)

        # Get partition information
        spec = table.spec()
        is_partitioned = not spec.is_unpartitioned()

        # Get file information from current snapshot
        current = table.current_snapshot()
        if not current:
            return jsonify({
                "table": table_name,
                "is_partitioned": is_partitioned,
                "partitions": [],
                "message": "No current snapshot"
            })

        # Gather partition-level metrics
        scan = table.scan()
        files = list(scan.plan_files())

        # Group files by partition
        partition_stats = {}

        for file_task in files:
            # Get partition value from file path or partition data
            data_file = file_task.file
            file_size = data_file.file_size_in_bytes

            # Extract partition value from file path
            # Format: s3://bucket/warehouse/table/partition=value/datafile.parquet
            path = data_file.file_path
            partition_value = _extract_partition_value(path)

            if partition_value not in partition_stats:
                partition_stats[partition_value] = {
                    "partition_value": partition_value,
                    "file_count": 0,
                    "total_size_bytes": 0,
                    "file_sizes": [],
                }

            partition_stats[partition_value]["file_count"] += 1
            partition_stats[partition_value]["total_size_bytes"] += file_size
            partition_stats[partition_value]["file_sizes"].append(file_size)

        # Calculate health scores for each partition
        partitions = []
        for pv, stats in partition_stats.items():
            avg_file_size_mb = (stats["total_size_bytes"] / max(1, stats["file_count"])) / (1024 * 1024)

            # Health score based on file count and size
            file_count_score = min(1.0, 20 / max(1, stats["file_count"]))  # Fewer files = better
            file_size_score = min(1.0, avg_file_size_mb / 128)  # Closer to 128MB = better
            health_score = (file_count_score * 0.6 + file_size_score * 0.4)

            partitions.append({
                "partition_value": pv,
                "file_count": stats["file_count"],
                "total_size_mb": round(stats["total_size_bytes"] / (1024 * 1024), 2),
                "avg_file_size_mb": round(avg_file_size_mb, 2),
                "health_score": round(health_score, 2),
                "needs_compaction": stats["file_count"] > 20 or avg_file_size_mb < 32,
            })

        # Sort by partition value
        partitions.sort(key=lambda p: p["partition_value"])

        return jsonify({
            "table": table_name,
            "is_partitioned": is_partitioned,
            "partitions": partitions,
            "total_files": len(files),
            "total_partitions": len(partitions),
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/fsn/iceberg/recommendations")
def fsn_iceberg_recommendations():
    """Get RETE recommendations for all tables.

    Returns actionable recommendations with priorities for the overlay panel.
    """
    try:
        catalog = get_catalog()
        namespaces = catalog.list_namespaces()

        all_recommendations = []

        for namespace in namespaces:
            namespace_str = ".".join(namespace)
            table_list = catalog.list_tables(namespace_str)

            for table_id in table_list:
                table_name = table_id[1] if isinstance(table_id, tuple) else str(table_id)
                full_name = f"{namespace_str}.{table_name}"

                try:
                    table = catalog.load_table(full_name)
                    recs = _get_table_recommendations(table, full_name)
                    all_recommendations.extend(recs)
                except Exception as e:
                    print(f"Error analyzing {full_name}: {e}")
                    continue

        # Sort by priority descending
        all_recommendations.sort(key=lambda r: -r["priority"])

        return jsonify({
            "recommendations": all_recommendations,
            "count": len(all_recommendations),
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/fsn/iceberg/apply", methods=["POST"])
def fsn_iceberg_apply():
    """Execute an optimization fix.

    Accepts: table, action (compact, expire, rewrite_manifests)
    """
    try:
        data = request.get_json() or {}
        table_name = data.get("table")
        action = data.get("action")
        dry_run = data.get("dry_run", False)

        if not table_name or not action:
            return jsonify({"error": "Missing required fields: table, action"}), 400

        catalog = get_catalog()
        table = catalog.load_table(table_name)

        if dry_run:
            # Return preview of what would happen
            preview = _preview_optimization(table, table_name, action)
            return jsonify({
                "dry_run": True,
                "table": table_name,
                "action": action,
                "preview": preview,
            })

        # Execute the action
        result = _execute_optimization(table, table_name, action)

        return jsonify({
            "dry_run": False,
            "table": table_name,
            "action": action,
            "success": result.get("success", False),
            "message": result.get("message", ""),
            "error": result.get("error"),
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/fsn/iceberg/what-if", methods=["POST"])
def fsn_iceberg_what_if():
    """Preview fix outcome without applying.

    Uses RETE what-if analysis to predict the result of schema changes.
    """
    try:
        data = request.get_json() or {}
        table_name = data.get("table")
        changes = data.get("changes", {})
        goal = data.get("goal", "production_ready")

        if not table_name:
            return jsonify({"error": "Missing required field: table"}), 400

        catalog = get_catalog()
        table = catalog.load_table(table_name)

        # Gather current stats
        metrics = _gather_table_optimization_metrics(table, table_name)

        # Build hypothetical changes list
        change_list = []
        for key, value in changes.items():
            # Map friendly keys to fact patterns
            fact_pattern = f"cloudtrail_table.{table_name.split('.')[-1]}.{key}"
            change_list.append((fact_pattern, value))

        # Run what-if analysis using CloudTrailOptimizer
        try:
            from cybersec.rete.cloudtrail import CloudTrailOptimizer, CloudTrailTableStats

            # Create stats object
            ct_stats = CloudTrailTableStats(
                table_name=table_name.split(".")[-1],
                namespace=table_name.split(".")[0] if "." in table_name else "default",
                events_per_day=metrics.get("row_count", 0),
                total_events=metrics.get("row_count", 0),
                total_size_gb=metrics.get("total_size_mb", 0) / 1024,
                days_of_data=1,
                is_partitioned=bool(metrics.get("partition_spec")),
                has_time_partition=False,  # Will be updated by what-if
                partition_granularity="",
                denormalized_fields=[],
                has_json_blob=True,
                sort_columns=[],
                file_count=metrics.get("file_count", 0),
                avg_file_size_mb=metrics.get("avg_file_size_mb", 0),
                snapshot_count=metrics.get("snapshot_count", 0),
                unique_users=0,
                unique_ips=0,
                unique_event_sources=0,
                error_rate=0,
            )

            optimizer = CloudTrailOptimizer()
            optimizer.analyze(ct_stats)

            if change_list:
                explanation = optimizer.what_if(goal, change_list)
                result = {
                    "conclusion": explanation.conclusion.value,
                    "summary": explanation.summary,
                    "steps": [
                        {"step": s.step_number, "action": s.action, "description": s.description, "result": s.result}
                        for s in explanation.steps
                    ],
                }
            else:
                # No changes specified - analyze current state
                gaps = optimizer.analyze_gaps(goal)
                result = {
                    "conclusion": gaps.status.value,
                    "summary": f"Current status for goal '{goal}'",
                    "blocking_conditions": gaps.blocking_conditions,
                    "acquisition_plan": gaps.acquisition_plan,
                }
        except ImportError:
            # Fallback if RETE module not available
            result = {
                "conclusion": "unknown",
                "summary": "RETE analysis not available",
                "steps": [],
            }

        return jsonify({
            "table": table_name,
            "goal": goal,
            "changes": changes,
            "current_health_score": metrics.get("health_score", 0),
            "analysis": result,
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# --- FSN Iceberg Helper Functions ---

def _gather_table_optimization_metrics(table, full_table_name: str) -> dict:
    """Gather optimization metrics for FSN visualization."""
    parts = full_table_name.split(".")
    namespace = parts[0] if len(parts) > 1 else "default"
    table_name = parts[-1]

    # Get snapshots
    snapshots = list(table.snapshots())
    snapshot_count = len(snapshots)

    # Get files
    current = table.current_snapshot()
    file_count = 0
    total_size = 0
    row_count = 0

    if current:
        scan = table.scan()
        files = list(scan.plan_files())
        file_count = len(files)
        total_size = sum(f.file.file_size_in_bytes for f in files)

        # Get row count from snapshot summary if available
        if current.summary:
            props = current.summary.additional_properties if hasattr(current.summary, 'additional_properties') else {}
            row_count = int(props.get('total-records', 0))

    total_size_mb = total_size // (1024 * 1024)
    avg_file_size_mb = total_size_mb // max(1, file_count)

    # Get partition spec
    spec = table.spec()
    partition_spec = "" if spec.is_unpartitioned() else str(spec)

    # Calculate health score (0.0 to 1.0)
    # Components: file size, file count, snapshot count, partitioning
    file_size_score = min(1.0, avg_file_size_mb / 128) if avg_file_size_mb > 0 else 0.5
    file_count_score = min(1.0, 100 / max(1, file_count))  # Fewer files relative to content
    snapshot_score = min(1.0, 50 / max(1, snapshot_count))
    partition_score = 0.8 if partition_spec else 0.3  # Partitioned tables score higher

    health_score = (
        file_size_score * 0.3 +
        file_count_score * 0.3 +
        snapshot_score * 0.2 +
        partition_score * 0.2
    )

    # Detect specific issues
    recommendations = []
    if avg_file_size_mb < 32 and file_count > 10:
        recommendations.append("compact_small_files")
    if snapshot_count > 50:
        recommendations.append("expire_snapshots")
    if file_count > 500:
        recommendations.append("alert_extreme_fragmentation")
    if not partition_spec and row_count > 100000:
        recommendations.append("add_partitioning")

    return {
        "name": table_name,
        "full_name": full_table_name,
        "namespace": namespace,
        "row_count": row_count,
        "file_count": file_count,
        "total_size_mb": total_size_mb,
        "avg_file_size_mb": avg_file_size_mb,
        "partition_spec": partition_spec or "unpartitioned",
        "partition_count": 1,  # Simplified
        "snapshot_count": snapshot_count,
        "health_score": round(health_score, 2),
        "recommendations": recommendations,
    }


def _extract_partition_value(file_path: str) -> str:
    """Extract partition value from file path."""
    # Handle various partition formats:
    # s3://bucket/warehouse/table/partition=value/file.parquet
    # s3://bucket/warehouse/table/year=2024/month=01/file.parquet

    parts = file_path.split("/")
    partition_parts = []

    for part in parts:
        if "=" in part:
            partition_parts.append(part)

    if partition_parts:
        return "/".join(partition_parts)
    return "unpartitioned"


def _get_table_recommendations(table, full_table_name: str) -> list:
    """Get RETE-based recommendations for a table."""
    metrics = _gather_table_optimization_metrics(table, full_table_name)

    recommendations = []

    # Check for small files
    if metrics["avg_file_size_mb"] < 32 and metrics["file_count"] > 10:
        recommendations.append({
            "table": full_table_name,
            "rule_id": "compact_small_files",
            "priority": 800,
            "severity": "critical" if metrics["avg_file_size_mb"] < 16 else "warning",
            "action_type": "compaction",
            "description": f"Compact {metrics['file_count']} small files (avg {metrics['avg_file_size_mb']}MB)",
            "impact": f"Reduce from {metrics['file_count']} files to ~{max(1, metrics['total_size_mb'] // 128)} files",
        })

    # Check for snapshot explosion
    if metrics["snapshot_count"] > 50:
        recommendations.append({
            "table": full_table_name,
            "rule_id": "expire_old_snapshots",
            "priority": 400 if metrics["snapshot_count"] < 100 else 700,
            "severity": "critical" if metrics["snapshot_count"] > 100 else "warning",
            "action_type": "expire_snapshots",
            "description": f"Expire old snapshots ({metrics['snapshot_count']} accumulated)",
            "impact": f"Reduce to ~10 snapshots, reclaim metadata storage",
        })

    # Check for extreme fragmentation
    if metrics["file_count"] > 500:
        recommendations.append({
            "table": full_table_name,
            "rule_id": "alert_extreme_fragmentation",
            "priority": 950,
            "severity": "critical",
            "action_type": "alert",
            "description": f"Extreme fragmentation: {metrics['file_count']} files",
            "impact": "Query performance severely degraded",
        })

    # Check for missing partitioning
    if metrics["partition_spec"] == "unpartitioned" and metrics["row_count"] > 100000:
        recommendations.append({
            "table": full_table_name,
            "rule_id": "suggest_partitioning",
            "priority": 600,
            "severity": "warning",
            "action_type": "partition",
            "description": f"Consider partitioning ({metrics['row_count']:,} rows unpartitioned)",
            "impact": "Enable partition pruning for time-based queries",
        })

    return recommendations


def _preview_optimization(table, table_name: str, action: str) -> dict:
    """Preview the result of an optimization action."""
    metrics = _gather_table_optimization_metrics(table, table_name)

    if action == "compact":
        current_files = metrics["file_count"]
        target_size_mb = 128
        expected_files = max(1, metrics["total_size_mb"] // target_size_mb)

        return {
            "current_files": current_files,
            "expected_files": expected_files,
            "reduction_percent": round((1 - expected_files / max(1, current_files)) * 100, 1),
            "target_file_size_mb": target_size_mb,
        }

    elif action == "expire":
        current_snapshots = metrics["snapshot_count"]
        keep_count = 10
        to_expire = max(0, current_snapshots - keep_count)

        return {
            "current_snapshots": current_snapshots,
            "to_expire": to_expire,
            "to_keep": min(current_snapshots, keep_count),
        }

    elif action == "rewrite_manifests":
        return {
            "description": "Rewrite manifest files to optimize metadata",
        }

    return {"error": f"Unknown action: {action}"}


def _execute_optimization(table, table_name: str, action: str) -> dict:
    """Execute an optimization action."""
    try:
        if action == "compact":
            # PyIceberg compaction
            if hasattr(table, 'rewrite_data_files'):
                table.rewrite_data_files(target_file_size_bytes=128 * 1024 * 1024)
                return {"success": True, "message": "Compaction completed"}
            else:
                return {
                    "success": False,
                    "error": "Compaction not available in this PyIceberg version",
                    "manual_command": f"CALL catalog.system.rewrite_data_files(table => '{table_name}')",
                }

        elif action == "expire":
            if hasattr(table, 'expire_snapshots'):
                from datetime import datetime, timedelta
                cutoff = datetime.now() - timedelta(hours=24)
                table.expire_snapshots().older_than(cutoff).retain_last(10).commit()
                return {"success": True, "message": "Snapshots expired"}
            else:
                return {
                    "success": False,
                    "error": "Snapshot expiration not available",
                }

        elif action == "rewrite_manifests":
            if hasattr(table, 'rewrite_manifests'):
                table.rewrite_manifests().commit()
                return {"success": True, "message": "Manifests rewritten"}
            else:
                return {"success": False, "error": "Manifest rewriting not available"}

        else:
            return {"success": False, "error": f"Unknown action: {action}"}

    except Exception as e:
        return {"success": False, "error": str(e)}


@app.route("/api/catalog/table/<path:table_name>")
def get_table_details(table_name):
    """Get detailed table information"""
    try:
        catalog = get_catalog()
        table = catalog.load_table(table_name)
        metadata = table.metadata
        
        # Schema
        schema_fields = []
        for field in table.schema().fields:
            schema_fields.append({
                "id": field.field_id,
                "name": field.name,
                "type": str(field.field_type),
                "required": field.required,
                "doc": field.doc
            })
            
        # Partition Spec
        partitions = []
        for field in table.spec().fields:
            partitions.append({
                "field_id": field.field_id,
                "source_id": field.source_id,
                "name": field.name,
                "transform": str(field.transform)
            })
            
        # Snapshots (Limit to last 50 for performance)
        snapshots = []
        for s in metadata.snapshots[-50:]:
            # Summary is a pydantic model - use model_dump() to serialize
            summary_dict = s.summary.model_dump() if hasattr(s.summary, 'model_dump') else {"operation": str(s.summary.operation)}
            snapshots.append({
                "snapshot_id": s.snapshot_id,
                "timestamp_ms": s.timestamp_ms,
                "timestamp": datetime.fromtimestamp(s.timestamp_ms / 1000).isoformat(),
                "manifest_list": s.manifest_list,
                "summary": summary_dict
            })
            
        # Reverse snapshots to show newest first
        snapshots.reverse()
            
        return jsonify({
            "identifier": str(table_name),
            "properties": metadata.properties,
            "schema": schema_fields,
            "partitions": partitions,
            "snapshots": snapshots,
            "location": metadata.location,
            "current_snapshot_id": metadata.current_snapshot_id,
            "format_version": metadata.format_version
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500



if __name__ == "__main__":
    # Create templates directory if it doesn't exist
    os.makedirs("templates", exist_ok=True)
    
    print("=" * 60)
    print("Cyberphy Hub - Iceberg Browser")
    print("=" * 60)
    print("Starting web server on http://localhost:5050")
    print("Make sure the following services are running:")
    print("  - PostgreSQL (localhost:5438)")
    print("  - RustFS (localhost:9010 API / 9011 console)")
    print("  - Polaris (localhost:8181 catalog / 8182 admin)")
    print("=" * 60)
    
    app.run(host="0.0.0.0", port=5050, debug=True)
