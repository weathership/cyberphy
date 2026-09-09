"""
Query Interface for CloudTrail Iceberg Tables
Provides convenient methods to query CloudTrail events stored in Iceberg format

Requirements: pyiceberg[s3fs,sql-postgres]>=0.10.0
"""

from pyiceberg.catalog import load_catalog
import pyarrow as pa
import pyarrow.compute as pc
from datetime import datetime, timedelta
import os


class CloudTrailQuery:
    """Query interface for CloudTrail events in Iceberg"""

    def __init__(self, catalog_uri: str, warehouse_path: str):
        """Initialize with catalog connection details.

        Args:
            catalog_uri: PostgreSQL URI for catalog
            warehouse_path: S3/MinIO path for Iceberg warehouse
        """
        # Configure Iceberg catalog with PostgreSQL (sql-postgres) and S3 (s3fs)
        # Prefer MINIO_* credentials when S3_ENDPOINT is set (local MinIO)
        endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9010")
        if endpoint:
            access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID", "admin")
            secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY", "admin")
        else:
            access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
            secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "admin")

        self.catalog = load_catalog(
            "cybersec",
            **{
                "type": "sql",
                "uri": catalog_uri,
                "warehouse": warehouse_path,
                "s3.endpoint": endpoint,
                "s3.access-key-id": access_key,
                "s3.secret-access-key": secret_key,
                "s3.path-style-access": "true",
                # Force use of s3fs for S3 operations
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
            }
        )
    
    def get_table(self, namespace: str = "cybersec", table_name: str = "cloudtrail_events"):
        """Load the CloudTrail events table"""
        return self.catalog.load_table(f"{namespace}.{table_name}")
    
    def query_recent_events(self, hours: int = 24, limit: int = 1000):
        """Query recent CloudTrail events"""
        table = self.get_table()
        
        # Calculate timestamp threshold
        threshold = datetime.now() - timedelta(hours=hours)
        
        # Scan with filter
        scan = table.scan(
            row_filter=f"event_timestamp >= '{threshold.isoformat()}'",
            limit=limit
        )
        
        return scan.to_arrow()
    
    def query_by_event_name(self, event_name: str, limit: int = 1000):
        """Query events by event name"""
        table = self.get_table()
        
        scan = table.scan(
            row_filter=f"event_name == '{event_name}'",
            limit=limit
        )
        
        return scan.to_arrow()
    
    def query_by_source_ip(self, source_ip: str, limit: int = 1000):
        """Query events by source IP"""
        table = self.get_table()
        
        scan = table.scan(
            row_filter=f"source_ip == '{source_ip}'",
            limit=limit
        )
        
        return scan.to_arrow()
    
    def get_event_statistics(self, hours: int = 24):
        """Get statistics about recent events"""
        events = self.query_recent_events(hours=hours, limit=100000)
        
        if events.num_rows == 0:
            return {"total_events": 0}
        
        # Calculate statistics
        stats = {
            "total_events": events.num_rows,
            "unique_event_names": len(pc.unique(events['event_name'])),
            "unique_accounts": len(pc.unique(events['account_id'])),
            "unique_ips": len(pc.unique(events['source_ip'])),
            "unique_regions": len(pc.unique(events['aws_region'])),
            "read_only_events": pc.sum(pc.cast(events['read_only'], pa.int64())).as_py(),
        }
        
        return stats


def main():
    """Example queries"""
    catalog_uri = os.getenv(
        "ICEBERG_CATALOG_URI",
        "postgresql://postgres@localhost:5438/cybersec"
    )
    warehouse_path = os.getenv(
        "ICEBERG_WAREHOUSE",
        "s3://cyberphy/iceberg/warehouse"
    )
    
    query = CloudTrailQuery(catalog_uri, warehouse_path)
    
    # Get statistics
    print("\n=== CloudTrail Event Statistics (Last 24h) ===")
    stats = query.get_event_statistics(hours=24)
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    # Query recent events
    print("\n=== Recent Events ===")
    recent = query.query_recent_events(hours=1, limit=10)
    print(f"Found {recent.num_rows} events in the last hour")
    if recent.num_rows > 0:
        print(recent.to_pandas()[['event_timestamp', 'event_name', 'source_ip', 'aws_region']].head())


if __name__ == "__main__":
    main()
