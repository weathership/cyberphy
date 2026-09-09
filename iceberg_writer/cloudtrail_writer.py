"""
PyIceberg Writer for CloudTrail Events
Provides table creation and batch writing utilities for Iceberg tables.

Requirements:
- pyiceberg[s3fs,sql-postgres]>=0.10.0
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, BooleanType, TimestampType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, HourTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform
import pyarrow as pa
import os
from datetime import datetime


class IcebergWriter:
    """Write CloudTrail events to Iceberg tables using PostgreSQL catalog"""

    def __init__(self, catalog_uri: str, warehouse_path: str):
        """
        Initialize the Iceberg writer

        Args:
            catalog_uri: PostgreSQL connection URI
            warehouse_path: S3/MinIO path for Iceberg warehouse
        """
        # Configure Iceberg catalog with PostgreSQL backend and S3 storage
        # Uses sql-postgres extra for PostgreSQL catalog support
        # Uses s3fs extra for S3-compatible storage (MinIO)
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

    def create_cloudtrail_table(self, namespace: str = "cybersec", table_name: str = "cloudtrail_events"):
        """Create Iceberg table for CloudTrail events if it doesn't exist"""

        # Define the schema for CloudTrail events (all fields nullable for flexibility)
        schema = Schema(
            NestedField(1, "event_id", StringType(), required=False),
            NestedField(2, "event_version", StringType(), required=False),
            NestedField(3, "event_timestamp", TimestampType(), required=False),
            NestedField(4, "event_source", StringType(), required=False),
            NestedField(5, "event_name", StringType(), required=False),
            NestedField(6, "aws_region", StringType(), required=False),
            NestedField(7, "source_ip", StringType(), required=False),
            NestedField(8, "user_agent", StringType(), required=False),
            NestedField(9, "user_type", StringType(), required=False),
            NestedField(10, "user_arn", StringType(), required=False),
            NestedField(11, "account_id", StringType(), required=False),
            NestedField(12, "read_only", BooleanType(), required=False),
            NestedField(13, "event_type", StringType(), required=False),
            NestedField(14, "processing_time", TimestampType(), required=False),
        )

        # Partition by day + region for efficient time-range and region queries
        # This enables partition pruning in the FSN visualization
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=3,  # event_timestamp
                field_id=1000,
                transform=HourTransform(),
                name="event_hour"
            ),
            PartitionField(
                source_id=6,  # aws_region
                field_id=1001,
                transform=IdentityTransform(),
                name="region"
            )
        )

        # Sort by event_timestamp and event_id for better query performance
        sort_order = SortOrder(
            SortField(source_id=3, transform=IdentityTransform()),  # event_timestamp
            SortField(source_id=1, transform=IdentityTransform())   # event_id
        )

        try:
            # Create namespace if it doesn't exist
            try:
                self.catalog.create_namespace(namespace)
                print(f"Created namespace: {namespace}")
            except Exception as e:
                print(f"Namespace {namespace} already exists or error: {e}")

            # Create table
            table = self.catalog.create_table(
                identifier=f"{namespace}.{table_name}",
                schema=schema,
                partition_spec=partition_spec,
                sort_order=sort_order,
                properties={
                    "write.metadata.delete-after-commit.enabled": "true",
                    "write.metadata.previous-versions-max": "5",
                    "history.expire.max-snapshot-age-ms": "3600000"
                }
            )
            print(f"Created table: {namespace}.{table_name}")
            return table
        except Exception as e:
            print(f"Table {namespace}.{table_name} already exists or error: {e}")
            return self.catalog.load_table(f"{namespace}.{table_name}")

    def write_batch(self, table, batch: list[dict]):
        """Write a batch of records to Iceberg table using direct file write"""
        if not batch:
            return

        # Convert to PyArrow table
        pa_table = pa.Table.from_pylist(batch)

        # Write directly as Parquet file to MinIO
        import pyarrow.parquet as pq

        # Create a unique filename
        filename = f"data-{datetime.now().strftime('%Y%m%d-%H%M%S')}.parquet"
        filepath = f"{table.location()}/data/{filename}"

        # Use the catalog's file IO to write
        output_file = table.io.new_output(filepath)
        with output_file.create() as f:
            pq.write_table(pa_table, f)


if __name__ == "__main__":
    # Example usage - create table only (datagen writes directly via Flink)
    catalog_uri = os.getenv(
        "ICEBERG_CATALOG_URI",
        "postgresql://postgres@localhost:5438/cybersec"
    )
    warehouse_path = os.getenv(
        "ICEBERG_WAREHOUSE",
        "s3://cyberphy/iceberg/warehouse"
    )

    print("Initializing Iceberg Writer...")
    print(f"  Catalog URI: {catalog_uri}")
    print(f"  Warehouse: {warehouse_path}")

    writer = IcebergWriter(catalog_uri, warehouse_path)
    table = writer.create_cloudtrail_table()
    print(f"Table ready: cybersec.cloudtrail_events")
