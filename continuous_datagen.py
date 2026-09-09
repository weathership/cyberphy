#!/usr/bin/env python3
"""
Continuous Streaming DataGen -> Iceberg
Simulates Flink DataGen behavior: generates test records every 3 seconds
Data is written to Iceberg and can be monitored via SSE in the browser
"""

import time
import random
import string
from datetime import datetime
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# Catalog configuration (SQL catalog - works reliably)
CATALOG_CONFIG = {
    "type": "sql",
    "uri": "postgresql://cybersec:cybersec@localhost:5438/iceberg",
    "warehouse": "s3://cyberphy/iceberg/warehouse",
    "s3.endpoint": "http://localhost:9010",
    "s3.path-style-access": "true",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
}

def generate_record():
    """Generate a single test record"""
    return {
        'id': ''.join(random.choices(string.ascii_letters + string.digits, k=10)),
        'name': ''.join(random.choices(string.ascii_letters, k=20)),
        'amount': random.randint(1, 1000),
        'region': str(random.randint(0, 4))
    }

def create_arrow_table(record):
    """Create PyArrow table from record with proper schema"""
    schema = pa.schema([
        ('id', pa.string(), False),  # Not nullable (required)
        ('name', pa.string()),
        ('amount', pa.int64()),
        ('region', pa.string())
    ])
    
    return pa.Table.from_arrays(
        [
            pa.array([record['id']], type=pa.string()),
            pa.array([record['name']], type=pa.string()),
            pa.array([record['amount']], type=pa.int64()),
            pa.array([record['region']], type=pa.string())
        ],
        schema=schema
    )

def main():
    print("=" * 70)
    print("  Continuous Streaming: DataGen → Iceberg → MinIO")
    print("=" * 70)
    print("\nConnecting to Iceberg catalog...")
    
    # Connect to catalog
    catalog = load_catalog("cybersec", **CATALOG_CONFIG)
    table = catalog.load_table(("e2e_test", "test_data"))
    
    print(f"✓ Connected to table: e2e_test.test_data")
    print(f"✓ Generating 1 record every 3 seconds")
    print(f"✓ Open browser at http://localhost:5050 to monitor via SSE")
    print(f"✓ Press Ctrl+C to stop")
    print("=" * 70)
    print()
    
    count = 0
    try:
        while True:
            # Generate one record
            record = generate_record()
            
            # Create Arrow table
            arrow_table = create_arrow_table(record)
            
            # Write to Iceberg
            table.append(arrow_table)
            count += 1
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] Record #{count:4d} → region={record['region']}, amount={record['amount']:4d}, id={record['id']}")
            
            # Wait 3 seconds before next record
            time.sleep(3)
            
    except KeyboardInterrupt:
        print(f"\n{'=' * 70}")
        print(f"  Stopped. Total records written: {count}")
        print(f"{'=' * 70}")

if __name__ == "__main__":
    main()
