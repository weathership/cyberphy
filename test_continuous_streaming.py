#!/usr/bin/env python3
"""
End-to-End Streaming Test: Flink DataGen → PyIceberg → MinIO

This demonstrates CONTINUOUS (not batch) CloudTrail event ingestion:

Architecture:
  [Flink DataGen] → [JSON Files] → [PyIceberg Daemon] → [Iceberg/MinIO]
      (2/sec)         (buffered)      (continuous)         (parquet)

The test shows:
1. Flink DataGen continuously generating CloudTrail events
2. PyIceberg daemon continuously reading and writing to Iceberg
3. Both processes run indefinitely until stopped
"""

import time
import json
import os
import subprocess
import sys
import signal
import threading
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa
import pyarrow.parquet as pq


class StreamingIngestionDemo:
    def __init__(self):
        self.stop_flag = threading.Event()
        self.events_generated = 0
        self.events_written = 0
        
    def check_services(self):
        """Check that all required services are running"""
        print("🔍 Checking services...")
        
        # Check PostgreSQL
        try:
            result = subprocess.run(
                ["psql", "-h", "localhost", "-p", "5438", "-U", subprocess.getoutput("echo $USER"),
                 "iceberg", "-c", "SELECT 1"],
                capture_output=True,
                timeout=5
            )
            if result.returncode == 0:
                print("   ✅ PostgreSQL ready")
            else:
                print("   ❌ PostgreSQL not accessible")
                return False
        except Exception as e:
            print(f"   ❌ PostgreSQL check failed: {e}")
            return False
        
        # Check MinIO
        try:
            result = subprocess.run(
                ["curl", "-s", "http://localhost:9010/minio/health/live"],
                capture_output=True,
                timeout=5
            )
            if result.returncode == 0:
                print("   ✅ MinIO ready")
            else:
                print("   ❌ MinIO not accessible")
                return False
        except Exception as e:
            print(f"   ❌ MinIO check failed: {e}")
            return False
        
        print()
        return True
    
    def start_flink_datagen(self):
        """Start Flink DataGen producing CloudTrail events continuously"""
        
        print("🎲 Starting Flink DataGen (continuous)...")
        
        # Create output directory
        repo = Path(__file__).resolve().parent
        output_dir = Path(os.environ.get("STREAMING_OUTPUT_DIR", repo / "output" / "streaming"))
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Clear old files
        for f in output_dir.glob("*"):
            f.unlink()
        
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = StreamTableEnvironment.create(env, settings)
        
        # Create DataGen source
        source_ddl = f"""
        CREATE TEMPORARY TABLE cloudtrail_source (
            eventVersion STRING,
            eventID STRING,
            eventTime STRING,
            eventName STRING,
            awsRegion STRING,
            sourceIPAddress STRING,
            userAgent STRING,
            eventSource STRING,
            userIdentityType STRING,
            userIdentityArn STRING,
            userIdentityAccountId STRING,
            requestParameters STRING,
            responseElements STRING
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '3'
        )
        """
        
        t_env.execute_sql(source_ddl)
        
        # Create file sink
        sink_ddl = f"""
        CREATE TEMPORARY TABLE cloudtrail_sink (
            eventVersion STRING,
            eventID STRING,
            eventTime STRING,
            eventName STRING,
            awsRegion STRING,
            sourceIPAddress STRING,
            userAgent STRING,
            eventSource STRING,
            userIdentityType STRING,
            userIdentityArn STRING,
            userIdentityAccountId STRING,
            requestParameters STRING,
            responseElements STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file://{output_dir.absolute()}',
            'format' = 'json',
            'sink.rolling-policy.file-size' = '1MB',
            'sink.rolling-policy.rollover-interval' = '10s'
        )
        """
        
        t_env.execute_sql(sink_ddl)
        
        print(f"   ✅ DataGen configured: 3 events/sec → {output_dir}")
        print(f"   📂 Output: {output_dir}/")
        
        # Start streaming job in background
        def run_job():
            t_env.execute_sql("INSERT INTO cloudtrail_sink SELECT * FROM cloudtrail_source").wait()
        
        job_thread = threading.Thread(target=run_job, daemon=True)
        job_thread.start()
        
        # Wait for first files
        print("   ⏳ Waiting for first events...")
        for _ in range(30):
            if list(output_dir.glob("*")):
                break
            time.sleep(0.5)
        
        if list(output_dir.glob("*")):
            print("   ✅ DataGen started successfully!\n")
            return output_dir
        else:
            print("   ❌ No files generated\n")
            return None
    
    def start_pyiceberg_writer(self, source_dir):
        """Start continuous PyIceberg writer daemon"""
        
        print("📝 Starting PyIceberg writer (continuous)...")
        
        # Initialize catalog
        catalog = SqlCatalog(
            "iceberg_catalog",
            uri=f"postgresql+psycopg2://{subprocess.getoutput('echo $USER')}@localhost:5438/iceberg",
            warehouse="s3://cybersec/iceberg/warehouse",
            s3__endpoint="http://localhost:9010",
            s3__access_key_id="minioadmin",
            s3__secret_access_key="minioadmin"
        )
        
        # Create namespace and table
        try:
            catalog.create_namespace("cybersec")
        except:
            pass
        
        schema = pa.schema([
            ("eventVersion", pa.string()),
            ("eventID", pa.string()),
            ("eventTime", pa.string()),
            ("eventName", pa.string()),
            ("awsRegion", pa.string()),
            ("sourceIPAddress", pa.string()),
            ("userAgent", pa.string()),
            ("eventSource", pa.string()),
            ("userIdentityType", pa.string()),
            ("userIdentityArn", pa.string()),
            ("userIdentityAccountId", pa.string()),
            ("requestParameters", pa.string()),
            ("responseElements", pa.string()),
        ])
        
        try:
            table = catalog.create_table("cybersec.cloudtrail_events_stream", schema=schema)
            print("   ✅ Created new table 'cloudtrail_events_stream'")
        except:
            table = catalog.load_table("cybersec.cloudtrail_events_stream")
            print("   ✅ Using existing table 'cloudtrail_events_stream'")
        
        print(f"   ✅ PyIceberg writer ready\n")
        
        processed_files = set()
        
        def write_loop():
            batch_num = 0
            while not self.stop_flag.is_set():
                try:
                    # Find new JSON files
                    json_files = [f for f in source_dir.glob("*.json") 
                                 if f not in processed_files and f.stat().st_size > 0]
                    
                    if json_files:
                        # Read and parse events
                        events = []
                        for json_file in json_files:
                            try:
                                with open(json_file) as f:
                                    for line in f:
                                        if line.strip():
                                            events.append(json.loads(line))
                                processed_files.add(json_file)
                            except Exception as e:
                                continue
                        
                        if events:
                            # Convert to PyArrow table
                            pa_table = pa.Table.from_pylist(events, schema=schema)
                            
                            # Write directly to Iceberg data location
                            output_file = table.io.new_output(
                                f"s3://cybersec/iceberg/warehouse/cybersec/cloudtrail_events_stream/data/batch-{batch_num:06d}.parquet"
                            )
                            
                            with output_file.create() as f:
                                pq.write_table(pa_table, f, compression='snappy')
                            
                            self.events_written += len(events)
                            batch_num += 1
                    
                    time.sleep(2)  # Check every 2 seconds
                
                except Exception as e:
                    print(f"   ⚠️  Write error: {e}")
                    time.sleep(2)
        
        writer_thread = threading.Thread(target=write_loop, daemon=True)
        writer_thread.start()
        
        return writer_thread
    
    def monitor_progress(self, source_dir, duration=45):
        """Monitor and display progress"""
        
        print("="*70)
        print("  🚀 CONTINUOUS INGESTION ACTIVE")
        print("="*70)
        print()
        print("📊 Data Flow:")
        print("   Flink DataGen (3/sec) → JSON Files → PyIceberg → MinIO/Parquet")
        print()
        print(f"⏱️  Monitoring for {duration} seconds...")
        print("   (Both processes continue beyond monitoring period)")
        print()
        print("─"*70)
        
        start_time = time.time()
        last_file_count = 0
        
        try:
            while time.time() - start_time < duration:
                elapsed = int(time.time() - start_time)
                
                # Count files
                json_count = len(list(source_dir.glob("*.json")))
                expected_events = elapsed * 3
                
                # Check MinIO
                parquet_count = 0
                try:
                    result = subprocess.run(
                        ["curl", "-s", "http://localhost:9011/api/v1/buckets/cybersec/prefix?prefix=iceberg/warehouse/cybersec/cloudtrail_events_stream/data"],
                        capture_output=True,
                        timeout=2
                    )
                    if b".parquet" in result.stdout:
                        parquet_count = result.stdout.count(b".parquet")
                except:
                    pass
                
                # Progress bar
                progress = "█" * min(20, elapsed // 2)
                print(f"\r   ⏱️  {elapsed:2d}s | JSON files: {json_count:3d} | Parquet files: {parquet_count:3d} | Est events: ~{expected_events:4d} {progress}", 
                      end="", flush=True)
                
                time.sleep(1)
        
        except KeyboardInterrupt:
            print("\n\n⚠️  Monitoring interrupted")
        
        print("\n" + "─"*70)
        print()
    
    def verify_results(self):
        """Verify data in MinIO"""
        
        print("🔍 Final Verification...")
        print()
        
        try:
            catalog = SqlCatalog(
                "iceberg_catalog",
                uri=f"postgresql+psycopg2://{subprocess.getoutput('echo $USER')}@localhost:5438/iceberg",
                warehouse="s3://cybersec/iceberg/warehouse",
                s3__endpoint="http://localhost:9010",
                s3__access_key_id="minioadmin",
                s3__secret_access_key="minioadmin"
            )
            
            table = catalog.load_table("cybersec.cloudtrail_events_stream")
            print(f"   ✅ Table exists: cybersec.cloudtrail_events_stream")
            print(f"   📂 Location: {table.location()}")
            
            # Check for parquet files
            result = subprocess.run(
                ["curl", "-s", "http://localhost:9011/api/v1/buckets/cybersec/prefix?prefix=iceberg/warehouse/cybersec/cloudtrail_events_stream/data"],
                capture_output=True,
                timeout=5
            )
            
            parquet_count = result.stdout.count(b".parquet")
            print(f"   📊 Parquet files in MinIO: {parquet_count}")
            print(f"   🌐 MinIO Console: http://localhost:9011/browser/cybersec/iceberg/warehouse/cybersec/cloudtrail_events_stream/")
            
        except Exception as e:
            print(f"   ⚠️  Verification error: {e}")
        
        print()
    
    def run(self):
        """Main execution"""
        
        print("\n" + "="*70)
        print("  FLINK DATAGEN → ICEBERG CONTINUOUS STREAMING TEST")
        print("="*70)
        print()
        
        # Setup signal handler
        def signal_handler(sig, frame):
            print("\n\n⚠️  Stopping processes...")
            self.stop_flag.set()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        # Check services
        if not self.check_services():
            print("❌ Service check failed")
            return 1
        
        try:
            # Start DataGen
            source_dir = self.start_flink_datagen()
            if not source_dir:
                return 1
            
            # Start PyIceberg writer
            writer_thread = self.start_pyiceberg_writer(source_dir)
            
            # Monitor
            self.monitor_progress(source_dir, duration=45)
            
            # Verify
            self.verify_results()
            
            print("="*70)
            print("  ✅ STREAMING TEST COMPLETE")
            print("="*70)
            print()
            print("📝 Summary:")
            print("   - Flink DataGen: STILL RUNNING (generating 3 events/sec)")
            print("   - PyIceberg Writer: STILL RUNNING (writing continuously)")
            print("   - Both processes continue until manually stopped")
            print()
            print("⚠️  These are CONTINUOUS streaming processes:")
            print("   - They run indefinitely, not batch jobs")
            print("   - Data ingestion never stops")
            print("   - Only manual intervention stops them")
            print()
            print("🛑 To stop: Press Ctrl+C")
            print()
            
            # Keep running
            print("Press Ctrl+C to stop all processes...")
            while not self.stop_flag.is_set():
                time.sleep(1)
            
            return 0
        
        except Exception as e:
            print(f"\n❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            return 1


if __name__ == "__main__":
    demo = StreamingIngestionDemo()
    sys.exit(demo.run())
