"""
Cyberphy Toolkit - CloudTrail Event Pipeline Orchestrator

This provides utilities for:
1. Checking service health
2. Initializing Iceberg catalog
3. Viewing pipeline status

Note: The datagen jobs (Python or Java) are started via devenv processes, not this script.
"""

import subprocess
import sys
from pathlib import Path


class PipelineOrchestrator:
    """Orchestrate the CloudTrail event processing pipeline"""

    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.processes = {}

    def check_services(self):
        """Check if required services are running"""
        print("Checking required services...")

        services = {
            "PostgreSQL": ("localhost", 5438),
            "RustFS": ("localhost", 9010),
            "Flink JobManager": ("localhost", 8081),
            "Polaris REST": ("localhost", 8181),
        }

        import socket
        for service, (host, port) in services.items():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex((host, port))
                sock.close()

                if result == 0:
                    print(f"  ✓ {service} is running on {host}:{port}")
                else:
                    print(f"  ✗ {service} is NOT running on {host}:{port}")
                    print(f"    Run 'devenv up' to start services")
                    return False
            except Exception as e:
                print(f"  ✗ Error checking {service}: {e}")
                return False

        return True

    def initialize_iceberg_catalog(self):
        """Initialize Iceberg catalog in PostgreSQL"""
        print("\nInitializing Iceberg catalog...")
        try:
            subprocess.run(
                ["devenv", "run", "init-iceberg"],
                check=True,
                capture_output=True,
                text=True
            )
            print("  ✓ Iceberg catalog initialized")
        except subprocess.CalledProcessError as e:
            print(f"  ! Iceberg catalog initialization failed (may already exist): {e}")

    def show_status(self):
        """Show pipeline status and useful URLs"""
        print("\n" + "=" * 60)
        print("CloudTrail Event Processing Pipeline")
        print("=" * 60)
        print("\n📊 Service URLs:")
        print("  • Flink Dashboard:  http://localhost:8081")
        print("  • Iceberg Browser:  http://localhost:5050")
        print("  • RustFS Console:   http://localhost:9011")
        print("  • Polaris REST:     http://localhost:8181")
        print("  • PostgreSQL:       localhost:5438 (user: postgres, db: cybersec)")

        print("\n📈 Pipeline Components:")
        print("  • Java DataGen (default): Writes directly to Iceberg at 100 rows/sec")
        print("  • Python DataGen (optional): Writes directly to Iceberg at 10 rows/sec")

        print("\n🔍 Query Examples:")
        print("  python iceberg_writer/cloudtrail_query.py")

        print("\n⚙️  Configuration:")
        print("  • Iceberg Catalog: Polaris REST (localhost:8181)")
        print("  • Iceberg Warehouse: s3://cyberphy/iceberg/warehouse (MinIO)")

        print("\n" + "=" * 60)

    def run_interactive(self):
        """Run pipeline in interactive mode"""
        print("\n🚀 Cyberphy CloudTrail Pipeline")
        print("=" * 60)

        if not self.check_services():
            print("\n⚠️  Required services are not running!")
            print("Please run 'devenv up' in a separate terminal first.")
            return

        self.initialize_iceberg_catalog()
        self.show_status()

        print("\n📝 Pipeline Status:")
        print("  The Java CloudTrail DataGen runs automatically with 'devenv up'.")
        print("  Check the Flink Dashboard for job status: http://localhost:8081")
        print("  Browse Iceberg data: http://localhost:5050")

        print("\n💡 To enable Python datagen instead:")
        print("  Set 'disabled = false' on cloudtrail-datagen in devenv.nix")
        print("  Set 'disabled = true' on java-cloudtrail-datagen\n")


def main():
    """Main entry point"""
    orchestrator = PipelineOrchestrator()

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "check":
            orchestrator.check_services()
        elif command == "init":
            orchestrator.initialize_iceberg_catalog()
        elif command == "status":
            orchestrator.show_status()
        else:
            print(f"Unknown command: {command}")
            print("Available commands: check, init, status")
    else:
        orchestrator.run_interactive()


if __name__ == "__main__":
    main()
