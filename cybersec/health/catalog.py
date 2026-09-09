"""FMEA Failure Mode Catalog for cybersec toolkit.

Defines failure modes with base FMEA scores for:
- Iceberg/PyIceberg issues (ICE_*)
- Flink issues (FLINK_*)
- NiFi issues (NIFI_*)
- Infrastructure issues (INFRA_*)
- Data quality issues (DATA_*)
"""

from .models import AutomationLevel, FailureMode

# Iceberg/PyIceberg failure modes
ICE_001 = FailureMode(
    failure_mode_id="ICE_001",
    category="iceberg",
    name="Scan Memory Exhaustion",
    description="PyIceberg scan loads too much data into memory, causing OOM",
    base_severity=8,   # High impact - browser crashes
    base_occurrence=4,  # Moderate - happens with large tables
    base_detection=3,   # Good - can monitor memory
    symptom="Browser OOM, 7GB+ memory usage, slow/timeout responses on /api/events",
    cause="PyIceberg scan().to_pandas() without limit on large table (420k+ records)",
    detection_method="Monitor process memory via psutil, warn at 2GB, critical at 4GB",
    remediation_steps=[
        "Restart iceberg_browser process",
        "Verify all scans use limit=N parameter",
        "Check MAX_SCAN_ROWS in iceberg_browser.py (line 307)",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

ICE_002 = FailureMode(
    failure_mode_id="ICE_002",
    category="iceberg",
    name="Catalog Connection Failed",
    description="Cannot connect to Polaris REST catalog",
    base_severity=9,   # Critical - no table access
    base_occurrence=3,  # Low-moderate - usually stable
    base_detection=2,   # Easy to detect
    symptom="500 errors on /api/tables, /api/events returns 'Table not found'",
    cause="Polaris REST catalog unreachable or credentials invalid",
    detection_method="catalog.list_namespaces() call succeeds",
    remediation_steps=[
        "Check Polaris is running: curl http://localhost:8181/q/health/ready",
        "Verify credentials in POLARIS_CLIENT_ID/SECRET",
        "Restart Polaris: devenv tasks run restart:polaris",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

ICE_003 = FailureMode(
    failure_mode_id="ICE_003",
    category="iceberg",
    name="Stale Data",
    description="No new snapshots being written to table",
    base_severity=5,   # Moderate - data is old but system works
    base_occurrence=4,  # Moderate - pipeline can stall
    base_detection=4,   # Moderate - need to check timestamps
    symptom="Dashboard shows old events, real-time metrics stuck",
    cause="Pipeline not writing new snapshots (Flink job stopped, writer failed)",
    detection_method="Compare latest snapshot timestamp vs current time (> 30 min = stale)",
    remediation_steps=[
        "Check Flink job status: curl http://localhost:8081/jobs/overview",
        "Verify iceberg_writer is running",
        "Check for write errors in Flink logs",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

# Flink failure modes
FLINK_001 = FailureMode(
    failure_mode_id="FLINK_001",
    category="flink",
    name="TaskManager Missing",
    description="No Flink TaskManagers registered with JobManager",
    base_severity=9,   # Critical - jobs can't run
    base_occurrence=3,  # Low-moderate - usually stable
    base_detection=2,   # Easy to detect
    symptom="Jobs stuck in CREATED state, 'No available slots' errors",
    cause="TaskManager process not started or crashed",
    detection_method="Query /taskmanagers API, check count > 0",
    remediation_steps=[
        "Check TaskManager process: pgrep -f TaskManager",
        "Start TaskManager: $FLINK_HOME/bin/taskmanager.sh start",
        "Check logs: $FLINK_HOME/log/flink-*-taskexecutor-*.log",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

FLINK_002 = FailureMode(
    failure_mode_id="FLINK_002",
    category="flink",
    name="Job Failed",
    description="Flink job crashed or failed",
    base_severity=8,   # High - no data processing
    base_occurrence=4,  # Moderate - can happen
    base_detection=2,   # Easy to detect
    symptom="No new data arriving in Iceberg table",
    cause="Job exception, resource exhaustion, or dependency failure",
    detection_method="Query /jobs/overview API for jobs in FAILED state",
    remediation_steps=[
        "Check job status: curl http://localhost:8081/jobs/overview",
        "View exceptions: curl http://localhost:8081/jobs/<jid>/exceptions",
        "Restart job: flink run -py flink_jobs/cloudtrail_datagen.py",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

FLINK_003 = FailureMode(
    failure_mode_id="FLINK_003",
    category="flink",
    name="Checkpoint Stale",
    description="Flink checkpoints not being created",
    base_severity=6,   # Moderate - data loss risk on restart
    base_occurrence=3,  # Low-moderate
    base_detection=5,   # Moderate - need to check files
    symptom="Data loss risk on restart, checkpoint directory has old files",
    cause="Checkpoint failures due to storage issues or job problems",
    detection_method="Check checkpoint directory for recent files (< 5 min old)",
    remediation_steps=[
        "Check checkpoint config in Flink job",
        "Verify local S3 (RustFS) storage is healthy",
        "Review Flink logs for checkpoint errors",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.C,
)

# PyFlink-specific failure modes
PYFLINK_001 = FailureMode(
    failure_mode_id="PYFLINK_001",
    category="pyflink",
    name="PyFlink Not Installed",
    description="PyFlink package not installed or not importable",
    base_severity=9,   # Critical - PyFlink jobs can't run
    base_occurrence=4,  # Moderate - common on fresh setup
    base_detection=1,   # Very easy to detect
    symptom="ImportError when running PyFlink jobs, 'No module named pyflink'",
    cause="apache-flink package not installed in Python environment",
    detection_method="import pyflink succeeds",
    remediation_steps=[
        "Install PyFlink: uv sync  # editable from thirdparty/flink-python",
        "Verify installation: python -c 'import pyflink; print(pyflink.__version__)'",
        "Ensure using correct Python environment (devenv venv)",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

PYFLINK_002 = FailureMode(
    failure_mode_id="PYFLINK_002",
    category="pyflink",
    name="Python Path Mismatch",
    description="Flink using different Python than PyFlink installed in",
    base_severity=8,   # High - jobs fail with cryptic errors
    base_occurrence=6,  # High on macOS - common issue
    base_detection=4,   # Moderate - need to compare paths
    symptom="'Python process exits with code: 1', PyFlink import errors in TaskManager logs",
    cause="python.executable not set in config.yaml or points to wrong Python",
    detection_method="Compare sys.executable with config.yaml python settings (Flink 1.20+)",
    remediation_steps=[
        "Run: /health fix --apply",
        "Or manually add python section to config.yaml",
        "Then restart: devenv tasks run restart:clean",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

PYFLINK_003 = FailureMode(
    failure_mode_id="PYFLINK_003",
    category="pyflink",
    name="kafka-python Missing",
    description="kafka-python package required but not installed",
    base_severity=7,   # High - Kafka connectors fail
    base_occurrence=4,  # Moderate - common oversight
    base_detection=1,   # Very easy to detect
    symptom="'No module named kafka' errors, Kafka source/sink fails",
    cause="kafka-python package not installed",
    detection_method="import kafka succeeds",
    remediation_steps=[
        "Install kafka-python: uv pip install kafka-python",
        "Verify: python -c 'import kafka; print(\"OK\")'",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

PYFLINK_004 = FailureMode(
    failure_mode_id="PYFLINK_004",
    category="pyflink",
    name="FLINK_HOME Not Set",
    description="FLINK_HOME environment variable not configured",
    base_severity=8,   # High - can't submit jobs
    base_occurrence=4,  # Moderate - common on fresh setup
    base_detection=1,   # Very easy to detect
    symptom="'flink' command not found, job submission fails",
    cause="Flink not installed or FLINK_HOME not exported",
    detection_method="FLINK_HOME env var set and points to valid directory",
    remediation_steps=[
        "Run bootstrap: devenv tasks run restart:clean (builds Flink on first run)",
        "Or set manually: export FLINK_HOME=/path/to/flink-1.20.1",
        "Verify: $FLINK_HOME/bin/flink --version",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

PYFLINK_005 = FailureMode(
    failure_mode_id="PYFLINK_005",
    category="pyflink",
    name="macOS Python Configuration",
    description="macOS-specific Python path issues with Flink",
    base_severity=7,   # High - jobs fail
    base_occurrence=7,  # Very high on macOS
    base_detection=3,   # Good - can detect platform
    symptom="PyFlink works locally but fails in Flink cluster on macOS",
    cause="macOS has multiple Python installations, Flink picks wrong one",
    detection_method="Platform is Darwin AND python settings not in config.yaml",
    remediation_steps=[
        "Run: /health fix --apply",
        "Or manually add python section to $FLINK_HOME/conf/config.yaml",
        "Then restart: devenv tasks run restart:clean",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

PYFLINK_006 = FailureMode(
    failure_mode_id="PYFLINK_006",
    category="pyflink",
    name="Job Submission Log Errors",
    description="Errors detected in PyFlink job submission log",
    base_severity=6,   # Moderate-high
    base_occurrence=5,  # Moderate
    base_detection=2,   # Easy - check log file
    symptom="Job submission fails, errors in /tmp/cloudtrail_submit.log",
    cause="Various - check log for specific error",
    detection_method="Check /tmp/cloudtrail_submit.log for ERROR/Exception lines",
    remediation_steps=[
        "Review full log: cat /tmp/cloudtrail_submit.log",
        "Check for Python errors (import, syntax)",
        "Check for Flink errors (cluster connectivity, resource allocation)",
        "Verify Flink cluster is running: curl http://localhost:8081/overview",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.C,
)

PYFLINK_007 = FailureMode(
    failure_mode_id="PYFLINK_007",
    category="pyflink",
    name="Config Written But Not Applied",
    description="config.yaml has Python settings but Flink not using them",
    base_severity=8,   # High - fix appears successful but doesn't work
    base_occurrence=6,  # High - common after fix without restart
    base_detection=3,   # Moderate - need to check both config and runtime
    symptom="'Python process exits with code: 1' persists after /health fix --apply",
    cause="Flink cluster not restarted after config change, JVM still using old config",
    detection_method="Config has python.executable but TaskManager logs show wrong Python",
    remediation_steps=[
        "Stop Flink cluster: $FLINK_HOME/bin/stop-cluster.sh",
        "Verify config: cat $FLINK_HOME/conf/config.yaml | grep -A2 python",
        "Start Flink cluster: $FLINK_HOME/bin/start-cluster.sh",
        "Or run: devenv tasks run restart:clean",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

PYFLINK_008 = FailureMode(
    failure_mode_id="PYFLINK_008",
    category="pyflink",
    name="Flink Cluster Stale After Config Change",
    description="Flink JVM processes running with old configuration",
    base_severity=7,   # High - silent failure
    base_occurrence=5,  # Moderate - happens when restart skipped
    base_detection=4,   # Moderate - need to compare timestamps
    symptom="Config file newer than Flink process start time",
    cause="Flink cluster not restarted after config.yaml modification",
    detection_method="Compare config.yaml mtime vs TaskManager process start time",
    remediation_steps=[
        "Stop Flink: $FLINK_HOME/bin/stop-cluster.sh",
        "Start Flink: $FLINK_HOME/bin/start-cluster.sh",
        "Verify processes restarted: ps aux | grep -i taskmanager",
        "Or run full restart: devenv tasks run restart:clean",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

PYFLINK_009 = FailureMode(
    failure_mode_id="PYFLINK_009",
    category="pyflink",
    name="Python Executable Not Found by Flink",
    description="Configured Python path in config.yaml does not exist or is not executable",
    base_severity=8,   # High - jobs fail immediately
    base_occurrence=3,  # Low-moderate - config error
    base_detection=2,   # Easy - check file exists
    symptom="'Python process exits with code: 1', Python path in config invalid",
    cause="Configured python.executable path does not exist or changed",
    detection_method="Check if python.executable path from config.yaml exists and is executable",
    remediation_steps=[
        "Check configured path: cat $FLINK_HOME/conf/config.yaml | grep -A2 python",
        "Verify path exists: ls -la /path/to/python3",
        "Re-run fix: /health fix --apply",
        "Restart cluster: devenv tasks run restart:clean",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

PYFLINK_010 = FailureMode(
    failure_mode_id="PYFLINK_010",
    category="pyflink",
    name="FLINK_HOME Not Exported",
    description="FLINK_HOME env var not set in current shell",
    base_severity=5,   # Moderate - inconvenience, not blocking
    base_occurrence=6,  # High - common outside devenv shell
    base_detection=1,   # Very easy to detect
    symptom="$FLINK_HOME not available in shell, manual flink commands fail",
    cause="Running outside devenv shell or shell not refreshed after devenv.nix change",
    detection_method="Check if FLINK_HOME environment variable is set",
    remediation_steps=[
        "Re-enter devenv shell: exit && devenv shell",
        "Or run: direnv reload (if using direnv)",
        "Verify: echo $FLINK_HOME",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

PYFLINK_011 = FailureMode(
    failure_mode_id="PYFLINK_011",
    category="pyflink",
    name="Iceberg AWS Bundle Missing",
    description="iceberg-aws-bundle JAR not installed in Flink lib directory",
    base_severity=9,   # Critical - S3FileIO completely fails
    base_occurrence=5,  # Moderate - common on fresh setup or after Flink rebuild
    base_detection=1,   # Very easy to detect - just check file exists
    symptom="NoClassDefFoundError: software/amazon/awssdk/core/exception/SdkException",
    cause="iceberg-aws-bundle-*.jar not in $FLINK_HOME/lib/, bootstrap skipped or failed",
    detection_method="Check for iceberg-aws-bundle-*.jar in $FLINK_HOME/lib/",
    remediation_steps=[
        "Run bootstrap to build and install: cybersec bootstrap run",
        "Or manually build: cd thirdparty/iceberg && ./gradlew :iceberg-aws-bundle:shadowJar",
        "Then copy: cp aws-bundle/build/libs/iceberg-aws-bundle-*.jar $FLINK_HOME/lib/",
        "Restart Flink: devenv tasks run restart:clean",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

PYFLINK_012 = FailureMode(
    failure_mode_id="PYFLINK_012",
    category="pyflink",
    name="Iceberg Flink Runtime Missing",
    description="iceberg-flink-runtime JAR not installed in Flink lib directory",
    base_severity=9,   # Critical - Iceberg catalog fails completely
    base_occurrence=5,  # Moderate - common on fresh setup
    base_detection=1,   # Very easy to detect
    symptom="'No factory implements IcebergCatalog' or 'Could not find a suitable table factory'",
    cause="iceberg-flink-runtime-1.20-*.jar not in $FLINK_HOME/lib/",
    detection_method="Check for iceberg-flink-runtime-1.20-*.jar in $FLINK_HOME/lib/",
    remediation_steps=[
        "Run bootstrap to build and install: cybersec bootstrap run",
        "Or manually build: cd thirdparty/iceberg && ./gradlew :iceberg-flink:iceberg-flink-runtime-1.20:shadowJar",
        "Then copy: cp flink/v1.20/flink-runtime/build/libs/iceberg-flink-runtime-1.20-*.jar $FLINK_HOME/lib/",
        "Restart Flink: devenv tasks run restart:clean",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

PYFLINK_013 = FailureMode(
    failure_mode_id="PYFLINK_013",
    category="pyflink",
    name="Git Submodules Not Initialized",
    description="Required git submodules (Flink, Iceberg) not initialized",
    base_severity=9,   # Critical - can't build required components
    base_occurrence=6,  # High - common on fresh clone
    base_detection=1,   # Very easy to detect
    symptom="Missing gradlew, pom.xml, or other build files in thirdparty/",
    cause="Repository cloned without --recursive or submodule update not run",
    detection_method="Check for thirdparty/iceberg/gradlew or thirdparty/flink/pom.xml",
    remediation_steps=[
        "Only when a local Flink/Iceberg *source* build is needed (disk-heavy):",
        "  git submodule update --init thirdparty/flink thirdparty/iceberg",
        "PyFlink itself does not require those submodules — `uv sync` uses thirdparty/flink-python.",
        "Then run bootstrap: devenv tasks run restart:clean",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

# Flink job runtime failure modes
FLINK_004 = FailureMode(
    failure_mode_id="FLINK_004",
    category="flink",
    name="DataGen Job Finishes Immediately",
    description="CloudTrail DataGen job completes immediately instead of running continuously",
    base_severity=7,   # High - E2E verification fails, no continuous data
    base_occurrence=5,  # Moderate - happens when datagen has bounded rows
    base_detection=2,   # Easy - check job status shows FINISHED quickly
    symptom="DataGen job submitted but immediately shows FINISHED, no RUNNING jobs, E2E verification times out",
    cause="DataGen source configured with bounded row count (fields.event_id.end), job completes after generating all rows",
    detection_method="Check Flink jobs API: job submitted but status is FINISHED within seconds, jobs-running=0",
    remediation_steps=[
        "Remove bounded row configuration from datagen source (remove fields.event_id.end)",
        "Or: Increase event_id.end to a very large number for longer runtime",
        "Or: Use 'number-of-rows' with negative value (-1) for unbounded",
        "Restart the job: devenv tasks run restart:clean",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

FLINK_005 = FailureMode(
    failure_mode_id="FLINK_005",
    category="flink",
    name="Job Submission Timeout",
    description="Flink job submitted but never transitions to RUNNING state",
    base_severity=7,   # High - job not processing data
    base_occurrence=4,  # Moderate
    base_detection=3,   # Good - can monitor job state transitions
    symptom="Job shows in CREATED or INITIALIZING state for >60 seconds, verification times out",
    cause="Resource constraints, missing JARs, Python environment issues, or cluster overload",
    detection_method="Monitor job state: CREATED/INITIALIZING for >60s without transitioning to RUNNING",
    remediation_steps=[
        "Check TaskManager has available slots: curl http://localhost:8081/overview",
        "Check JobManager logs for errors: $FLINK_HOME/log/flink-*-standalonesession-*.log",
        "Verify Iceberg JARs installed: ls $FLINK_HOME/lib/iceberg-*",
        "Run PyFlink diagnostics: cybersec --cmd '/health pyflink'",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

PYFLINK_014 = FailureMode(
    failure_mode_id="PYFLINK_014",
    category="pyflink",
    name="Iceberg JAR Version Mismatch",
    description="Multiple Iceberg JAR versions in classpath causing serialization failures",
    base_severity=9,   # Critical - jobs fail immediately with cryptic errors
    base_occurrence=5,  # Moderate - happens when JARs built at different times or from different sources
    base_detection=3,   # Good - can detect from job exception logs
    symptom="InvalidClassException: org.apache.iceberg.Schema; local class incompatible: stream classdesc serialVersionUID differs",
    cause="PyFlink client and Flink TaskManager have different Iceberg JAR versions (e.g., client has iceberg from pip, TaskManager has locally-built JARs)",
    detection_method="Check Flink job exceptions API for InvalidClassException with serialVersionUID mismatch on org.apache.iceberg classes",
    remediation_steps=[
        "Remove ALL Iceberg JARs from $FLINK_HOME/lib/: rm $FLINK_HOME/lib/iceberg-*.jar",
        "Rebuild Iceberg JARs from source: cd thirdparty/iceberg && ./gradlew clean",
        "Build fresh JARs: ./gradlew -PflinkVersions=1.20 :iceberg-flink:iceberg-flink-runtime-1.20:shadowJar :iceberg-aws-bundle:shadowJar -x test",
        "Copy new JARs: cp flink/v1.20/flink-runtime/build/libs/iceberg-flink-runtime-*.jar aws-bundle/build/libs/iceberg-aws-bundle-*.jar $FLINK_HOME/lib/",
        "Restart Flink cluster: devenv tasks run restart:clean",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

# Infrastructure failure modes
INFRA_001 = FailureMode(
    failure_mode_id="INFRA_001",
    category="infra",
    name="PostgreSQL Down",
    description="PostgreSQL database not running",
    base_severity=9,   # Critical - catalog needs DB
    base_occurrence=2,  # Low - usually stable
    base_detection=2,   # Easy to detect
    symptom="Catalog operations fail, 'connection refused' errors",
    cause="PostgreSQL process not running or port blocked",
    detection_method="TCP connection test to port 5438",
    remediation_steps=[
        "Check PostgreSQL: pg_isready -p 5438",
        "Start with devenv: devenv up postgres",
        "Check logs: journalctl -u postgresql",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

INFRA_002 = FailureMode(
    failure_mode_id="INFRA_002",
    category="infra",
    name="Local S3 (RustFS) Unhealthy",
    description="Local S3 (RustFS) object storage not responding",
    base_severity=9,   # Critical - no data storage
    base_occurrence=2,  # Low - usually stable
    base_detection=2,   # Easy to detect
    symptom="Write failures, 'connection refused' on S3 operations",
    cause="RustFS process not running or storage full",
    detection_method="Check /health endpoint (RustFS; legacy /minio/health/live)",
    remediation_steps=[
        "Check RustFS health: curl http://localhost:9010/health",
        "Start with devenv: devenv up -d  (services.rustfs)",
        "Check disk space: df -h $DEVENV_STATE/rustfs/data",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

INFRA_003 = FailureMode(
    failure_mode_id="INFRA_003",
    category="infra",
    name="Polaris Degraded",
    description="Polaris catalog service degraded or slow",
    base_severity=7,   # High - catalog operations affected
    base_occurrence=3,  # Low-moderate
    base_detection=3,   # Good detection
    symptom="Slow catalog operations, intermittent 503 errors",
    cause="Polaris overloaded or resource constrained",
    detection_method="Check /q/health/ready endpoint response time",
    remediation_steps=[
        "Check Polaris health: curl http://localhost:8182/q/health/ready",
        "Restart Polaris: devenv tasks run restart:polaris",
        "Check memory usage of Polaris process",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

INFRA_004 = FailureMode(
    failure_mode_id="INFRA_004",
    category="infra",
    name="macOS Shared Memory Exhaustion",
    description="Orphaned IPC shared memory segments exhaust system limits",
    base_severity=8,   # High - services won't start
    base_occurrence=6,  # Common on macOS after crashes
    base_detection=3,   # Moderate - parse logs
    symptom="PostgreSQL fails with 'could not create shared memory segment: No space left on device'",
    cause="Orphaned IPC shm segments from previous devenv crashes on macOS",
    detection_method="Parse process-compose.log for 'No space left on device' + 'shared memory'",
    remediation_steps=[
        "List orphaned segments: ipcs -m",
        "Clean up segments owned by current user: ipcrm -m <shmid>",
        "Restart services: devenv up",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

INFRA_005 = FailureMode(
    failure_mode_id="INFRA_005",
    category="infra",
    name="AWS Credentials Invalid",
    description="AWS credentials are missing, expired, or blocked by stale environment variables",
    base_severity=9,   # Critical - AWS deployment impossible
    base_occurrence=5,  # Common - tokens expire, env vars get stale
    base_detection=1,   # Very easy - aws sts get-caller-identity
    symptom="AWS CLI returns InvalidClientTokenId or 'credentials not configured'",
    cause="Stale env vars, expired session tokens, or missing credentials file",
    detection_method="aws sts get-caller-identity fails, but --profile default may work",
    remediation_steps=[
        "Check for stale env vars: echo $AWS_ACCESS_KEY_ID",
        "Test with profile: aws sts get-caller-identity --profile default",
        "If profile works: unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY",
        "Or add to .envrc.local: export AWS_PROFILE=default",
        "If profile fails: aws sso login or aws configure",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,  # Manual env fix needed
)

# System-level failure modes (OS configuration)
SYSTEM_001 = FailureMode(
    failure_mode_id="SYSTEM_001",
    category="system",
    name="macOS Shared Memory Limits Too Low",
    description="macOS kernel shared memory limits are too low for PostgreSQL and other services",
    base_severity=9,   # Critical - PostgreSQL won't start
    base_occurrence=7,  # Very common on fresh macOS installs
    base_detection=1,   # Very easy - sysctl query
    symptom="PostgreSQL fails with 'could not create shared memory segment' or services hang",
    cause="macOS default kern.sysv.shmmax (4MB) is too low for PostgreSQL",
    detection_method="sysctl kern.sysv.shmmax < 1GB",
    remediation_steps=[
        "Apply temporary fix: sudo sysctl -w kern.sysv.shmmax=1073741824",
        "Apply temporary fix: sudo sysctl -w kern.sysv.shmall=262144",
        "Apply temporary fix: sudo sysctl -w kern.sysv.shmmni=256",
        "For permanent fix, create /etc/sysctl.conf with these values and reboot",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,  # Requires sudo
)

# NiFi failure modes
NIFI_001 = FailureMode(
    failure_mode_id="NIFI_001",
    category="nifi",
    name="NiFi Not Installed",
    description="NiFi binary not found in expected location",
    base_severity=8,   # High - observability pipeline broken
    base_occurrence=5,  # Moderate - common on macOS fresh setup
    base_detection=1,   # Very easy - check directory exists
    symptom="NiFi process not starting, 'binary not found' errors in devenv",
    cause="NiFi not downloaded or NIFI_HOME not set correctly",
    detection_method="Check thirdparty/nifi/nifi-*/bin/nifi.sh exists",
    remediation_steps=[
        "Run: ./scripts/setup_nifi_bin.sh 2.0.0",
        "Or: cybersec bootstrap run",
        "Verify: ls thirdparty/nifi/nifi-*/bin/nifi.sh",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)

NIFI_002 = FailureMode(
    failure_mode_id="NIFI_002",
    category="nifi",
    name="NiFi Not Running",
    description="NiFi web API not responding",
    base_severity=7,   # High - can't receive traces
    base_occurrence=3,  # Low-moderate
    base_detection=2,   # Easy - HTTP check
    symptom="Port 8450 not responding, traces not flowing to NiFi",
    cause="NiFi process crashed or not started",
    detection_method="HTTP GET to http://localhost:8450/nifi-api/system-diagnostics",
    remediation_steps=[
        "Check NiFi process: pgrep -f nifi",
        "Restart: devenv up nifi",
        "Check logs: $NIFI_HOME/logs/nifi-app.log",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,
)

NIFI_003 = FailureMode(
    failure_mode_id="NIFI_003",
    category="nifi",
    name="NiFi OTLP Receiver Not Ready",
    description="OTLP receiver port not accepting connections",
    base_severity=6,   # Moderate - traces lost but system runs
    base_occurrence=4,  # Moderate
    base_detection=2,   # Easy - TCP check
    symptom="OTEL traces not appearing in NiFi, port 4319 not open",
    cause="OTLP receiver processor not configured or stopped",
    detection_method="TCP connection test to localhost:4319",
    remediation_steps=[
        "Check OTLP port: nc -z localhost 4319",
        "Verify NiFi flow has OTLP receiver configured",
        "Check NiFi logs for processor errors",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.C,
)

# K8s failure modes
K8S_001 = FailureMode(
    failure_mode_id="K8S_001",
    category="k8s",
    name="Kubeconfig Stale",
    description="User kubeconfig is older than system kubeconfig after RKE2 restart",
    base_severity=8,   # High - kubectl fails silently
    base_occurrence=5,  # Moderate - happens on every RKE2 restart
    base_detection=2,   # Easy - compare file mtimes
    symptom="kubectl fails with connection refused or certificate errors",
    cause="RKE2 restarted, system kubeconfig regenerated, user copy not updated",
    detection_method="Compare mtime of /etc/rancher/rke2/rke2.yaml vs ~/.kube/rke2.yaml",
    remediation_steps=[
        "Run: /k8s rke2 refresh --apply",
        "Or manually: sudo cp /etc/rancher/rke2/rke2.yaml ~/.kube/rke2.yaml",
        "Then: sudo chown $USER:$USER ~/.kube/rke2.yaml && chmod 600 ~/.kube/rke2.yaml",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.B,  # Requires sudo
)

# Data quality failure modes
DATA_001 = FailureMode(
    failure_mode_id="DATA_001",
    category="data",
    name="Snapshot Accumulation",
    description="Too many snapshots accumulated in table",
    base_severity=4,   # Low-moderate - performance degradation
    base_occurrence=5,  # Moderate-high - happens over time
    base_detection=4,   # Moderate
    symptom="Slow metadata operations, increased memory usage",
    cause="Snapshot expiration not configured or not running",
    detection_method="Count snapshots in table.metadata.snapshots (> 100 = issue)",
    remediation_steps=[
        "Run snapshot expiration: table.expire_snapshots()",
        "Configure automatic expiration in writer",
        "Consider compaction for optimal read performance",
    ],
    observation_level=AutomationLevel.A,
    solution_level=AutomationLevel.A,
)


# Failure mode registry
FAILURE_MODES: dict[str, FailureMode] = {
    "ICE_001": ICE_001,
    "ICE_002": ICE_002,
    "ICE_003": ICE_003,
    "FLINK_001": FLINK_001,
    "FLINK_002": FLINK_002,
    "FLINK_003": FLINK_003,
    "FLINK_004": FLINK_004,
    "FLINK_005": FLINK_005,
    "PYFLINK_001": PYFLINK_001,
    "PYFLINK_002": PYFLINK_002,
    "PYFLINK_003": PYFLINK_003,
    "PYFLINK_004": PYFLINK_004,
    "PYFLINK_005": PYFLINK_005,
    "PYFLINK_006": PYFLINK_006,
    "PYFLINK_007": PYFLINK_007,
    "PYFLINK_008": PYFLINK_008,
    "PYFLINK_009": PYFLINK_009,
    "PYFLINK_010": PYFLINK_010,
    "PYFLINK_011": PYFLINK_011,
    "PYFLINK_012": PYFLINK_012,
    "PYFLINK_013": PYFLINK_013,
    "PYFLINK_014": PYFLINK_014,
    "INFRA_001": INFRA_001,
    "INFRA_002": INFRA_002,
    "INFRA_003": INFRA_003,
    "INFRA_004": INFRA_004,
    "INFRA_005": INFRA_005,
    "NIFI_001": NIFI_001,
    "NIFI_002": NIFI_002,
    "NIFI_003": NIFI_003,
    "DATA_001": DATA_001,
    "SYSTEM_001": SYSTEM_001,
    "K8S_001": K8S_001,
}

# Category groupings - organized by type
# Note: Only include categories with actual checks. Future categories
# (kafka, aws-s3, infra) can be added when checks are implemented.
CATEGORIES: dict[str, list[str]] = {
    # Cloudera OSS components (named directly)
    "flink": [
        # Core Flink
        "FLINK_001", "FLINK_002", "FLINK_003", "FLINK_004", "FLINK_005",
        # PyFlink (same component from health perspective)
        "PYFLINK_001", "PYFLINK_002", "PYFLINK_003", "PYFLINK_004", "PYFLINK_005",
        "PYFLINK_006", "PYFLINK_007", "PYFLINK_008", "PYFLINK_009", "PYFLINK_010",
        "PYFLINK_011", "PYFLINK_012", "PYFLINK_013", "PYFLINK_014",
    ],
    "nifi": ["NIFI_001", "NIFI_002", "NIFI_003"],

    # Provider-agnostic (swappable components)
    "rest-catalog": ["ICE_001", "ICE_002", "ICE_003", "INFRA_003"],  # Iceberg + Polaris
    "local-s3": ["INFRA_002"],   # RustFS (local S3; historical minio)

    # Infrastructure
    "postgres": ["INFRA_001"],
    "system": ["INFRA_004", "SYSTEM_001"],  # OS-level: shm segments, shm limits, eBPF
    "aws": ["INFRA_005"],        # AWS credentials and IAM
    "data": ["DATA_001"],
    "k8s": ["K8S_001"],
}

# Backward compatibility aliases
CATEGORY_ALIASES: dict[str, str] = {
    "iceberg": "rest-catalog",
    "pyflink": "flink",  # PyFlink is just Flink from health perspective
}

# Quick checks (critical infrastructure only)
QUICK_CHECKS: list[str] = [
    "INFRA_001",  # PostgreSQL
    "INFRA_002",  # local-s3 / RustFS
    "FLINK_001",  # TaskManager
    "ICE_002",    # Catalog connection
    "NIFI_002",   # NiFi running
    "K8S_001",    # Kubeconfig stale
]


def get_failure_mode(failure_mode_id: str) -> FailureMode | None:
    """Get a failure mode by ID."""
    return FAILURE_MODES.get(failure_mode_id)


def get_category_mode_ids(category: str) -> list[str]:
    """Get failure mode IDs for a category (supports aliases)."""
    resolved = CATEGORY_ALIASES.get(category, category)
    return CATEGORIES.get(resolved, [])


def get_category_modes(category: str) -> list[FailureMode]:
    """Get all failure modes in a category."""
    mode_ids = get_category_mode_ids(category)
    return [FAILURE_MODES[mid] for mid in mode_ids if mid in FAILURE_MODES]


def get_all_categories() -> list[str]:
    """Get list of all available categories."""
    return list(CATEGORIES.keys())


def get_quick_check_modes() -> list[FailureMode]:
    """Get failure modes for quick health check."""
    return [FAILURE_MODES[mid] for mid in QUICK_CHECKS if mid in FAILURE_MODES]
