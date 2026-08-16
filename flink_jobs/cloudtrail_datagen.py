"""
Flink DataGen Job for AWS CloudTrail Events
Generates synthetic CloudTrail events for testing the cybersec pipeline

Generates realistic security patterns that trigger visualization widgets:
- Volume bursts → sparkline/horizon charts
- Privilege escalation → threat scores
- Error sequences → threat scores with high severity
- Regional patterns → geo badges
- Read-heavy sessions → ring/proportion charts

Requirements: apache-flink>=2.2.0
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.expressions import col
from pyflink.table.udf import udf
import json
import os
import random
from datetime import datetime, timezone


class CloudTrailDataGen:
    """Generate realistic AWS CloudTrail events with security-relevant patterns"""

    # Event sources mapped to their typical operations
    EVENT_SOURCE_OPS = {
        "signin.amazonaws.com": {
            "events": ["ConsoleLogin", "GetSigninToken", "CheckMfa"],
            "weight": 10,
        },
        "s3.amazonaws.com": {
            "events": ["GetObject", "PutObject", "DeleteObject", "ListBucket",
                       "CreateBucket", "DeleteBucket", "PutBucketPolicy", "GetBucketAcl"],
            "weight": 30,
        },
        "ec2.amazonaws.com": {
            "events": ["DescribeInstances", "RunInstances", "TerminateInstances",
                       "AuthorizeSecurityGroupIngress", "ModifyInstanceAttribute"],
            "weight": 25,
        },
        "iam.amazonaws.com": {
            "events": ["CreateUser", "DeleteUser", "CreateAccessKey", "DeleteAccessKey",
                       "AttachUserPolicy", "DetachUserPolicy", "PutUserPolicy",
                       "CreateRole", "AttachRolePolicy", "UpdateAssumeRolePolicy"],
            "weight": 20,
        },
        "sts.amazonaws.com": {
            "events": ["AssumeRole", "GetCallerIdentity", "GetSessionToken"],
            "weight": 15,
        },
    }

    # Regions with realistic distribution
    REGIONS = {
        "us-east-1": 40,      # Primary region
        "us-west-2": 25,      # Secondary
        "eu-west-1": 20,      # Europe
        "ap-southeast-1": 10, # Asia-Pacific
        "ap-northeast-1": 5,  # Tokyo (rare)
    }

    # User agents with realistic distribution
    USER_AGENTS = {
        "aws-cli/2.13.0 Python/3.11.4": 30,
        "Boto3/1.28.0 Python/3.10.0": 25,
        "aws-sdk-java/2.20.0": 15,
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36": 15,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)": 10,
        "aws-sdk-go/1.44.0": 5,
    }

    # Error codes with context
    ERROR_CODES = {
        "AccessDenied": {"weight": 40, "events": ["GetObject", "PutObject", "AssumeRole"]},
        "UnauthorizedOperation": {"weight": 25, "events": ["RunInstances", "TerminateInstances"]},
        "InvalidParameterValue": {"weight": 15, "events": ["CreateBucket", "PutBucketPolicy"]},
        "EntityAlreadyExists": {"weight": 10, "events": ["CreateUser", "CreateRole"]},
        "NoSuchEntity": {"weight": 10, "events": ["DeleteUser", "DeleteRole", "GetUser"]},
    }

    # Persistent user pool for realistic patterns
    _users = None
    _current_scenario = None
    _scenario_counter = 0

    @classmethod
    def _init_users(cls):
        """Initialize a pool of users with consistent IPs and regions"""
        if cls._users is None:
            cls._users = []
            for i in range(20):
                # Each user has a "home" region and IP
                home_region = cls._weighted_choice(cls.REGIONS)
                cls._users.append({
                    "id": i,
                    "name": f"user-{i:03d}",
                    "type": random.choices(
                        ["IAMUser", "AssumedRole", "FederatedUser"],
                        weights=[60, 35, 5]
                    )[0],
                    "home_region": home_region,
                    "home_ip": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
                    "account_id": str(random.randint(100000000000, 999999999999)),
                })
            # Add one rare root user
            cls._users.append({
                "id": 99,
                "name": "root",
                "type": "Root",
                "home_region": "us-east-1",
                "home_ip": "10.0.0.1",
                "account_id": str(random.randint(100000000000, 999999999999)),
            })

    @classmethod
    def _weighted_choice(cls, weighted_dict):
        """Select from a dict with value weights"""
        items = list(weighted_dict.keys())
        weights = list(weighted_dict.values())
        return random.choices(items, weights=weights)[0]

    @classmethod
    def _pick_scenario(cls):
        """Pick a scenario to generate - creates patterns for widgets"""
        cls._scenario_counter += 1

        # Every N events, start a new scenario
        if cls._scenario_counter % 50 == 0:
            scenarios = [
                ("normal", 50),           # Normal activity
                ("read_burst", 15),       # Read-heavy burst → sparkline
                ("write_burst", 10),      # Write-heavy burst → sparkline
                ("privilege_escalation", 8),  # IAM changes → threat score
                ("failed_auth", 7),       # Auth failures → threat score
                ("cross_region", 5),      # Multi-region → geo badge
                ("delete_spree", 3),      # Mass deletion → high threat
                ("root_activity", 2),     # Root usage → critical threat
            ]
            cls._current_scenario = random.choices(
                [s[0] for s in scenarios],
                weights=[s[1] for s in scenarios]
            )[0]

        return cls._current_scenario or "normal"

    @classmethod
    def generate_event(cls) -> str:
        """Generate a single CloudTrail event with realistic patterns"""
        cls._init_users()
        scenario = cls._pick_scenario()

        # Select user based on scenario
        if scenario == "root_activity":
            user = cls._users[-1]  # Root user
        else:
            user = random.choice(cls._users[:-1])  # Non-root

        # Generate event based on scenario
        if scenario == "read_burst":
            event = cls._gen_read_burst(user)
        elif scenario == "write_burst":
            event = cls._gen_write_burst(user)
        elif scenario == "privilege_escalation":
            event = cls._gen_privilege_escalation(user)
        elif scenario == "failed_auth":
            event = cls._gen_failed_auth(user)
        elif scenario == "cross_region":
            event = cls._gen_cross_region(user)
        elif scenario == "delete_spree":
            event = cls._gen_delete_spree(user)
        elif scenario == "root_activity":
            event = cls._gen_root_activity(user)
        else:
            event = cls._gen_normal(user)

        return json.dumps(event)

    @classmethod
    def _base_event(cls, user, event_source, event_name, region=None, error=None):
        """Create base event structure"""
        region = region or user["home_region"]
        ip = user["home_ip"] if region == user["home_region"] else f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"

        event = {
            "eventVersion": "1.08",
            "userIdentity": {
                "type": user["type"],
                "principalId": f"AIDA{random.randint(100000000000, 999999999999)}",
                "arn": f"arn:aws:iam::{user['account_id']}:user/{user['name']}",
                "accountId": user["account_id"],
                "accessKeyId": f"AKIA{random.randint(1000000000000000, 9999999999999999)}",
                "userName": user["name"],
            },
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "eventSource": event_source,
            "eventName": event_name,
            "awsRegion": region,
            "sourceIPAddress": ip,
            "userAgent": cls._weighted_choice(cls.USER_AGENTS),
            "requestID": f"{random.randint(10**15, 10**16-1):016x}",
            "eventID": f"{random.randint(10**31, 10**32-1):032x}",
            "readOnly": event_name.startswith(("Get", "List", "Describe")),
            "eventType": "AwsApiCall",
            "managementEvent": True,
            "recipientAccountId": user["account_id"],
            "eventCategory": "Management",
        }

        if error:
            event["errorCode"] = error
            event["errorMessage"] = f"User: {user['name']} is not authorized to perform: {event_name}"

        # Add context-specific request parameters
        if "s3" in event_source:
            event["requestParameters"] = {
                "bucketName": f"company-bucket-{random.randint(1, 50)}",
                "key": f"data/{random.choice(['logs', 'exports', 'backups'])}/file-{random.randint(1, 1000)}.json"
            }
        elif "ec2" in event_source:
            event["requestParameters"] = {
                "instancesSet": {"items": [{"instanceId": f"i-{random.randint(10**16, 10**17-1):017x}"}]}
            }
        elif "iam" in event_source:
            event["requestParameters"] = {
                "userName": f"target-user-{random.randint(1, 20)}",
                "policyArn": f"arn:aws:iam::aws:policy/{random.choice(['AdministratorAccess', 'PowerUserAccess', 'ReadOnlyAccess'])}"
            }

        return event

    @classmethod
    def _gen_normal(cls, user):
        """Normal activity - mixed read/write operations"""
        source = cls._weighted_choice({k: v["weight"] for k, v in cls.EVENT_SOURCE_OPS.items()})
        event_name = random.choice(cls.EVENT_SOURCE_OPS[source]["events"])

        # 5% error rate for normal operations
        error = None
        if random.random() < 0.05:
            error = cls._weighted_choice({k: v["weight"] for k, v in cls.ERROR_CODES.items()})

        return cls._base_event(user, source, event_name, error=error)

    @classmethod
    def _gen_read_burst(cls, user):
        """Read-heavy burst - triggers sparkline widget"""
        read_events = [
            ("s3.amazonaws.com", "GetObject"),
            ("s3.amazonaws.com", "ListBucket"),
            ("ec2.amazonaws.com", "DescribeInstances"),
            ("iam.amazonaws.com", "GetUser"),
            ("iam.amazonaws.com", "ListUsers"),
        ]
        source, event_name = random.choice(read_events)
        return cls._base_event(user, source, event_name)

    @classmethod
    def _gen_write_burst(cls, user):
        """Write-heavy burst - triggers sparkline widget"""
        write_events = [
            ("s3.amazonaws.com", "PutObject"),
            ("s3.amazonaws.com", "DeleteObject"),
            ("ec2.amazonaws.com", "RunInstances"),
        ]
        source, event_name = random.choice(write_events)
        return cls._base_event(user, source, event_name)

    @classmethod
    def _gen_privilege_escalation(cls, user):
        """Privilege escalation pattern - triggers high threat score"""
        escalation_sequence = [
            ("iam.amazonaws.com", "CreateAccessKey"),
            ("iam.amazonaws.com", "AttachUserPolicy"),
            ("iam.amazonaws.com", "PutUserPolicy"),
            ("iam.amazonaws.com", "AttachRolePolicy"),
            ("iam.amazonaws.com", "UpdateAssumeRolePolicy"),
        ]
        source, event_name = random.choice(escalation_sequence)

        # 30% of escalation attempts fail
        error = "AccessDenied" if random.random() < 0.3 else None

        return cls._base_event(user, source, event_name, error=error)

    @classmethod
    def _gen_failed_auth(cls, user):
        """Failed authentication attempts - triggers threat score"""
        auth_events = [
            ("signin.amazonaws.com", "ConsoleLogin"),
            ("sts.amazonaws.com", "AssumeRole"),
        ]
        source, event_name = random.choice(auth_events)

        # 80% failure rate for this scenario
        error = random.choice(["AccessDenied", "UnauthorizedOperation"]) if random.random() < 0.8 else None

        return cls._base_event(user, source, event_name, error=error)

    @classmethod
    def _gen_cross_region(cls, user):
        """Cross-region activity - triggers geo badge"""
        # Use a different region than user's home
        other_regions = [r for r in cls.REGIONS.keys() if r != user["home_region"]]
        region = random.choice(other_regions)

        source = cls._weighted_choice({k: v["weight"] for k, v in cls.EVENT_SOURCE_OPS.items()})
        event_name = random.choice(cls.EVENT_SOURCE_OPS[source]["events"])

        return cls._base_event(user, source, event_name, region=region)

    @classmethod
    def _gen_delete_spree(cls, user):
        """Mass deletion - triggers high threat score"""
        delete_events = [
            ("s3.amazonaws.com", "DeleteObject"),
            ("s3.amazonaws.com", "DeleteBucket"),
            ("iam.amazonaws.com", "DeleteUser"),
            ("iam.amazonaws.com", "DeleteAccessKey"),
            ("ec2.amazonaws.com", "TerminateInstances"),
        ]
        source, event_name = random.choice(delete_events)
        return cls._base_event(user, source, event_name)

    @classmethod
    def _gen_root_activity(cls, user):
        """Root account usage - triggers critical threat score"""
        # Root should only do sensitive operations
        root_events = [
            ("iam.amazonaws.com", "CreateUser"),
            ("iam.amazonaws.com", "AttachUserPolicy"),
            ("s3.amazonaws.com", "PutBucketPolicy"),
            ("signin.amazonaws.com", "ConsoleLogin"),
        ]
        source, event_name = random.choice(root_events)
        return cls._base_event(user, source, event_name)


def create_cloudtrail_datagen_job():
    """Create and run CloudTrail data generation job"""
    from cybersec.flink_paths import checkpoint_uri, flink_home as resolve_flink_home

    # Relocatable: FLINK_HOME or repo-relative dist — never os.getcwd() + a baked tree.
    # Iceberg connectors are installed into $FLINK_HOME/lib by flink-bootstrap;
    # do not set pipeline.jars=file://… (that freezes the submitter path in the job graph).
    flink_home = resolve_flink_home()
    os.environ.setdefault("FLINK_HOME", str(flink_home))

    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Enable checkpointing for data commits
    # Checkpoints trigger Iceberg commits - without this, data stays buffered!
    env.enable_checkpointing(10000)  # Checkpoint every 10 seconds

    # Create table environment with streaming settings
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, settings)

    # Set table configuration for faster commits
    t_env.get_config().set("table.exec.sink.not-null-enforcer", "drop")
    t_env.get_config().set("execution.checkpointing.interval", "10s")
    t_env.get_config().set("state.checkpoint-storage", "filesystem")
    t_env.get_config().set("state.checkpoints.dir", checkpoint_uri())
    
    # Register UDF for generating CloudTrail events
    t_env.create_temporary_system_function(
        "generate_cloudtrail", 
        udf(lambda: CloudTrailDataGen.generate_event(), result_type=DataTypes.STRING())
    )
    
    # Create Iceberg catalog for Polaris REST
    # In Polaris REST API, the catalog is accessed at /api/catalog/warehouse_name
    # The catalog name in Flink must match the warehouse name in Polaris
    # Uses S3FileIO (via iceberg-aws-bundle) to avoid Hadoop dependencies
    t_env.execute_sql("""
        CREATE CATALOG cybersec WITH (
            'type' = 'iceberg',
            'catalog-type' = 'rest',
            'uri' = 'http://localhost:8181/api/catalog',
            'credential' = 'admin:admin',
            'scope' = 'PRINCIPAL_ROLE:ALL',
            'warehouse' = 'cybersec',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = 'http://localhost:9010',
            's3.region' = 'us-east-1',
            's3.path-style-access' = 'true',
            's3.access-key-id' = 'minioadmin',
            's3.secret-access-key' = 'minioadmin',
            'client.region' = 'us-east-1'
        )
    """)
    
    # Use the catalog
    t_env.use_catalog('cybersec')
    
    # Create database if not exists (default is a reserved keyword, must be quoted)
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `default`")
    t_env.use_database('default')
    
    # Create Iceberg sink table for CloudTrail events
    # Disable write.metadata.metrics.default to avoid classloader conflicts
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS cloudtrail_events (
            event_data STRING,
            event_time TIMESTAMP(3)
        ) WITH (
            'write.metadata.metrics.default' = 'none'
        )
    """)
    
    # Create a temporary datagen source table (not persisted to Iceberg catalog)
    # Temporary tables support computed columns and connectors like datagen
    t_env.execute_sql("""
        CREATE TEMPORARY TABLE datagen_source (
            event_id BIGINT,
            event_timestamp AS PROCTIME()
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '10',
            'fields.event_id.kind' = 'sequence',
            'fields.event_id.start' = '1',
            'fields.event_id.end' = '1000000'
        )
    """)
    
    # Generate and insert CloudTrail events
    # .wait() blocks until the job completes (for bounded) or is cancelled (for streaming)
    result = t_env.execute_sql("""
        INSERT INTO cloudtrail_events
        SELECT
            generate_cloudtrail() as event_data,
            CURRENT_TIMESTAMP as event_time
        FROM datagen_source
    """)
    print("Job submitted, waiting for completion...")
    result.wait()


if __name__ == "__main__":
    print("Starting CloudTrail DataGen Job...")
    create_cloudtrail_datagen_job()
