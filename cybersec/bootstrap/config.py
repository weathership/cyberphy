"""Bootstrap configuration management.

Handles loading, saving, and validating bootstrap settings from TOML files.
Settings can come from:
1. .cybersec/config.toml (project-local, primary)
2. Environment variables (override)
3. Defaults (fallback)
"""

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional
import os

# Handle TOML parsing - use tomllib (3.11+) or tomli
try:
    import tomllib
except ImportError:
    import tomli as tomllib

import tomli_w


@dataclass
class ServiceEndpoint:
    """Configuration for a service endpoint."""

    host: str = "localhost"
    port: int = 0
    url: Optional[str] = None

    def get_url(self, scheme: str = "http") -> str:
        """Get the full URL for this endpoint."""
        if self.url:
            return self.url
        return f"{scheme}://{self.host}:{self.port}"


@dataclass
class BootstrapConfig:
    """Complete bootstrap configuration."""

    # Bootstrap state
    completed: bool = False
    last_run: Optional[str] = None

    # Paths (empty string = use default from $DEVENV_STATE)
    minio_data_dir: str = ""
    flink_home: str = ""
    flink_state_dir: str = ""
    log_dir: str = ""

    # Service endpoints
    postgres_host: str = "localhost"
    postgres_port: int = 5438
    postgres_database: str = "iceberg"

    polaris_api_url: str = "http://localhost:8181"
    polaris_admin_url: str = "http://localhost:8182"

    flink_url: str = "http://localhost:8081"

    minio_endpoint: str = "http://localhost:9010"  # RustFS S3 API
    minio_console: str = "http://localhost:9011/rustfs/console/"

    iceberg_browser_port: int = 5050

    # Catalog configuration
    catalog_name: str = "cyberphy"
    catalog_warehouse: str = "s3://cyberphy/iceberg/warehouse"

    # Credentials (for dev convenience; production should use env vars)
    polaris_client_id: str = "admin"
    polaris_client_secret: str = "admin"
    minio_access_key: str = "admin"
    minio_secret_key: str = "admin"

    # Flink build options
    flink_version: str = "1.20.1"
    flink_repo: str = "https://github.com/apache/flink.git"
    flink_branch: str = "release-1.20.1"

    # NiFi configuration
    nifi_home: str = ""
    nifi_state_dir: str = ""
    nifi_url: str = "http://localhost:8450"
    nifi_otlp_port: int = 4319
    nifi_version: str = "2.0.0"

    # Polaris configuration
    polaris_home: str = ""  # Empty = build from thirdparty/polaris
    polaris_version: str = "1.3.0-incubating"

    # UI configuration
    ui_theme: str = "nord"

    # FSN visualization settings
    fsn_default_mode: str = "cloudtrail"  # "cloudtrail" | "iceberg"
    fsn_remember_mode: bool = True
    fsn_iceberg_auto_refresh: bool = False
    fsn_iceberg_refresh_interval: int = 60  # seconds

    # RKE2 local environment
    rke2_system_kubeconfig: str = "/etc/rancher/rke2/rke2.yaml"
    rke2_user_kubeconfig: str = "~/.kube/rke2.yaml"
    rke2_service_name: str = "rke2-server"
    rke2_auto_refresh: bool = False

    # AWS configuration
    aws_profile: str = "default"
    aws_region: str = "us-east-1"
    aws_project: str = "cybersec-dask"
    developer_prefix: str = ""  # Empty = auto-generate from git email
    developer_email: str = ""  # Empty = auto-detect from git config

    def get_minio_data_dir(self) -> Path:
        """Get local object-store data directory (RustFS).

        Config field name remains ``minio_data_dir`` for toml compatibility.
        Order: explicit config → ``RUSTFS_DATA_DIR`` →
        ``/raid/build/cyberphy/data`` → ``$DEVENV_STATE/rustfs/data`` → legacy minio.
        """
        if self.minio_data_dir:
            return Path(self.minio_data_dir).expanduser()
        env_dir = os.environ.get("RUSTFS_DATA_DIR", "").strip()
        if env_dir:
            return Path(env_dir).expanduser()
        raid = Path("/raid/build/cyberphy/data")
        if raid.exists():
            return raid
        devenv_state = os.environ.get("DEVENV_STATE", ".devenv/state")
        rustfs = Path(devenv_state) / "rustfs" / "data"
        if rustfs.exists():
            return rustfs
        return Path(devenv_state) / "minio"

    def get_flink_home(self) -> Optional[Path]:
        """Get Flink home directory."""
        if self.flink_home:
            return Path(self.flink_home).expanduser()
        # Default to thirdparty build location
        default = Path(f"thirdparty/flink/flink-dist/target/flink-{self.flink_version}-bin/flink-{self.flink_version}")
        if default.exists():
            return default
        return None

    def get_flink_state_dir(self) -> Path:
        """Get Flink state directory."""
        if self.flink_state_dir:
            return Path(self.flink_state_dir).expanduser()
        devenv_state = os.environ.get("DEVENV_STATE", ".devenv/state")
        return Path(devenv_state) / "flink"

    def get_log_dir(self) -> Path:
        """Get log directory."""
        if self.log_dir:
            return Path(self.log_dir).expanduser()
        devenv_state = os.environ.get("DEVENV_STATE", ".devenv/state")
        return Path(devenv_state) / "logs"

    def get_nifi_home(self) -> Optional[Path]:
        """Get NiFi home directory.

        Returns path to NiFi installation. Checks in order:
        1. Configured nifi_home path
        2. Maven build output from thirdparty/nifi submodule
        3. Extracted binary from thirdparty/nifi (legacy download approach)
        """
        if self.nifi_home:
            path = Path(self.nifi_home).expanduser()
            if path.exists():
                return path
        # Default: Maven build output from thirdparty/nifi submodule
        maven_build = Path(f"thirdparty/nifi/nifi-assembly/target/nifi-{self.nifi_version}-bin/nifi-{self.nifi_version}")
        if maven_build.exists():
            return maven_build
        # Fallback: extracted binary (legacy download approach)
        extracted = Path(f"thirdparty/nifi/nifi-{self.nifi_version}")
        if extracted.exists():
            return extracted
        return None

    def get_nifi_state_dir(self) -> Path:
        """Get NiFi state directory for writable data."""
        if self.nifi_state_dir:
            return Path(self.nifi_state_dir).expanduser()
        devenv_state = os.environ.get("DEVENV_STATE", ".devenv/state")
        return Path(devenv_state) / "nifi"

    def get_polaris_home(self) -> Optional[Path]:
        """Get Polaris home directory.

        Returns path to Polaris installation. Checks in order:
        1. Configured polaris_home path
        2. Extracted distribution from thirdparty/polaris build
        """
        if self.polaris_home:
            path = Path(self.polaris_home).expanduser()
            if path.exists():
                return path
        # Default: extracted build from thirdparty/polaris
        default = Path(f"thirdparty/polaris/polaris-bin-{self.polaris_version}")
        if default.exists():
            return default
        return None

    def get_developer_prefix(self) -> str:
        """Get developer prefix, auto-generating if not set."""
        from .identity import get_developer_prefix
        return get_developer_prefix(self)

    def get_developer_email(self) -> str:
        """Get developer email, auto-detecting if not set."""
        from .identity import get_developer_email
        return get_developer_email(self)

    def get_aws_bucket_name(self) -> str:
        """Get AWS bucket name with developer prefix."""
        from .identity import get_aws_bucket_name
        return get_aws_bucket_name(self.aws_project, self)

    def get_expected_tags(self) -> dict:
        """Get expected tags for ownership verification."""
        return {
            "ManagedBy": "opentofu",
            "Owner": self.get_developer_email(),
            "Project": self.aws_project,
        }

    def get_rke2_config(self) -> "RKE2Config":
        """Get RKE2 configuration from bootstrap config."""
        from ..k8s.rke2 import RKE2Config
        return RKE2Config(
            system_kubeconfig=self.rke2_system_kubeconfig,
            user_kubeconfig=self.rke2_user_kubeconfig,
            service_name=self.rke2_service_name,
            auto_refresh=self.rke2_auto_refresh,
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "BootstrapConfig":
        """Create from dictionary."""
        # Filter to only known fields
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)


class SettingsManager:
    """Manages loading and saving of bootstrap settings."""

    DEFAULT_CONFIG_PATH = Path(".cybersec/config.toml")

    def __init__(self, config_path: Optional[Path] = None):
        """Initialize settings manager.

        Args:
            config_path: Path to config file. Defaults to .cybersec/config.toml
        """
        self.config_path = config_path or self.DEFAULT_CONFIG_PATH
        self._config: Optional[BootstrapConfig] = None

    def exists(self) -> bool:
        """Check if config file exists."""
        return self.config_path.exists()

    def load(self) -> BootstrapConfig:
        """Load settings from disk, creating defaults if needed."""
        if self._config is not None:
            return self._config

        if self.config_path.exists():
            with open(self.config_path, "rb") as f:
                data = tomllib.load(f)
            self._config = self._parse_toml(data)
        else:
            self._config = BootstrapConfig()

        # Apply environment variable overrides
        self._apply_env_overrides()

        return self._config

    def _parse_toml(self, data: dict) -> BootstrapConfig:
        """Parse TOML data into BootstrapConfig."""
        flat = {}

        # Bootstrap section
        if "bootstrap" in data:
            flat["completed"] = data["bootstrap"].get("completed", False)
            flat["last_run"] = data["bootstrap"].get("last_run")

        # Paths section
        if "paths" in data:
            flat["minio_data_dir"] = data["paths"].get("minio_data_dir", "")
            flat["flink_home"] = data["paths"].get("flink_home", "")
            flat["flink_state_dir"] = data["paths"].get("flink_state_dir", "")
            flat["log_dir"] = data["paths"].get("log_dir", "")

        # Services section
        if "services" in data:
            services = data["services"]
            if "postgres" in services:
                flat["postgres_host"] = services["postgres"].get("host", "localhost")
                flat["postgres_port"] = services["postgres"].get("port", 5438)
                flat["postgres_database"] = services["postgres"].get("database", "iceberg")
            if "polaris" in services:
                flat["polaris_api_url"] = services["polaris"].get("api_url", "http://localhost:8181")
                flat["polaris_admin_url"] = services["polaris"].get("admin_url", "http://localhost:8182")
            if "flink" in services:
                flat["flink_url"] = services["flink"].get("url", "http://localhost:8081")
            if "minio" in services:
                flat["minio_endpoint"] = services["minio"].get("endpoint", "http://localhost:9010")
                flat["minio_console"] = services["minio"].get("console", "http://localhost:9011")
            if "iceberg_browser" in services:
                flat["iceberg_browser_port"] = services["iceberg_browser"].get("port", 5050)
            if "nifi" in services:
                flat["nifi_url"] = services["nifi"].get("url", "http://localhost:8450")
                flat["nifi_otlp_port"] = services["nifi"].get("otlp_port", 4319)

        # Catalog section
        if "catalog" in data:
            flat["catalog_name"] = data["catalog"].get("name", "cyberphy")
            flat["catalog_warehouse"] = data["catalog"].get("warehouse", "s3://cyberphy/iceberg/warehouse")

        # Credentials section
        if "credentials" in data:
            flat["polaris_client_id"] = data["credentials"].get("polaris_client_id", "admin")
            flat["polaris_client_secret"] = data["credentials"].get("polaris_client_secret", "admin")
            flat["minio_access_key"] = data["credentials"].get("minio_access_key", "admin")
            flat["minio_secret_key"] = data["credentials"].get("minio_secret_key", "admin")

        # Flink build section
        if "flink" in data:
            flat["flink_version"] = data["flink"].get("version", "1.20.1")
            flat["flink_repo"] = data["flink"].get("repo", "https://github.com/apache/flink.git")
            flat["flink_branch"] = data["flink"].get("branch", "release-1.20.1")

        # NiFi build section
        if "nifi" in data:
            flat["nifi_version"] = data["nifi"].get("version", "2.0.0")
            flat["nifi_home"] = data["nifi"].get("home", "")
            flat["nifi_state_dir"] = data["nifi"].get("state_dir", "")

        # Polaris build section
        if "polaris" in data:
            flat["polaris_version"] = data["polaris"].get("version", "1.3.0-incubating")
            flat["polaris_home"] = data["polaris"].get("home", "")

        # Paths section - NiFi paths
        if "paths" in data:
            if "nifi_home" in data["paths"]:
                flat["nifi_home"] = data["paths"]["nifi_home"]
            if "nifi_state_dir" in data["paths"]:
                flat["nifi_state_dir"] = data["paths"]["nifi_state_dir"]

        # UI section
        if "ui" in data:
            flat["ui_theme"] = data["ui"].get("theme", "nord")

        # FSN section
        if "fsn" in data:
            flat["fsn_default_mode"] = data["fsn"].get("default_mode", "cloudtrail")
            flat["fsn_remember_mode"] = data["fsn"].get("remember_mode", True)
            flat["fsn_iceberg_auto_refresh"] = data["fsn"].get("iceberg_auto_refresh", False)
            flat["fsn_iceberg_refresh_interval"] = data["fsn"].get("iceberg_refresh_interval", 60)

        # K8s section
        if "k8s" in data:
            k8s = data["k8s"]
            if "rke2" in k8s:
                flat["rke2_system_kubeconfig"] = k8s["rke2"].get("system_kubeconfig", "/etc/rancher/rke2/rke2.yaml")
                flat["rke2_user_kubeconfig"] = k8s["rke2"].get("user_kubeconfig", "~/.kube/rke2.yaml")
                flat["rke2_service_name"] = k8s["rke2"].get("service_name", "rke2-server")
                flat["rke2_auto_refresh"] = k8s["rke2"].get("auto_refresh", False)

        # AWS section
        if "aws" in data:
            flat["aws_profile"] = data["aws"].get("profile", "default")
            flat["aws_region"] = data["aws"].get("region", "us-east-1")
            flat["aws_project"] = data["aws"].get("project", "cybersec-dask")
            flat["developer_prefix"] = data["aws"].get("developer_prefix", "")
            flat["developer_email"] = data["aws"].get("developer_email", "")

        return BootstrapConfig.from_dict(flat)

    def _apply_env_overrides(self):
        """Apply environment variable overrides to config."""
        if self._config is None:
            return

        # Map of env vars to config fields
        env_map = {
            "CYBERSEC_POSTGRES_PORT": ("postgres_port", int),
            "CYBERSEC_POLARIS_API_URL": ("polaris_api_url", str),
            "CYBERSEC_FLINK_URL": ("flink_url", str),
            "CYBERSEC_MINIO_ENDPOINT": ("minio_endpoint", str),
            "CYBERSEC_CATALOG_NAME": ("catalog_name", str),
            "POLARIS_CLIENT_ID": ("polaris_client_id", str),
            "POLARIS_CLIENT_SECRET": ("polaris_client_secret", str),
            "AWS_ACCESS_KEY_ID": ("minio_access_key", str),
            "AWS_SECRET_ACCESS_KEY": ("minio_secret_key", str),
            "FLINK_HOME": ("flink_home", str),
            "MINIO_DATA_DIR": ("minio_data_dir", str),
            "NIFI_HOME": ("nifi_home", str),
            "NIFI_STATE_DIR": ("nifi_state_dir", str),
            "POLARIS_HOME": ("polaris_home", str),
            # RKE2 configuration
            "CYBERSEC_RKE2_SYSTEM_KUBECONFIG": ("rke2_system_kubeconfig", str),
            "CYBERSEC_RKE2_USER_KUBECONFIG": ("rke2_user_kubeconfig", str),
            # AWS configuration
            "AWS_PROFILE": ("aws_profile", str),
            "AWS_REGION": ("aws_region", str),
            "CYBERSEC_AWS_PROJECT": ("aws_project", str),
            "CYBERSEC_DEVELOPER_PREFIX": ("developer_prefix", str),
            "CYBERSEC_DEVELOPER_EMAIL": ("developer_email", str),
        }

        for env_var, (field_name, converter) in env_map.items():
            value = os.environ.get(env_var)
            if value is not None:
                setattr(self._config, field_name, converter(value))

    def save(self, config: Optional[BootstrapConfig] = None):
        """Save settings to disk."""
        if config:
            self._config = config

        if self._config is None:
            self._config = BootstrapConfig()

        self.config_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert to nested TOML structure
        data = self._to_toml_dict()

        with open(self.config_path, "wb") as f:
            tomli_w.dump(data, f)

    def _to_toml_dict(self) -> dict:
        """Convert config to nested TOML structure."""
        if self._config is None:
            return {}

        return {
            "bootstrap": {
                "completed": self._config.completed,
                "last_run": self._config.last_run or "",  # TOML can't serialize None
            },
            "paths": {
                "minio_data_dir": self._config.minio_data_dir,
                "flink_home": self._config.flink_home,
                "flink_state_dir": self._config.flink_state_dir,
                "log_dir": self._config.log_dir,
            },
            "services": {
                "postgres": {
                    "host": self._config.postgres_host,
                    "port": self._config.postgres_port,
                    "database": self._config.postgres_database,
                },
                "polaris": {
                    "api_url": self._config.polaris_api_url,
                    "admin_url": self._config.polaris_admin_url,
                },
                "flink": {
                    "url": self._config.flink_url,
                },
                "minio": {
                    "endpoint": self._config.minio_endpoint,
                    "console": self._config.minio_console,
                },
                "iceberg_browser": {
                    "port": self._config.iceberg_browser_port,
                },
                "nifi": {
                    "url": self._config.nifi_url,
                    "otlp_port": self._config.nifi_otlp_port,
                },
            },
            "catalog": {
                "name": self._config.catalog_name,
                "warehouse": self._config.catalog_warehouse,
            },
            "credentials": {
                "polaris_client_id": self._config.polaris_client_id,
                "polaris_client_secret": self._config.polaris_client_secret,
                "minio_access_key": self._config.minio_access_key,
                "minio_secret_key": self._config.minio_secret_key,
            },
            "flink": {
                "version": self._config.flink_version,
                "repo": self._config.flink_repo,
                "branch": self._config.flink_branch,
            },
            "nifi": {
                "version": self._config.nifi_version,
                "home": self._config.nifi_home,
                "state_dir": self._config.nifi_state_dir,
            },
            "polaris": {
                "version": self._config.polaris_version,
                "home": self._config.polaris_home,
            },
            "ui": {
                "theme": self._config.ui_theme,
            },
            "fsn": {
                "default_mode": self._config.fsn_default_mode,
                "remember_mode": self._config.fsn_remember_mode,
                "iceberg_auto_refresh": self._config.fsn_iceberg_auto_refresh,
                "iceberg_refresh_interval": self._config.fsn_iceberg_refresh_interval,
            },
            "k8s": {
                "rke2": {
                    "system_kubeconfig": self._config.rke2_system_kubeconfig,
                    "user_kubeconfig": self._config.rke2_user_kubeconfig,
                    "service_name": self._config.rke2_service_name,
                    "auto_refresh": self._config.rke2_auto_refresh,
                },
            },
            "aws": {
                "profile": self._config.aws_profile,
                "region": self._config.aws_region,
                "project": self._config.aws_project,
                "developer_prefix": self._config.developer_prefix,
                "developer_email": self._config.developer_email,
            },
        }

    def update(self, **kwargs) -> BootstrapConfig:
        """Update specific settings."""
        config = self.load()
        for key, value in kwargs.items():
            if hasattr(config, key):
                setattr(config, key, value)
        self.save(config)
        return config

    @property
    def config(self) -> BootstrapConfig:
        """Get current config, loading if needed."""
        return self.load()
