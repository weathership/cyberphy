"""Bootstrap service - main orchestrator for bootstrap operations.

This is the central service that all interfaces (CLI, MCP, Web) use to perform
bootstrap operations. It ensures consistent behavior across all interfaces.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, AsyncIterator, Callable, Any
import asyncio
import socket
import subprocess
import shutil

from .config import BootstrapConfig, SettingsManager
from .state import BootstrapState, BootstrapPhase, TaskStatus
from .events import EventEmitter, EventType, BootstrapEvent, PromptOption, EventCollector


class BootstrapService:
    """Main service for bootstrap operations.

    Provides unified API for:
    - Environment assessment
    - Configuration management
    - Service health checks
    - Bootstrap execution
    - Status reporting
    """

    def __init__(
        self,
        config: Optional[BootstrapConfig] = None,
        settings_manager: Optional[SettingsManager] = None,
        project_root: Optional[Path] = None,
    ):
        """Initialize bootstrap service.

        Args:
            config: Bootstrap configuration. If None, loads from settings.
            settings_manager: Settings manager for persistence.
            project_root: Project root directory. Defaults to cwd.
        """
        self.settings_manager = settings_manager or SettingsManager()
        self._config = config
        self.emitter = EventEmitter()
        self.state = BootstrapState()
        self._project_root = project_root or Path.cwd()

        # Callback for handling user prompts (set by interface)
        self._prompt_handler: Optional[Callable[[str, list[PromptOption], bool], str]] = None

    @property
    def project_root(self) -> Path:
        """Get project root directory."""
        return self._project_root

    @property
    def config(self) -> BootstrapConfig:
        """Get current configuration."""
        if self._config is None:
            self._config = self.settings_manager.load()
        return self._config

    @property
    def settings(self) -> SettingsManager:
        """Get the settings manager."""
        return self.settings_manager

    def set_prompt_handler(self, handler: Callable[[str, list[PromptOption], bool], str]):
        """Set the handler for user prompts.

        The handler receives (message, options, allow_custom) and returns the selected option.
        """
        self._prompt_handler = handler

    # =========================================================================
    # Configuration Operations (info, settings)
    # =========================================================================

    def get_config(self) -> BootstrapConfig:
        """Get current bootstrap configuration."""
        return self.config

    def get_config_dict(self) -> dict:
        """Get configuration as dictionary."""
        return self.config.to_dict()

    def update_config(self, **kwargs) -> BootstrapConfig:
        """Update configuration settings."""
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        self.settings_manager.save(self.config)
        return self.config

    def config_exists(self) -> bool:
        """Check if configuration file exists (first-run detection)."""
        return self.settings_manager.exists()

    # =========================================================================
    # Status Operations (status, verify)
    # =========================================================================

    async def check_service_health(self, service_name: str) -> dict:
        """Check health of a single service."""
        checks = {
            "postgres": self._check_postgres,
            "polaris": self._check_polaris,
            "flink": self._check_flink,
            "minio": self._check_minio,
            "iceberg_browser": self._check_iceberg_browser,
            "nifi": self._check_nifi,
        }

        if service_name not in checks:
            return {"name": service_name, "status": "unknown", "message": "Unknown service"}

        return await checks[service_name]()

    async def check_all_services(self) -> list[dict]:
        """Check health of all services."""
        services = ["postgres", "polaris", "flink", "minio", "iceberg_browser", "nifi"]
        results = []

        for service in services:
            result = await self.check_service_health(service)
            # Add healthy key for CLI compatibility
            result["healthy"] = result.get("status") == "up"
            result["service"] = result.get("name", service)
            results.append(result)
            self.state.update_service_health(service, result["healthy"])

        return results

    async def _check_postgres(self) -> dict:
        """Check PostgreSQL health."""
        host = self.config.postgres_host
        port = self.config.postgres_port

        try:
            # Try to connect to port
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=5.0,
            )
            writer.close()
            await writer.wait_closed()

            return {
                "name": "postgres",
                "status": "up",
                "endpoint": f"{host}:{port}",
                "message": "PostgreSQL is responding",
            }
        except Exception as e:
            return {
                "name": "postgres",
                "status": "down",
                "endpoint": f"{host}:{port}",
                "message": str(e),
            }

    async def _check_polaris(self) -> dict:
        """Check Polaris health."""
        import httpx

        url = f"{self.config.polaris_admin_url}/q/health/ready"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=5.0)
                if response.status_code == 200:
                    data = response.json()
                    status = data.get("status", "UNKNOWN")
                    return {
                        "name": "polaris",
                        "status": "up" if status == "UP" else "degraded",
                        "endpoint": self.config.polaris_api_url,
                        "message": f"Polaris status: {status}",
                    }
                else:
                    return {
                        "name": "polaris",
                        "status": "degraded",
                        "endpoint": self.config.polaris_api_url,
                        "message": f"HTTP {response.status_code}",
                    }
        except Exception as e:
            return {
                "name": "polaris",
                "status": "down",
                "endpoint": self.config.polaris_api_url,
                "message": str(e),
            }

    async def _check_flink(self) -> dict:
        """Check Flink health."""
        import httpx

        url = f"{self.config.flink_url}/overview"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=5.0)
                if response.status_code == 200:
                    data = response.json()
                    taskmanagers = data.get("taskmanagers", 0)
                    return {
                        "name": "flink",
                        "status": "up" if taskmanagers > 0 else "degraded",
                        "endpoint": self.config.flink_url,
                        "message": f"Flink cluster: {taskmanagers} task manager(s)",
                    }
                else:
                    return {
                        "name": "flink",
                        "status": "degraded",
                        "endpoint": self.config.flink_url,
                        "message": f"HTTP {response.status_code}",
                    }
        except Exception as e:
            return {
                "name": "flink",
                "status": "down",
                "endpoint": self.config.flink_url,
                "message": str(e),
            }

    async def _check_minio(self) -> dict:
        """Check local S3 (RustFS) health.

        Wire name remains ``minio`` for CLI/MCP compatibility; probes RustFS
        ``/health`` first, then legacy MinIO ``/minio/health/live``.
        """
        import httpx

        base = self.config.minio_endpoint.rstrip("/")
        urls = [f"{base}/health", f"{base}/minio/health/live"]

        last_err: Exception | str | None = None
        try:
            async with httpx.AsyncClient() as client:
                for url in urls:
                    try:
                        response = await client.get(url, timeout=5.0)
                        if response.status_code == 200:
                            return {
                                "name": "minio",
                                "status": "up",
                                "endpoint": self.config.minio_endpoint,
                                "message": "RustFS is healthy",
                                "display_name": "rustfs",
                            }
                        last_err = f"HTTP {response.status_code} at {url}"
                    except Exception as e:
                        last_err = e
                        continue
                return {
                    "name": "minio",
                    "status": "degraded",
                    "endpoint": self.config.minio_endpoint,
                    "message": str(last_err) if last_err else "unhealthy",
                    "display_name": "rustfs",
                }
        except Exception as e:
            return {
                "name": "minio",
                "status": "down",
                "endpoint": self.config.minio_endpoint,
                "message": str(e),
                "display_name": "rustfs",
            }

    async def _check_iceberg_browser(self) -> dict:
        """Check Iceberg Browser health."""
        import httpx

        port = self.config.iceberg_browser_port
        url = f"http://localhost:{port}/"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=5.0)
                if response.status_code == 200:
                    return {
                        "name": "iceberg_browser",
                        "status": "up",
                        "endpoint": url,
                        "message": "Iceberg Browser is running",
                    }
                else:
                    return {
                        "name": "iceberg_browser",
                        "status": "degraded",
                        "endpoint": url,
                        "message": f"HTTP {response.status_code}",
                    }
        except Exception as e:
            return {
                "name": "iceberg_browser",
                "status": "down",
                "endpoint": url,
                "message": str(e),
            }

    async def _check_nifi(self) -> dict:
        """Check NiFi health."""
        import httpx

        url = f"{self.config.nifi_url}/nifi-api/system-diagnostics"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=5.0)
                if response.status_code == 200:
                    data = response.json()
                    heap_used = data.get("systemDiagnostics", {}).get("aggregateSnapshot", {}).get("usedHeap", "unknown")
                    return {
                        "name": "nifi",
                        "status": "up",
                        "endpoint": self.config.nifi_url,
                        "message": f"NiFi is healthy (heap: {heap_used})",
                    }
                else:
                    return {
                        "name": "nifi",
                        "status": "degraded",
                        "endpoint": self.config.nifi_url,
                        "message": f"HTTP {response.status_code}",
                    }
        except Exception as e:
            return {
                "name": "nifi",
                "status": "down",
                "endpoint": self.config.nifi_url,
                "message": str(e),
            }

    async def verify(self) -> dict:
        """Run all verification checks."""
        services = await self.check_all_services()
        environment = await self._verify_environment()

        # Build checks list for CLI display
        checks = []

        # Tool checks
        for tool, found in environment["tools"].items():
            checks.append({
                "name": f"Tool: {tool}",
                "passed": found,
                "message": "Found" if found else "Not found in PATH",
            })

        # Flink check
        checks.append({
            "name": "Flink Installation",
            "passed": environment["flink_installed"],
            "message": environment.get("flink_home") or "Not configured",
        })

        # Iceberg connector check
        checks.append({
            "name": "Iceberg Connector",
            "passed": environment.get("iceberg_connector_installed", False),
            "message": "Installed" if environment.get("iceberg_connector_installed") else "Missing - run bootstrap",
        })

        # Service checks
        for svc in services:
            checks.append({
                "name": f"Service: {svc['name']}",
                "passed": svc["status"] == "up",
                "message": svc.get("message", ""),
            })

        # Config check
        checks.append({
            "name": "Bootstrap Config",
            "passed": self.config_exists(),
            "message": str(self.settings_manager.config_path) if self.config_exists() else "Not created",
        })

        # Bootstrap completed check
        checks.append({
            "name": "Bootstrap Completed",
            "passed": self.config.completed,
            "message": f"Last run: {self.config.last_run}" if self.config.completed else "Not completed",
        })

        all_passed = all(c["passed"] for c in checks)

        return {
            "checks": checks,
            "all_passed": all_passed,
            "services": services,
            "environment": environment,
        }

    async def _verify_environment(self) -> dict:
        """Verify environment requirements."""
        tools = {
            "java": shutil.which("java") is not None,
            "mvn": shutil.which("mvn") is not None,
            "psql": shutil.which("psql") is not None,
            "curl": shutil.which("curl") is not None,
            "jq": shutil.which("jq") is not None,
            "git": shutil.which("git") is not None,
        }

        ports = {}
        for name, port in [
            ("postgres", self.config.postgres_port),
            ("polaris_api", 8181),
            ("polaris_admin", 8182),
            ("flink", 8081),
            ("minio", 9010),
            ("minio_console", 9011),
            ("iceberg_browser", self.config.iceberg_browser_port),
        ]:
            ports[name] = await self._is_port_available(port)

        flink_home = self.config.get_flink_home()
        flink_installed = flink_home is not None and flink_home.exists()

        # Check for Iceberg connector (any version - we build from source)
        iceberg_connector_installed = False
        if flink_installed and flink_home:
            iceberg_jars = list((flink_home / "lib").glob("iceberg-flink-runtime-1.20-*.jar"))
            aws_bundle_jars = list((flink_home / "lib").glob("iceberg-aws-bundle-*.jar"))
            iceberg_connector_installed = bool(iceberg_jars) and bool(aws_bundle_jars)

        return {
            "tools": tools,
            "ports_available": ports,
            "flink_home": str(flink_home) if flink_home else None,
            "flink_installed": flink_installed,
            "iceberg_connector_installed": iceberg_connector_installed,
        }

    async def _is_port_available(self, port: int) -> bool:
        """Check if a port is available for binding."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("localhost", port))
            return True
        except OSError:
            return False
        finally:
            sock.close()

    # =========================================================================
    # Bootstrap Execution (run)
    # =========================================================================

    async def run(
        self,
        skip_flink: bool = False,
        flink_path: Optional[str] = None,
        skip_nifi: bool = False,
        nifi_path: Optional[str] = None,
        prompt_handler: Optional[Callable[[BootstrapEvent], Any]] = None,
        dry_run: bool = False,
    ) -> AsyncIterator[BootstrapEvent]:
        """Execute the full bootstrap sequence.

        Yields BootstrapEvent objects for progress updates.

        Args:
            skip_flink: Skip Flink setup (services will fail without it)
            flink_path: Path to existing Flink installation
            skip_nifi: Skip NiFi setup
            nifi_path: Path to existing NiFi installation
            prompt_handler: Async callback for user prompts (receives BootstrapEvent, returns response)
            dry_run: If True, don't make any changes
        """
        if prompt_handler:
            self._prompt_handler = prompt_handler
        self._dry_run = dry_run
        self.state = BootstrapState()
        self.state.start()

        await self.emitter.emit_async(
            BootstrapEvent(
                event_type=EventType.BOOTSTRAP_STARTED,
                message="Starting bootstrap process",
            )
        )
        yield BootstrapEvent(
            event_type=EventType.BOOTSTRAP_STARTED,
            message="Starting bootstrap process",
        )

        try:
            # Phase 1: Environment check
            async for event in self._run_environment_check():
                yield event

            # Phase 2: Port availability
            async for event in self._run_port_check():
                yield event

            # Phase 3: Directory setup
            async for event in self._run_directory_setup():
                yield event

            # Phase 4: Git submodules
            async for event in self._run_git_submodules():
                yield event

            # Phase 5: Flink setup (with user interaction)
            if not skip_flink:
                async for event in self._run_flink_setup(flink_path):
                    yield event

            # Phase 6: Flink connectors (Iceberg, etc.)
            if not skip_flink:
                async for event in self._run_flink_connectors_setup():
                    yield event

            # Phase 7: NiFi setup
            if not skip_nifi:
                async for event in self._run_nifi_setup(nifi_path):
                    yield event

            # Phase 8: Polaris setup (build from source if needed)
            async for event in self._run_polaris_setup():
                yield event

            # Mark bootstrap as complete
            self.config.completed = True
            self.config.last_run = datetime.now().isoformat()
            self.settings_manager.save(self.config)

            self.state.complete(success=True)
            final_event = BootstrapEvent(
                event_type=EventType.BOOTSTRAP_COMPLETED,
                message="Bootstrap completed successfully",
                progress=1.0,
            )
            await self.emitter.emit_async(final_event)
            yield final_event

        except Exception as e:
            self.state.complete(success=False, error=str(e))
            error_event = BootstrapEvent(
                event_type=EventType.BOOTSTRAP_FAILED,
                message=f"Bootstrap failed: {e}",
            )
            await self.emitter.emit_async(error_event)
            yield error_event

    async def _run_environment_check(self) -> AsyncIterator[BootstrapEvent]:
        """Check required tools are installed."""
        task_id = "environment_check"
        self.state.phase = BootstrapPhase.ENVIRONMENT_CHECK
        self.state.start_task(task_id, "Checking required tools")

        yield BootstrapEvent(
            event_type=EventType.TASK_STARTED,
            task_id=task_id,
            message="Checking required tools",
        )

        required_tools = ["java", "mvn", "psql", "curl", "jq", "git"]
        missing = []

        for tool in required_tools:
            if shutil.which(tool) is None:
                missing.append(tool)

        if missing:
            self.state.complete_task(
                task_id,
                success=False,
                error=f"Missing tools: {', '.join(missing)}",
            )
            yield BootstrapEvent(
                event_type=EventType.TASK_FAILED,
                task_id=task_id,
                message=f"Missing tools: {', '.join(missing)}",
            )
            raise RuntimeError(f"Missing required tools: {', '.join(missing)}")

        self.state.complete_task(task_id, success=True, message="All required tools found")
        yield BootstrapEvent(
            event_type=EventType.TASK_COMPLETED,
            task_id=task_id,
            message="All required tools found",
            progress=1.0,
        )

    async def _run_port_check(self) -> AsyncIterator[BootstrapEvent]:
        """Check required ports are available."""
        task_id = "port_check"
        self.state.phase = BootstrapPhase.PORT_CHECK
        self.state.start_task(task_id, "Checking port availability")

        yield BootstrapEvent(
            event_type=EventType.TASK_STARTED,
            task_id=task_id,
            message="Checking port availability",
        )

        ports_to_check = [
            ("PostgreSQL", self.config.postgres_port),
            ("Polaris API", 8181),
            ("Polaris Admin", 8182),
            ("Flink", 8081),
            ("MinIO", 9010),
            ("MinIO Console", 9011),
            ("Iceberg Browser", self.config.iceberg_browser_port),
            ("NiFi", 8450),
            ("NiFi OTLP", self.config.nifi_otlp_port),
        ]

        in_use = []
        for name, port in ports_to_check:
            if not await self._is_port_available(port):
                in_use.append(f"{name} ({port})")

        if in_use:
            # Ports in use might be our own services - just warn
            self.state.complete_task(
                task_id,
                success=True,
                message=f"Ports in use (may be existing services): {', '.join(in_use)}",
            )
            yield BootstrapEvent(
                event_type=EventType.LOG_WARN,
                task_id=task_id,
                message=f"Ports in use: {', '.join(in_use)}",
            )
        else:
            self.state.complete_task(task_id, success=True, message="All ports available")

        yield BootstrapEvent(
            event_type=EventType.TASK_COMPLETED,
            task_id=task_id,
            message="Port check complete",
            progress=1.0,
        )

    async def _run_directory_setup(self) -> AsyncIterator[BootstrapEvent]:
        """Create required directories."""
        task_id = "directory_setup"
        self.state.phase = BootstrapPhase.DIRECTORY_SETUP
        self.state.start_task(task_id, "Creating directories")

        yield BootstrapEvent(
            event_type=EventType.TASK_STARTED,
            task_id=task_id,
            message="Creating directories",
        )

        directories = [
            self.config.get_minio_data_dir(),
            self.config.get_flink_state_dir(),
            self.config.get_nifi_state_dir(),
            self.config.get_log_dir(),
            Path(".cybersec"),
        ]

        for dir_path in directories:
            dir_path.mkdir(parents=True, exist_ok=True)
            yield BootstrapEvent(
                event_type=EventType.LOG_INFO,
                task_id=task_id,
                message=f"Created: {dir_path}",
            )

        self.state.complete_task(task_id, success=True, message="Directories created")
        yield BootstrapEvent(
            event_type=EventType.TASK_COMPLETED,
            task_id=task_id,
            message="Directories created",
            progress=1.0,
        )

    async def _run_git_submodules(self) -> AsyncIterator[BootstrapEvent]:
        """Initialize git submodules if needed using the submodules module."""
        task_id = "git_submodules"
        self.state.phase = BootstrapPhase.GIT_SUBMODULES
        self.state.start_task(task_id, "Checking git submodules")

        yield BootstrapEvent(
            event_type=EventType.TASK_STARTED,
            task_id=task_id,
            message="Checking git submodules",
        )

        from .submodules import prepare_all_submodules, get_submodule_status

        # Get current status
        status = get_submodule_status()
        all_initialized = all(s["initialized"] for s in status.values())

        if all_initialized:
            status_msgs = [f"{name}: {s['branch'] or 'detached'}" for name, s in status.items() if s["initialized"]]
            self.state.complete_task(task_id, success=True, message="All submodules initialized")
            yield BootstrapEvent(
                event_type=EventType.TASK_COMPLETED,
                task_id=task_id,
                message=f"All submodules initialized ({', '.join(status_msgs)})",
                progress=1.0,
            )
            return

        # Initialize missing submodules
        yield BootstrapEvent(
            event_type=EventType.LOG_INFO,
            task_id=task_id,
            message="Initializing git submodules...",
        )

        try:
            results = prepare_all_submodules()

            # Report results
            failed = []
            for name, (success, message) in results.items():
                if success:
                    yield BootstrapEvent(
                        event_type=EventType.LOG_INFO,
                        task_id=task_id,
                        message=f"  {name}: {message}",
                    )
                else:
                    failed.append(name)
                    yield BootstrapEvent(
                        event_type=EventType.LOG_WARN,
                        task_id=task_id,
                        message=f"  {name}: {message}",
                    )

            if failed:
                self.state.complete_task(task_id, success=False, error=f"Failed: {', '.join(failed)}")
                yield BootstrapEvent(
                    event_type=EventType.TASK_FAILED,
                    task_id=task_id,
                    message=f"Submodule init failed for: {', '.join(failed)}",
                )
            else:
                self.state.complete_task(task_id, success=True, message="Git submodules initialized")
                yield BootstrapEvent(
                    event_type=EventType.TASK_COMPLETED,
                    task_id=task_id,
                    message="Git submodules initialized",
                    progress=1.0,
                )

        except Exception as e:
            self.state.complete_task(task_id, success=False, error=str(e))
            yield BootstrapEvent(
                event_type=EventType.TASK_FAILED,
                task_id=task_id,
                message=f"Git submodule init failed: {e}",
            )

    async def _run_flink_setup(self, flink_path: Optional[str] = None) -> AsyncIterator[BootstrapEvent]:
        """Set up Flink - either use existing or build from source."""
        task_id = "flink_setup"
        self.state.phase = BootstrapPhase.FLINK_SETUP
        self.state.start_task(task_id, "Setting up Flink")

        yield BootstrapEvent(
            event_type=EventType.TASK_STARTED,
            task_id=task_id,
            message="Setting up Flink",
        )

        # Check if Flink is already available
        existing_flink = self.config.get_flink_home()
        if existing_flink and existing_flink.exists():
            self.config.flink_home = str(existing_flink)
            self.state.complete_task(task_id, success=True, message=f"Using existing Flink: {existing_flink}")
            yield BootstrapEvent(
                event_type=EventType.TASK_COMPLETED,
                task_id=task_id,
                message=f"Using existing Flink: {existing_flink}",
                progress=1.0,
            )
            return

        # Check if user provided a path
        if flink_path:
            flink_home = Path(flink_path).expanduser()
            if flink_home.exists():
                self.config.flink_home = str(flink_home)
                self.settings_manager.save(self.config)
                self.state.complete_task(task_id, success=True, message=f"Using Flink: {flink_home}")
                yield BootstrapEvent(
                    event_type=EventType.TASK_COMPLETED,
                    task_id=task_id,
                    message=f"Using Flink: {flink_home}",
                    progress=1.0,
                )
                return
            else:
                yield BootstrapEvent(
                    event_type=EventType.LOG_WARN,
                    task_id=task_id,
                    message=f"Provided Flink path does not exist: {flink_home}",
                )

        # Flink is required - auto-build if missing
        yield BootstrapEvent(
            event_type=EventType.LOG_INFO,
            task_id=task_id,
            message="Flink not found - building from source (Flink is required)",
        )
        async for event in self._build_flink():
            yield event

    async def _build_flink(self) -> AsyncIterator[BootstrapEvent]:
        """Build Flink from source."""
        task_id = "flink_build"
        self.state.start_task(task_id, f"Building Flink {self.config.flink_version}")

        yield BootstrapEvent(
            event_type=EventType.TASK_STARTED,
            task_id=task_id,
            message=f"Building Flink {self.config.flink_version} from source",
        )

        flink_dir = Path("thirdparty/flink")
        if not flink_dir.exists():
            yield BootstrapEvent(
                event_type=EventType.TASK_FAILED,
                task_id=task_id,
                message="Flink source directory not found. Run git submodule init first.",
            )
            return

        # Build Flink
        yield BootstrapEvent(
            event_type=EventType.LOG_INFO,
            task_id=task_id,
            message="Starting Maven build (this may take 10-15 minutes)...",
        )

        try:
            process = await asyncio.create_subprocess_exec(
                "mvn",
                "clean",
                "install",
                "-DskipTests",
                "-Dfast",
                "-Dscala-2.12",
                cwd=str(flink_dir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )

            # Stream output for progress
            line_count = 0
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                line_count += 1
                # Emit progress every 50 lines
                if line_count % 50 == 0:
                    yield BootstrapEvent(
                        event_type=EventType.TASK_PROGRESS,
                        task_id=task_id,
                        message=f"Building... ({line_count} lines)",
                        progress=min(0.9, line_count / 5000),  # Estimate ~5000 lines total
                    )

            await process.wait()

            if process.returncode == 0:
                # Find the built Flink
                flink_home = flink_dir / f"flink-dist/target/flink-{self.config.flink_version}-bin/flink-{self.config.flink_version}"
                if flink_home.exists():
                    self.config.flink_home = str(flink_home)
                    self.settings_manager.save(self.config)
                    self.state.complete_task(task_id, success=True, message=f"Flink built: {flink_home}")
                    yield BootstrapEvent(
                        event_type=EventType.TASK_COMPLETED,
                        task_id=task_id,
                        message=f"Flink built successfully: {flink_home}",
                        progress=1.0,
                    )
                else:
                    self.state.complete_task(task_id, success=False, error="Build succeeded but output not found")
                    yield BootstrapEvent(
                        event_type=EventType.TASK_FAILED,
                        task_id=task_id,
                        message="Build succeeded but Flink distribution not found",
                    )
            else:
                self.state.complete_task(task_id, success=False, error=f"Build failed with code {process.returncode}")
                yield BootstrapEvent(
                    event_type=EventType.TASK_FAILED,
                    task_id=task_id,
                    message=f"Maven build failed with exit code {process.returncode}",
                )

        except Exception as e:
            self.state.complete_task(task_id, success=False, error=str(e))
            yield BootstrapEvent(
                event_type=EventType.TASK_FAILED,
                task_id=task_id,
                message=f"Build failed: {e}",
            )

    async def _run_flink_connectors_setup(self) -> AsyncIterator[BootstrapEvent]:
        """Build and install Iceberg Flink connector from source.

        We build Iceberg from source (thirdparty/iceberg git submodule) to avoid
        classloader conflicts with Dropwizard metrics that occur with pre-built JARs.
        """
        task_id = "flink_connectors"
        self.state.start_task(task_id, "Setting up Flink connectors")

        yield BootstrapEvent(
            event_type=EventType.TASK_STARTED,
            task_id=task_id,
            message="Setting up Flink connectors (building Iceberg from source)",
        )

        flink_home = self.config.get_flink_home()
        if not flink_home or not flink_home.exists():
            self.state.skip_task(task_id, "Flink not installed - skipping connectors")
            yield BootstrapEvent(
                event_type=EventType.TASK_SKIPPED,
                task_id=task_id,
                message="Flink not installed - skipping connectors",
            )
            return

        lib_dir = flink_home / "lib"
        if not lib_dir.exists():
            lib_dir.mkdir(parents=True, exist_ok=True)

        # Iceberg source directory
        iceberg_dir = self.project_root / "thirdparty" / "iceberg"

        # Check if Iceberg JARs are already built and installed
        # We look for any iceberg-flink-runtime JAR (version may vary based on git tag)
        existing_jars = list(lib_dir.glob("iceberg-flink-runtime-1.20-*.jar"))
        aws_bundle_jars = list(lib_dir.glob("iceberg-aws-bundle-*.jar"))
        # S3 filesystem plugin must be in plugins/, NOT lib/ (uses classloader isolation)
        s3_plugin_dir = flink_home / "plugins" / "s3-fs-hadoop"
        s3_hadoop_jars = list(s3_plugin_dir.glob("flink-s3-fs-hadoop-*.jar")) if s3_plugin_dir.exists() else []
        # Hadoop JARs needed by Iceberg
        hadoop_common_jars = list(lib_dir.glob("hadoop-common-*.jar"))
        hdfs_client_jars = list(lib_dir.glob("hadoop-hdfs-client-*.jar"))
        if existing_jars and aws_bundle_jars and s3_hadoop_jars and hadoop_common_jars and hdfs_client_jars:
            yield BootstrapEvent(
                event_type=EventType.LOG_INFO,
                task_id=task_id,
                message=f"Already present: {existing_jars[0].name}",
            )
            self.state.complete_task(task_id, success=True, message="Flink connectors installed")
            yield BootstrapEvent(
                event_type=EventType.TASK_COMPLETED,
                task_id=task_id,
                message="Flink connectors already installed",
                progress=1.0,
            )
            return

        # Check if Iceberg submodule exists
        if not iceberg_dir.exists():
            yield BootstrapEvent(
                event_type=EventType.LOG_WARN,
                task_id=task_id,
                message="Iceberg submodule not found - run 'git submodule update --init thirdparty/iceberg'",
            )
            self.state.complete_task(task_id, success=False, error="Iceberg submodule not found")
            yield BootstrapEvent(
                event_type=EventType.TASK_FAILED,
                task_id=task_id,
                message="Iceberg submodule not found",
            )
            return

        # Build Iceberg Flink runtime and AWS bundle
        yield BootstrapEvent(
            event_type=EventType.LOG_INFO,
            task_id=task_id,
            message="Building Iceberg Flink 1.20 runtime (this may take a few minutes)...",
        )

        try:
            # Build iceberg-flink-runtime-1.20 and iceberg-aws-bundle
            build_cmd = [
                "./gradlew",
                "-PflinkVersions=1.20",
                ":iceberg-flink:iceberg-flink-runtime-1.20:shadowJar",
                ":iceberg-aws-bundle:shadowJar",
                "-x", "test",
                "-x", "integrationTest",
                "-x", "generateGitProperties",
            ]

            if self._dry_run:
                yield BootstrapEvent(
                    event_type=EventType.LOG_INFO,
                    task_id=task_id,
                    message=f"[DRY RUN] Would run: {' '.join(build_cmd)}",
                )
            else:
                process = await asyncio.create_subprocess_exec(
                    *build_cmd,
                    cwd=iceberg_dir,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )

                # Stream build output
                while True:
                    line = await process.stdout.readline()
                    if not line:
                        break
                    line_text = line.decode().strip()
                    if line_text and ("> Task" in line_text or "BUILD" in line_text):
                        yield BootstrapEvent(
                            event_type=EventType.LOG_INFO,
                            task_id=task_id,
                            message=line_text[:100],
                        )

                await process.wait()

                if process.returncode != 0:
                    raise RuntimeError(f"Iceberg build failed with exit code {process.returncode}")

            # Find and copy built JARs to Flink lib
            flink_runtime_jar = iceberg_dir / "flink/v1.20/flink-runtime/build/libs"
            aws_bundle_jar = iceberg_dir / "aws-bundle/build/libs"

            # Copy Flink runtime JAR
            for jar in flink_runtime_jar.glob("iceberg-flink-runtime-1.20-*.jar"):
                if not jar.name.endswith("-sources.jar") and not jar.name.endswith("-javadoc.jar"):
                    dest = lib_dir / jar.name
                    shutil.copy(jar, dest)
                    yield BootstrapEvent(
                        event_type=EventType.LOG_INFO,
                        task_id=task_id,
                        message=f"Installed: {jar.name}",
                    )
                    break

            # Copy AWS bundle JAR (provides S3FileIO and AWS SDK)
            for jar in aws_bundle_jar.glob("iceberg-aws-bundle-*.jar"):
                if not jar.name.endswith("-sources.jar") and not jar.name.endswith("-javadoc.jar"):
                    dest = lib_dir / jar.name
                    shutil.copy(jar, dest)
                    yield BootstrapEvent(
                        event_type=EventType.LOG_INFO,
                        task_id=task_id,
                        message=f"Installed: {jar.name}",
                    )
                    break

            # Copy Hadoop JARs from Gradle cache (populated by Iceberg build)
            # hadoop-common: Required by Iceberg FlinkCatalogFactory for Configuration class
            # hadoop-auth: Required by hadoop-common for UserGroupInformation
            # hadoop-shaded-guava: Required by hadoop-common for Maps and collections
            #   NOTE: Uses group org.apache.hadoop.thirdparty, not org.apache.hadoop
            # hadoop-hdfs-client: Required for HdfsConfiguration class
            # These are safe now that flink-s3-fs-hadoop is in plugins/ with classloader isolation
            gradle_cache = Path.home() / ".gradle" / "caches" / "modules-2" / "files-2.1"
            # (group, artifact, version)
            hadoop_jars = [
                ("org.apache.hadoop", "hadoop-common", "3.4.1"),
                ("org.apache.hadoop", "hadoop-auth", "3.4.1"),
                ("org.apache.hadoop.thirdparty", "hadoop-shaded-guava", "1.4.0"),
                ("org.apache.hadoop", "hadoop-hdfs-client", "3.4.1"),
            ]

            for group, artifact, version in hadoop_jars:
                jar_name = f"{artifact}-{version}.jar"
                dest = lib_dir / jar_name
                if dest.exists():
                    continue

                # Find in Gradle cache
                artifact_dir = gradle_cache / group / artifact / version
                if artifact_dir.exists():
                    for jar in artifact_dir.glob("*/*.jar"):
                        if jar.name == jar_name:
                            shutil.copy(jar, dest)
                            yield BootstrapEvent(
                                event_type=EventType.LOG_INFO,
                                task_id=task_id,
                                message=f"Copied from Gradle cache: {jar_name}",
                            )
                            break

            # Copy flink-python JAR from opt to lib (required for PyFlink UDFs)
            opt_dir = flink_home / "opt"
            python_jar_src = opt_dir / "flink-python-1.20.1.jar"
            python_jar_dst = lib_dir / "flink-python-1.20.1.jar"
            if python_jar_src.exists() and not python_jar_dst.exists():
                shutil.copy(python_jar_src, python_jar_dst)
                yield BootstrapEvent(
                    event_type=EventType.LOG_INFO,
                    task_id=task_id,
                    message="Copied flink-python to lib (required for PyFlink)",
                )

            # Install flink-s3-fs-hadoop as a PLUGIN (NOT in lib/)
            # This is critical: the plugin system uses isolated classloaders, which
            # prevents the "Delegation token provider s3-hadoop has multiple implementations" error
            # that occurs when the JAR is in lib/ alongside iceberg-aws-bundle
            s3_plugin_dir = flink_home / "plugins" / "s3-fs-hadoop"
            s3_jar_src = opt_dir / "flink-s3-fs-hadoop-1.20.1.jar"
            s3_jar_dst = s3_plugin_dir / "flink-s3-fs-hadoop-1.20.1.jar"
            if s3_jar_src.exists() and not s3_jar_dst.exists():
                s3_plugin_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy(s3_jar_src, s3_jar_dst)
                yield BootstrapEvent(
                    event_type=EventType.LOG_INFO,
                    task_id=task_id,
                    message="Installed flink-s3-fs-hadoop as plugin (S3 filesystem)",
                )

            # Remove from lib/ if previously installed there (causes delegation token conflict)
            s3_jar_in_lib = lib_dir / "flink-s3-fs-hadoop-1.20.1.jar"
            if s3_jar_in_lib.exists():
                s3_jar_in_lib.unlink()
                yield BootstrapEvent(
                    event_type=EventType.LOG_INFO,
                    task_id=task_id,
                    message="Removed flink-s3-fs-hadoop from lib/ (now in plugins/)",
                )

            # Only remove AWS SDK bundle if it conflicts with iceberg-aws-bundle
            # NOTE: With flink-s3-fs-hadoop in plugins/, Hadoop JARs no longer conflict
            conflicting_patterns = [
                "aws-java-sdk-bundle-*.jar",
            ]
            for pattern in conflicting_patterns:
                for jar in lib_dir.glob(pattern):
                    jar.unlink()
                    yield BootstrapEvent(
                        event_type=EventType.LOG_INFO,
                        task_id=task_id,
                        message=f"Removed conflicting JAR: {jar.name}",
                    )

            self.state.complete_task(task_id, success=True, message="Flink connectors built and installed")
            yield BootstrapEvent(
                event_type=EventType.TASK_COMPLETED,
                task_id=task_id,
                message="Flink connectors built and installed",
                progress=1.0,
            )

        except Exception as e:
            self.state.complete_task(task_id, success=False, error=str(e))
            yield BootstrapEvent(
                event_type=EventType.TASK_FAILED,
                task_id=task_id,
                message=f"Failed to build Iceberg connectors: {e}",
            )

    async def _run_nifi_setup(self, nifi_path: Optional[str] = None) -> AsyncIterator[BootstrapEvent]:
        """Set up NiFi - download binary or use existing installation."""
        task_id = "nifi_setup"
        self.state.start_task(task_id, "Setting up NiFi")

        yield BootstrapEvent(
            event_type=EventType.TASK_STARTED,
            task_id=task_id,
            message="Setting up NiFi",
        )

        # Check if NiFi is already available
        existing_nifi = self.config.get_nifi_home()
        if existing_nifi and existing_nifi.exists():
            self.config.nifi_home = str(existing_nifi)
            self.state.complete_task(task_id, success=True, message=f"Using existing NiFi: {existing_nifi}")
            yield BootstrapEvent(
                event_type=EventType.TASK_COMPLETED,
                task_id=task_id,
                message=f"Using existing NiFi: {existing_nifi}",
                progress=1.0,
            )
            return

        # Check if user provided a path
        if nifi_path:
            nifi_home = Path(nifi_path).expanduser()
            if nifi_home.exists():
                self.config.nifi_home = str(nifi_home)
                self.settings_manager.save(self.config)
                self.state.complete_task(task_id, success=True, message=f"Using NiFi: {nifi_home}")
                yield BootstrapEvent(
                    event_type=EventType.TASK_COMPLETED,
                    task_id=task_id,
                    message=f"Using NiFi: {nifi_home}",
                    progress=1.0,
                )
                return
            else:
                yield BootstrapEvent(
                    event_type=EventType.LOG_WARN,
                    task_id=task_id,
                    message=f"Provided NiFi path does not exist: {nifi_home}",
                )

        # NiFi is required - auto-download if missing
        yield BootstrapEvent(
            event_type=EventType.LOG_INFO,
            task_id=task_id,
            message="NiFi not found - downloading automatically (NiFi is required)",
        )
        async for event in self._download_nifi():
            yield event

    async def _download_nifi(self) -> AsyncIterator[BootstrapEvent]:
        """Download NiFi binary distribution."""
        task_id = "nifi_download"
        self.state.start_task(task_id, f"Downloading NiFi {self.config.nifi_version}")

        yield BootstrapEvent(
            event_type=EventType.TASK_STARTED,
            task_id=task_id,
            message=f"Downloading NiFi {self.config.nifi_version} binary",
        )

        # Run the setup script
        script_path = Path("scripts/setup_nifi_bin.sh")
        if not script_path.exists():
            yield BootstrapEvent(
                event_type=EventType.TASK_FAILED,
                task_id=task_id,
                message="NiFi setup script not found at scripts/setup_nifi_bin.sh",
            )
            return

        yield BootstrapEvent(
            event_type=EventType.LOG_INFO,
            task_id=task_id,
            message="Downloading NiFi binary (this may take a few minutes)...",
        )

        try:
            process = await asyncio.create_subprocess_exec(
                "bash",
                str(script_path),
                self.config.nifi_version,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )

            # Stream output
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                line_text = line.decode().strip()
                if line_text:
                    yield BootstrapEvent(
                        event_type=EventType.LOG_INFO,
                        task_id=task_id,
                        message=line_text,
                    )

            await process.wait()

            if process.returncode == 0:
                # Find the downloaded NiFi
                nifi_home = Path(f"thirdparty/nifi/nifi-{self.config.nifi_version}")
                if nifi_home.exists():
                    self.config.nifi_home = str(nifi_home)
                    self.settings_manager.save(self.config)
                    self.state.complete_task(task_id, success=True, message=f"NiFi downloaded: {nifi_home}")
                    yield BootstrapEvent(
                        event_type=EventType.TASK_COMPLETED,
                        task_id=task_id,
                        message=f"NiFi downloaded successfully: {nifi_home}",
                        progress=1.0,
                    )
                else:
                    self.state.complete_task(task_id, success=False, error="Download succeeded but NiFi not found")
                    yield BootstrapEvent(
                        event_type=EventType.TASK_FAILED,
                        task_id=task_id,
                        message="Download succeeded but NiFi distribution not found",
                    )
            else:
                self.state.complete_task(task_id, success=False, error=f"Download failed with code {process.returncode}")
                yield BootstrapEvent(
                    event_type=EventType.TASK_FAILED,
                    task_id=task_id,
                    message=f"Download failed with exit code {process.returncode}",
                )

        except Exception as e:
            self.state.complete_task(task_id, success=False, error=str(e))
            yield BootstrapEvent(
                event_type=EventType.TASK_FAILED,
                task_id=task_id,
                message=f"Download failed: {e}",
            )

    # =========================================================================
    # Polaris Setup
    # =========================================================================

    async def _run_polaris_setup(self) -> AsyncIterator[BootstrapEvent]:
        """Set up Polaris from thirdparty submodule - fully automatic.

        Builds Polaris from source if not already built. Uses the submodules
        module to auto-initialize the git submodule if needed.
        """
        task_id = "polaris_setup"
        self.state.phase = BootstrapPhase.POLARIS_SETUP
        self.state.start_task(task_id, "Setting up Apache Polaris")

        yield BootstrapEvent(
            event_type=EventType.TASK_STARTED,
            task_id=task_id,
            message="Setting up Apache Polaris...",
        )

        # Auto-prepare submodule (init if needed)
        from .submodules import prepare_submodule

        success, message = prepare_submodule("polaris")

        yield BootstrapEvent(
            event_type=EventType.LOG_INFO,
            task_id=task_id,
            message=message,
        )

        if not success:
            self.state.complete_task(task_id, success=False, error=message)
            yield BootstrapEvent(
                event_type=EventType.TASK_FAILED,
                task_id=task_id,
                message=message,
            )
            return

        polaris_home = self.config.get_polaris_home()

        # Check if already built
        if polaris_home and (polaris_home / "server" / "quarkus-run.jar").exists():
            self.state.complete_task(task_id, success=True, message=f"Polaris ready at {polaris_home}")
            yield BootstrapEvent(
                event_type=EventType.TASK_COMPLETED,
                task_id=task_id,
                message=f"Polaris ready at {polaris_home}",
                progress=1.0,
            )
            return

        # Build from source
        async for event in self._build_polaris():
            yield event

    async def _build_polaris(self) -> AsyncIterator[BootstrapEvent]:
        """Build Polaris from thirdparty/polaris submodule."""
        task_id = "polaris_build"
        self.state.start_task(task_id, f"Building Polaris {self.config.polaris_version}")

        yield BootstrapEvent(
            event_type=EventType.TASK_STARTED,
            task_id=task_id,
            message=f"Building Polaris {self.config.polaris_version} from source",
        )

        polaris_src = self.project_root / "thirdparty" / "polaris"

        if not polaris_src.exists():
            self.state.complete_task(task_id, success=False, error="Polaris source directory not found")
            yield BootstrapEvent(
                event_type=EventType.TASK_FAILED,
                task_id=task_id,
                message="Polaris source directory not found. Run git submodule init first.",
            )
            return

        yield BootstrapEvent(
            event_type=EventType.LOG_INFO,
            task_id=task_id,
            message="Building Polaris distribution (this may take several minutes)...",
        )

        try:
            # Build Polaris distribution using Gradle
            # :polaris-distribution:assemble creates the binary tarball
            build_cmd = [
                "./gradlew",
                ":polaris-distribution:assemble",
                "-x", "test",
                "-x", "integrationTest",
            ]

            if self._dry_run:
                yield BootstrapEvent(
                    event_type=EventType.LOG_INFO,
                    task_id=task_id,
                    message=f"[DRY RUN] Would run: {' '.join(build_cmd)}",
                )
                self.state.complete_task(task_id, success=True, message="Dry run completed")
                yield BootstrapEvent(
                    event_type=EventType.TASK_COMPLETED,
                    task_id=task_id,
                    message="[DRY RUN] Polaris build skipped",
                    progress=1.0,
                )
                return

            process = await asyncio.create_subprocess_exec(
                *build_cmd,
                cwd=polaris_src,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )

            # Stream progress
            line_count = 0
            while process.stdout:
                line = await process.stdout.readline()
                if not line:
                    break
                line_count += 1
                line_str = line.decode().strip()
                # Emit progress for significant lines
                if line_str and ("BUILD" in line_str or "> Task" in line_str or ":polaris" in line_str):
                    yield BootstrapEvent(
                        event_type=EventType.LOG_INFO,
                        task_id=task_id,
                        message=line_str[:100],
                    )
                # Emit progress every 100 lines
                if line_count % 100 == 0:
                    yield BootstrapEvent(
                        event_type=EventType.TASK_PROGRESS,
                        task_id=task_id,
                        message=f"Building... ({line_count} lines)",
                        progress=min(0.9, line_count / 3000),  # Estimate ~3000 lines
                    )

            await process.wait()

            if process.returncode != 0:
                self.state.complete_task(task_id, success=False, error=f"Build failed with exit code {process.returncode}")
                yield BootstrapEvent(
                    event_type=EventType.TASK_FAILED,
                    task_id=task_id,
                    message=f"Gradle build failed with exit code {process.returncode}",
                )
                return

            # Extract tarball
            dist_dir = polaris_src / "runtime" / "distribution" / "build" / "distributions"
            tarball = dist_dir / f"polaris-bin-{self.config.polaris_version}.tgz"

            if tarball.exists():
                import tarfile

                # Extract to thirdparty/polaris/
                extract_to = polaris_src
                yield BootstrapEvent(
                    event_type=EventType.LOG_INFO,
                    task_id=task_id,
                    message=f"Extracting {tarball.name}...",
                )

                with tarfile.open(tarball, "r:gz") as tar:
                    tar.extractall(path=extract_to)

                polaris_home = extract_to / f"polaris-bin-{self.config.polaris_version}"

                # Create wrapper scripts
                setup_script = self.project_root / "scripts" / "setup_polaris_bin.sh"
                if setup_script.exists():
                    yield BootstrapEvent(
                        event_type=EventType.LOG_INFO,
                        task_id=task_id,
                        message="Creating Polaris wrapper scripts...",
                    )
                    setup_process = await asyncio.create_subprocess_exec(
                        "bash",
                        str(setup_script),
                        str(polaris_home),
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await setup_process.wait()

                self.config.polaris_home = str(polaris_home)
                self.settings_manager.save(self.config)

                self.state.complete_task(task_id, success=True, message=f"Polaris built: {polaris_home}")
                yield BootstrapEvent(
                    event_type=EventType.TASK_COMPLETED,
                    task_id=task_id,
                    message=f"Polaris built successfully: {polaris_home}",
                    progress=1.0,
                )
            else:
                # Check for zip file instead
                zipfile_path = dist_dir / f"polaris-bin-{self.config.polaris_version}.zip"
                if zipfile_path.exists():
                    import zipfile

                    extract_to = polaris_src
                    yield BootstrapEvent(
                        event_type=EventType.LOG_INFO,
                        task_id=task_id,
                        message=f"Extracting {zipfile_path.name}...",
                    )

                    with zipfile.ZipFile(zipfile_path, "r") as zf:
                        zf.extractall(path=extract_to)

                    polaris_home = extract_to / f"polaris-bin-{self.config.polaris_version}"
                    self.config.polaris_home = str(polaris_home)
                    self.settings_manager.save(self.config)

                    self.state.complete_task(task_id, success=True, message=f"Polaris built: {polaris_home}")
                    yield BootstrapEvent(
                        event_type=EventType.TASK_COMPLETED,
                        task_id=task_id,
                        message=f"Polaris built successfully: {polaris_home}",
                        progress=1.0,
                    )
                else:
                    self.state.complete_task(task_id, success=False, error="Build succeeded but distribution not found")
                    yield BootstrapEvent(
                        event_type=EventType.TASK_FAILED,
                        task_id=task_id,
                        message="Build succeeded but Polaris distribution not found",
                    )

        except Exception as e:
            self.state.complete_task(task_id, success=False, error=str(e))
            yield BootstrapEvent(
                event_type=EventType.TASK_FAILED,
                task_id=task_id,
                message=f"Build failed: {e}",
            )

    # =========================================================================
    # Assessment (for devenv bootstrap-check process)
    # =========================================================================

    async def assess(self) -> dict:
        """Quick environment assessment for devenv bootstrap-check.

        Returns a summary suitable for display on startup.
        """
        env = await self._verify_environment()
        config_exists = self.config_exists()
        all_tools = all(env["tools"].values())
        needs_bootstrap = not config_exists or not self.config.completed

        nifi_home = self.config.get_nifi_home()
        nifi_installed = nifi_home is not None and nifi_home.exists()

        polaris_home = self.config.get_polaris_home()
        polaris_installed = polaris_home is not None and polaris_home.exists()

        iceberg_connector = env.get("iceberg_connector_installed", False)

        # Need bootstrap if config incomplete OR missing critical components
        needs_bootstrap = (
            not config_exists
            or not self.config.completed
            or not env["flink_installed"]
            or not iceberg_connector
            or not polaris_installed
        )

        return {
            "config_exists": config_exists,
            "tools": env["tools"],
            "all_tools_found": all_tools,
            "flink_installed": env["flink_installed"],
            "flink_home": env["flink_home"],
            "iceberg_connector_installed": iceberg_connector,
            "nifi_installed": nifi_installed,
            "nifi_home": str(nifi_home) if nifi_home else None,
            "polaris_installed": polaris_installed,
            "polaris_home": str(polaris_home) if polaris_home else None,
            "needs_bootstrap": needs_bootstrap,
            "ready": not needs_bootstrap and all_tools and env["flink_installed"] and iceberg_connector and polaris_installed,
            "settings_url": f"http://localhost:{self.config.iceberg_browser_port}/settings",
        }


# CLI entry point for assessment
async def main():
    """Entry point for `python -m cybersec.bootstrap.service assess`."""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "assess":
        service = BootstrapService()
        result = await service.assess()

        print("=== Environment Assessment ===")
        print(f"Config exists: {result['config_exists']}")
        print(f"Flink installed: {result['flink_installed']}")

        print("\nRequired tools:")
        for tool, found in result["tools"].items():
            status = "+" if found else "X"
            print(f"  [{status}] {tool}")

        if result["needs_bootstrap"]:
            print(f"\nBootstrap required. Visit: {result['settings_url']}")
        else:
            print("\nEnvironment ready!")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
