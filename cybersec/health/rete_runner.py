"""RETE-integrated health check runner.

This module integrates the RETE hybrid inference engine with the health
check system, providing:

1. Dependency-aware execution: Skip downstream checks when upstream fails
2. Goal-directed checking: Only run checks needed to prove/disprove a goal
3. Explainability: Full reasoning traces for every conclusion
4. Cost optimization: Run cheapest checks first

Usage:
    runner = ReteHealthRunner()

    # Run with dependency awareness
    report = await runner.run_all(ctx)

    # Run toward a specific goal
    result = await runner.run_for_goal("pyflink_ready", ctx)

    # Get explanation for current state
    explanation = runner.explain_goal("full_pipeline_ready")
"""

import asyncio
from typing import Any, Callable
from dataclasses import dataclass, field

from ..rete import (
    HybridEngine,
    Goal,
    Fact,
    Rule,
    Action,
    when,
    then,
    ProofStatus,
    GapAnalysis,
    Explanation,
    ProofNode,
)
from ..rete.health import HEALTH_RULES, CHECK_DEPENDENCIES
from .models import CheckResult, HealthContext, HealthReport, CheckStatus
from .catalog import CATEGORIES, get_failure_mode
from .checks import iceberg, flink, infra, pyflink


# Check function registry
CHECK_REGISTRY = {
    **iceberg.CHECKS,
    **flink.CHECKS,
    **infra.CHECKS,
    **pyflink.CHECKS,
}

# Acquisition costs (lower = cheaper/faster to check)
ACQUISITION_COSTS = {
    # Infrastructure - fast port checks
    "INFRA_001": 1,  # PostgreSQL port
    "INFRA_002": 1,  # MinIO port
    "INFRA_003": 2,  # Polaris API
    # Flink - API calls
    "FLINK_001": 2,  # Cluster overview
    "FLINK_002": 3,  # TaskManager status
    "FLINK_003": 3,  # Job status
    # Iceberg - database queries
    "ICE_001": 5,  # Catalog connection
    "ICE_002": 5,  # Table exists
    "ICE_003": 10,  # Orphan files (expensive scan)
    # PyFlink - file system checks
    "PYFLINK_001": 2,  # PyFlink installed
    "PYFLINK_002": 3,  # Python path
    "PYFLINK_011": 3,  # Iceberg JARs
    "PYFLINK_012": 3,  # Flink runtime JAR
    "PYFLINK_013": 1,  # Git submodules
    # Data - expensive queries
    "DATA_001": 20,  # Snapshot accumulation
}


@dataclass
class ReteHealthResult:
    """Result from RETE-aware health check run."""

    goal_id: str
    status: ProofStatus
    checks_run: list[str]
    checks_skipped: list[str]
    skip_reasons: dict[str, str]
    issues: list[dict[str, Any]]
    explanation: Explanation | None = None
    proof_tree: ProofNode | None = None
    report: HealthReport | None = None


class ReteHealthRunner:
    """
    RETE-integrated health check runner.

    Combines the hybrid inference engine with health check execution
    for intelligent, dependency-aware diagnostics.
    """

    def __init__(self):
        self.engine = HybridEngine()
        self.check_registry = CHECK_REGISTRY
        self._setup_engine()

    def _setup_engine(self):
        """Configure the hybrid engine with rules and goals."""
        # Add forward chaining rules
        for rule in HEALTH_RULES:
            self.engine.add_rule(rule)

        # Add goals for common diagnostic scenarios
        self._add_diagnostic_goals()

        # Set acquisition costs
        for check_id, cost in ACQUISITION_COSTS.items():
            self.engine.set_acquisition_cost(f"check.{check_id}.is_ok", cost)
            self.engine.set_acquisition_cost(f"check.{check_id}.status", cost)

    def _add_diagnostic_goals(self):
        """Add pre-configured diagnostic goals."""
        # Infrastructure health
        self.engine.add_goal(Goal(
            "infrastructure_healthy",
            conditions=[
                ("service.postgres.healthy", "==", True),
                ("service.minio.healthy", "==", True),
                ("service.polaris.healthy", "==", True),
            ],
            description="All infrastructure services are healthy",
            priority=100,
        ))

        # Flink cluster ready
        self.engine.add_goal(Goal(
            "flink_cluster_ready",
            conditions=[
                ("service.flink.healthy", "==", True),
                ("check.FLINK_001.is_ok", "==", True),
                ("check.FLINK_002.is_ok", "==", True),
            ],
            description="Flink cluster is ready for job submission",
            priority=90,
        ))

        # PyFlink environment ready
        self.engine.add_goal(Goal(
            "pyflink_ready",
            conditions=[
                ("service.flink.healthy", "==", True),
                ("check.PYFLINK_001.is_ok", "==", True),
                ("check.PYFLINK_011.is_ok", "==", True),
                ("check.PYFLINK_012.is_ok", "==", True),
                ("check.PYFLINK_013.is_ok", "==", True),
            ],
            description="PyFlink environment is ready for job submission",
            priority=80,
        ))

        # Iceberg catalog ready
        self.engine.add_goal(Goal(
            "iceberg_catalog_ready",
            conditions=[
                ("service.postgres.healthy", "==", True),
                ("service.polaris.healthy", "==", True),
                ("check.ICE_001.is_ok", "==", True),
                ("check.ICE_002.is_ok", "==", True),
            ],
            description="Iceberg catalog is accessible and configured",
            priority=85,
        ))

        # Full pipeline ready
        self.engine.add_goal(Goal(
            "full_pipeline_ready",
            conditions=[
                ("service.postgres.healthy", "==", True),
                ("service.minio.healthy", "==", True),
                ("service.polaris.healthy", "==", True),
                ("service.flink.healthy", "==", True),
                ("check.ICE_002.is_ok", "==", True),
                ("check.PYFLINK_011.is_ok", "==", True),
                ("check.PYFLINK_012.is_ok", "==", True),
            ],
            description="Full data pipeline is ready (Flink + Iceberg + Infrastructure)",
            priority=70,
        ))

        # E2E verification ready
        self.engine.add_goal(Goal(
            "e2e_ready",
            conditions=[
                ("service.postgres.healthy", "==", True),
                ("service.minio.healthy", "==", True),
                ("service.polaris.healthy", "==", True),
                ("service.flink.healthy", "==", True),
                ("check.FLINK_002.is_ok", "==", True),
                ("check.ICE_002.is_ok", "==", True),
                ("check.PYFLINK_011.is_ok", "==", True),
                ("check.PYFLINK_012.is_ok", "==", True),
                ("check.PYFLINK_013.is_ok", "==", True),
            ],
            description="Environment is ready for E2E verification",
            priority=60,
        ))

    async def run_all(self, ctx: HealthContext) -> HealthReport:
        """
        Run all health checks with dependency awareness.

        Uses RETE rules to skip downstream checks when upstream fails.
        """
        # First, check infrastructure to populate service facts
        await self._check_infrastructure(ctx)

        # Use backward chaining to determine which checks to run
        # Goal: run as many checks as possible while respecting dependencies
        checks_to_run = self._plan_checks()

        results: dict[str, list[CheckResult]] = {}

        for category, check_ids in CATEGORIES.items():
            category_results = []

            for check_id in check_ids:
                if check_id in checks_to_run["skip"]:
                    reason = checks_to_run["skip_reasons"].get(check_id, "Dependency not met")
                    category_results.append(CheckResult.skipped(
                        f"Skipped: {reason}",
                        check_id=check_id,
                    ))
                    continue

                check_fn = self.check_registry.get(check_id)
                if check_fn:
                    try:
                        result = await check_fn(ctx)
                        category_results.append(result)
                        # Update engine with result
                        self._record_check_result(check_id, result)
                    except Exception as e:
                        result = CheckResult.error(f"Check {check_id} failed: {e}", check_id=check_id)
                        category_results.append(result)
                        self._record_check_result(check_id, result)

            results[category] = category_results

        return HealthReport.from_results(results)

    async def run_for_goal(
        self,
        goal_id: str,
        ctx: HealthContext,
        max_checks: int = 20,
    ) -> ReteHealthResult:
        """
        Run checks needed to prove/disprove a specific goal.

        Uses backward chaining to identify minimum checks needed,
        then runs them in cost-optimized order.
        """
        checks_run = []
        checks_skipped = []
        skip_reasons = {}
        issues = []

        # First check infrastructure
        await self._check_infrastructure(ctx)

        # Analyze gaps for the goal
        gaps = self.engine.analyze_gaps(goal_id)

        # Acquire missing facts by running checks
        for i, acquisition in enumerate(gaps.acquisition_plan):
            if i >= max_checks:
                break

            fact_pattern = acquisition["fact_pattern"]
            check_id = self._pattern_to_check_id(fact_pattern)

            if not check_id:
                continue

            # Check dependencies
            deps = CHECK_DEPENDENCIES.get(check_id, [])
            deps_met = all(
                self._is_check_ok(dep) for dep in deps
            )

            if not deps_met:
                checks_skipped.append(check_id)
                skip_reasons[check_id] = f"Dependency not met: {deps}"
                continue

            # Run the check
            check_fn = self.check_registry.get(check_id)
            if check_fn:
                try:
                    result = await check_fn(ctx)
                    checks_run.append(check_id)
                    self._record_check_result(check_id, result)

                    if result.status in (CheckStatus.CRITICAL, CheckStatus.ERROR):
                        fm = get_failure_mode(check_id)
                        if fm:
                            issues.append({
                                "failure_mode_id": check_id,
                                "name": fm.name,
                                "severity": result.status.value,
                                "message": result.message,
                                "rpn": fm.calculate_rpn().rpn,
                            })
                except Exception as e:
                    checks_run.append(check_id)
                    self._record_check_result(
                        check_id,
                        CheckResult.error(str(e), check_id=check_id)
                    )

            # Re-evaluate goal
            gaps = self.engine.analyze_gaps(goal_id)
            if gaps.status != ProofStatus.UNKNOWN:
                break  # Goal is now proven or disproven

        # Get final explanation
        explanation = self.engine.explain_goal(goal_id)
        proof_tree = self.engine.get_proof_tree(goal_id)

        return ReteHealthResult(
            goal_id=goal_id,
            status=gaps.status,
            checks_run=checks_run,
            checks_skipped=checks_skipped,
            skip_reasons=skip_reasons,
            issues=issues,
            explanation=explanation,
            proof_tree=proof_tree,
        )

    async def _check_infrastructure(self, ctx: HealthContext):
        """Check infrastructure services and populate service facts."""
        import socket
        import httpx

        # PostgreSQL
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(("localhost", ctx.postgres_port))
            sock.close()
            healthy = result == 0
        except Exception:
            healthy = False

        self.engine.assert_fact(Fact("service", "postgres", healthy=healthy, port=ctx.postgres_port))

        # Local S3 (RustFS; fact name remains "minio" for rete compatibility)
        healthy = False
        try:
            base = ctx.minio_endpoint.rstrip("/")
            async with httpx.AsyncClient(timeout=5.0) as client:
                for path in ("/health", "/minio/health/live"):
                    try:
                        resp = await client.get(f"{base}{path}")
                        if resp.status_code == 200:
                            healthy = True
                            break
                    except Exception:
                        continue
        except Exception:
            healthy = False

        self.engine.assert_fact(Fact("service", "minio", healthy=healthy))

        # Polaris
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{ctx.polaris_url}/api/catalog/v1/config")
                healthy = resp.status_code in (200, 401)  # 401 is ok, means API is up
        except Exception:
            healthy = False

        self.engine.assert_fact(Fact("service", "polaris", healthy=healthy))

        # Flink
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{ctx.flink_url}/overview")
                healthy = resp.status_code == 200
        except Exception:
            healthy = False

        self.engine.assert_fact(Fact("service", "flink", healthy=healthy))

    def _plan_checks(self) -> dict[str, Any]:
        """Plan which checks to run based on current state."""
        from ..rete.health import HealthRuleEngine

        # Use the health rule engine for planning
        planner = HealthRuleEngine()

        # Copy service facts
        for fact_key, fact in self.engine.forward_engine.facts.items():
            if fact.fact_type == "service":
                planner.set_service_health(
                    fact.fact_id,
                    healthy=fact.attributes.get("healthy", False),
                    port=fact.attributes.get("port"),
                )

        plan = planner.plan_checks()

        return {
            "run": plan.checks_to_run,
            "skip": plan.checks_to_skip,
            "skip_reasons": plan.skip_reasons,
            "order": plan.execution_order,
        }

    def _record_check_result(self, check_id: str, result: CheckResult):
        """Record a check result in the engine."""
        is_ok = result.status == CheckStatus.OK
        is_critical = result.status in (CheckStatus.CRITICAL, CheckStatus.ERROR)

        self.engine.assert_fact(Fact(
            "check",
            check_id,
            is_ok=is_ok,
            is_critical=is_critical,
            status=result.status.value,
            message=result.message,
        ))

    def _is_check_ok(self, check_id: str) -> bool:
        """Check if a check has passed."""
        fact_key = f"check.{check_id}"
        fact = self.engine.forward_engine.facts.get(fact_key)
        if fact:
            return fact.attributes.get("is_ok", False)
        return False  # Unknown = not ok

    def _pattern_to_check_id(self, pattern: str) -> str | None:
        """Convert a fact pattern to a check ID."""
        # Pattern like "check.FLINK_001.is_ok" -> "FLINK_001"
        parts = pattern.split(".")
        if len(parts) >= 2 and parts[0] == "check":
            return parts[1]
        return None

    # === Explainability Methods ===

    def explain_goal(self, goal_id: str) -> Explanation:
        """Get explanation for a goal's current status."""
        return self.engine.explain_goal(goal_id)

    def analyze_gaps(self, goal_id: str) -> GapAnalysis:
        """Analyze what's missing to prove a goal."""
        return self.engine.analyze_gaps(goal_id)

    def get_proof_tree(self, goal_id: str) -> ProofNode:
        """Get visual proof tree for a goal."""
        return self.engine.get_proof_tree(goal_id)

    def what_if(self, goal_id: str, hypothetical_checks: list[str]) -> Explanation:
        """
        What-if analysis: what would happen if these checks passed?

        Args:
            goal_id: Goal to analyze
            hypothetical_checks: Check IDs to assume pass

        Returns:
            Explanation with hypothetical outcome
        """
        hypothetical_facts = [
            Fact("check", check_id, is_ok=True, status="ok")
            for check_id in hypothetical_checks
        ]
        return self.engine.what_if(goal_id, hypothetical_facts)

    def suggest_next_check(self, goal_id: str) -> dict[str, Any] | None:
        """Suggest the best next check to run toward a goal."""
        suggestion = self.engine.suggest_next_action(goal_id)
        if suggestion and suggestion.get("action") == "acquire":
            check_id = self._pattern_to_check_id(suggestion["fact_pattern"])
            if check_id:
                fm = get_failure_mode(check_id)
                return {
                    "check_id": check_id,
                    "name": fm.name if fm else check_id,
                    "cost": suggestion.get("cost", 1),
                    "remaining_unknowns": suggestion.get("remaining_unknowns", 0),
                    "description": fm.description if fm else "",
                }
        return suggestion

    def list_goals(self) -> list[dict[str, Any]]:
        """List all available diagnostic goals."""
        goals = []
        for goal_id, goal in self.engine.backward_engine.goals.items():
            status = self.engine.backward_engine.evaluate_goal(goal_id)
            goals.append({
                "goal_id": goal_id,
                "description": goal.description,
                "status": status.value,
                "conditions_count": len(goal.conditions),
            })
        return goals


# === Singleton instance ===

_rete_runner: ReteHealthRunner | None = None


def get_rete_runner() -> ReteHealthRunner:
    """Get the global RETE health runner instance."""
    global _rete_runner
    if _rete_runner is None:
        _rete_runner = ReteHealthRunner()
    return _rete_runner


async def run_health_check_with_rete(
    ctx: HealthContext,
    goal: str | None = None,
) -> HealthReport | ReteHealthResult:
    """
    Run health checks with RETE inference.

    Args:
        ctx: Health context
        goal: Optional goal to work toward (e.g., "pyflink_ready")

    Returns:
        HealthReport or ReteHealthResult depending on mode
    """
    runner = get_rete_runner()

    if goal:
        return await runner.run_for_goal(goal, ctx)
    else:
        return await runner.run_all(ctx)
