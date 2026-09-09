"""Cyberphy Health Check System with FMEA Integration.

This module provides health diagnostics for the cybersec data pipeline,
using FMEA (Failure Mode and Effects Analysis) for risk-based prioritization.

Usage:
    from cybersec.health import HealthRunner, HealthContext
    from cybersec.health.catalog import FAILURE_MODES

    runner = HealthRunner()
    ctx = HealthContext(config=bootstrap_config)
    report = await runner.run_all(ctx)
    print(report.to_dict())
"""

from .models import (
    AutomationLevel,
    CheckResult,
    CheckStatus,
    EscalationTier,
    FailureMode,
    HealthContext,
    HealthReport,
    RPNScore,
)

__all__ = [
    "AutomationLevel",
    "CheckResult",
    "CheckStatus",
    "EscalationTier",
    "FailureMode",
    "HealthContext",
    "HealthReport",
    "RPNScore",
]
