"""Unit tests for converge 0.5.1 partial-rollout classify + surface helpers."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_ZARF = Path(__file__).resolve().parents[1] / "zarf"
if str(_ZARF) not in sys.path:
    sys.path.insert(0, str(_ZARF))

from converge.discovery import (  # noqa: E402
    classify_helm_release,
    functional_surface,
    _deployment_progress_failed,
    _deployment_stalled_unavailable,
)
from converge.kube import Ctx  # noqa: E402
from converge import __version__  # noqa: E402


class TestClassifyHelm:
    def test_pending(self):
        assert classify_helm_release({
            "latest_status": "pending-upgrade",
            "history": {"deployed", "pending-upgrade"},
        }) == "pending"

    def test_dead(self):
        assert classify_helm_release({
            "latest_status": "failed",
            "history": {"failed"},
        }) == "dead"

    def test_interrupted(self):
        assert classify_helm_release({
            "latest_status": "failed",
            "history": {"deployed", "failed", "superseded"},
        }) == "interrupted"

    def test_ok(self):
        assert classify_helm_release({
            "latest_status": "deployed",
            "history": {"deployed"},
        }) == "ok"


class TestDeploymentStall:
    def test_progress_deadline(self):
        dep = {
            "status": {
                "conditions": [
                    {"type": "Progressing", "status": "False",
                     "reason": "ProgressDeadlineExceeded"},
                ],
            },
        }
        assert _deployment_progress_failed(dep) is True

    def test_unavailable_young_not_stalled(self):
        from datetime import datetime, timezone, timedelta
        ts = (datetime.now(timezone.utc) - timedelta(seconds=30)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        dep = {
            "metadata": {"creationTimestamp": ts},
            "spec": {"replicas": 2},
            "status": {"availableReplicas": 0},
        }
        assert _deployment_stalled_unavailable(dep) is False

    def test_unavailable_old_stalled(self):
        from datetime import datetime, timezone, timedelta
        ts = (datetime.now(timezone.utc) - timedelta(seconds=400)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        dep = {
            "metadata": {"creationTimestamp": ts},
            "spec": {"replicas": 2},
            "status": {"availableReplicas": 0},
        }
        assert _deployment_stalled_unavailable(dep) is True


class TestFunctionalSurface:
    def test_clean_slate_not_partial(self):
        ctx = MagicMock(spec=Ctx)
        ctx.get.return_value = None  # no namespaces
        s = functional_surface(ctx)
        assert s["ready"] is False
        assert s["partial"] is False  # pre-deploy, not mid-rollout

    def test_partial_when_some_ready_some_not(self):
        ctx = MagicMock(spec=Ctx)

        def get_ns(kind, name="", ns=None):
            if kind == "namespace" and name in (
                    "zarf", "dask-operator", "dask", "panel-viz", "jupyterhub"):
                return {"metadata": {"name": name}, "status": {"phase": "Active"}}
            return None

        ctx.get.side_effect = get_ns

        def pods_ready(ns, sel):
            if ns == "zarf":
                return 1, 1
            if ns == "dask-operator":
                return 1, 1
            if ns == "dask":
                return 0, 1  # scheduler not ready
            return 0, 0

        ctx.pods_ready.side_effect = pods_ready
        s = functional_surface(ctx)
        assert s["ready"] is False
        assert s["partial"] is True
        assert any("dask:" in i for i in s["issues"])


class TestRegistryReclaim:
    """matrix case 15 — Bound-but-Delete must not converge healthy."""

    def test_detect_fails_on_delete_reclaim(self):
        from converge.catalog import _det_registry_pv, REGISTRY_PV_NAME
        ctx = MagicMock(spec=Ctx)
        ctx.get.return_value = {
            "spec": {"persistentVolumeReclaimPolicy": "Delete"},
        }
        ctx.items.return_value = [
            {"status": {"phase": "Bound"}},  # would have early-passed before fix
        ]
        p = _det_registry_pv(ctx)
        assert p.ok is False
        assert "Retain" in p.detail

    def test_rem_patches_reclaim_only(self):
        from converge.catalog import _rem_registry_pv, REGISTRY_PV_NAME
        from types import SimpleNamespace
        ctx = MagicMock(spec=Ctx)
        ctx.get.return_value = {
            "spec": {"persistentVolumeReclaimPolicy": "Delete"},
        }
        ctx.k.return_value = SimpleNamespace(returncode=0, stderr="", stdout="")
        fix = _rem_registry_pv(ctx)
        assert fix.changed is True
        assert "Retain" in fix.detail
        argv = ctx.k.call_args[0][0]
        assert "patch" in argv and REGISTRY_PV_NAME in argv
        assert "Retain" in " ".join(argv)
        ctx.apply_yaml.assert_not_called()


class TestImageRetarget:
    """converge-10: recycle-only leaves CR on old tag → Ready-but-wrong loop."""

    def test_retarget_strips_zarf_suffix(self):
        from converge.catalog import _retarget_image_ref
        old = "127.0.0.1:31999/cybersec-dask:2025.2.0-e66f38622f-zarf-2560517462"
        new = _retarget_image_ref(old, "2025.2.0-85f3d9ecf5")
        assert new == "127.0.0.1:31999/cybersec-dask:2025.2.0-85f3d9ecf5"

    def test_retarget_plain(self):
        from converge.catalog import _retarget_image_ref
        old = "localhost:5555/cybersec-dask:2025.2.0-old"
        assert _retarget_image_ref(old, "2025.2.0-new") == \
            "localhost:5555/cybersec-dask:2025.2.0-new"


class TestWorkersAbsentCR:
    def test_capacity_ok_when_no_cr(self):
        from converge.catalog import _det_workers_capacity
        ctx = MagicMock(spec=Ctx)
        ctx.get.side_effect = lambda *a, **k: None  # no daskcluster
        ctx.items.return_value = []
        p = _det_workers_capacity(ctx)
        assert p.ok is True
        assert "N/A" in p.detail or "absent" in p.detail.lower()


class TestCrashLogClassify:
    def test_import_and_s3_and_scrub(self):
        from converge.catalog import _classify_log_text
        text = """
Traceback (most recent call last):
  File "app.py", line 1
ModuleNotFoundError: No module named 'h5py'
Invalid bucket name 's3:'
AWS_SECRET_ACCESS_KEY=supersecret should not appear as evidence
Address already in use
"""
        findings = _classify_log_text(text)
        ids = {f["id"] for f in findings}
        assert "import_error" in ids
        assert "s3_auth_or_bucket" in ids
        assert "port_in_use" in ids
        for f in findings:
            assert "supersecret" not in f["evidence"]


class TestVersion:
    def test_version(self):
        assert __version__.startswith("0.5.")
