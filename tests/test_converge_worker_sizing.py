"""Unit tests for converge 0.5.0 surgical worker sizing helpers.

No cluster required — pure functions over mocked Ctx / CR dicts.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# Engine is stdlib-only and lives under zarf/ (not the cybersec package).
_ZARF = Path(__file__).resolve().parents[1] / "zarf"
if str(_ZARF) not in sys.path:
    sys.path.insert(0, str(_ZARF))

from converge import catalog as cat  # noqa: E402
from converge.kube import Ctx, _mem_to_gib  # noqa: E402


def _ctx(s3=None, mem_gib=64.0, schedulable=1):
    ctx = MagicMock(spec=Ctx)
    ctx.s3 = dict(s3 or {})
    ctx._cap_cache = {
        "ready_nodes": schedulable,
        "schedulable_nodes": schedulable,
        "total_mem_gib": mem_gib,
    }
    ctx.node_capacity.return_value = ctx._cap_cache
    return ctx


class TestNormalizeAliases:
    def test_mem_limit_to_memory(self):
        ctx = _ctx({"DASK_WORKER_MEM_LIMIT": "28Gi"})
        cat._normalize_worker_aliases(ctx)
        assert ctx.s3["DASK_WORKER_MEMORY"] == "28Gi"

    def test_memory_wins_over_limit(self):
        ctx = _ctx({"DASK_WORKER_MEMORY": "6Gi", "DASK_WORKER_MEM_LIMIT": "28Gi"})
        cat._normalize_worker_aliases(ctx)
        assert ctx.s3["DASK_WORKER_MEMORY"] == "6Gi"

    def test_mem_request_fills_memory_when_empty(self):
        ctx = _ctx({"DASK_WORKER_MEM_REQUEST": "4Gi"})
        cat._normalize_worker_aliases(ctx)
        assert ctx.s3["DASK_WORKER_MEMORY"] == "4Gi"


class TestArgHelpers:
    def test_arg_after(self):
        args = ["dask-worker", "--nthreads", "2", "--memory-limit", "6Gi"]
        assert cat._arg_after(args, "--nthreads") == "2"
        assert cat._arg_after(args, "--memory-limit") == "6Gi"
        assert cat._arg_after(args, "--missing") is None

    def test_set_arg_replace(self):
        args = ["dask-worker", "--nthreads", "2"]
        out = cat._set_arg(args, "--nthreads", "4")
        assert cat._arg_after(out, "--nthreads") == "4"
        assert out[0] == "dask-worker"

    def test_set_arg_insert(self):
        args = ["dask-worker"]
        out = cat._set_arg(args, "--nthreads", "2")
        assert out == ["dask-worker", "--nthreads", "2"]


class TestNormalizeQty:
    def test_cpu_and_mem(self):
        assert cat._normalize_k8s_qty("2") == cat._normalize_k8s_qty("2.0")
        assert cat._normalize_k8s_qty("6Gi") == cat._normalize_k8s_qty("6gi")
        assert cat._normalize_k8s_qty("2000m") == cat._normalize_k8s_qty("2")


class TestMemFit:
    def test_fat_node_many_workers(self):
        ctx = _ctx(mem_gib=256.0)
        # (256 - 8) / 6 ≈ 41
        assert cat._mem_fit_workers(ctx, "6Gi") == 41

    def test_tiny_node_at_least_one(self):
        ctx = _ctx(mem_gib=10.0)
        assert cat._mem_fit_workers(ctx, "6Gi") == 1

    def test_mem_to_gib_roundtrip(self):
        assert _mem_to_gib("6Gi") == 6.0
        assert _mem_to_gib("1024Mi") == 1.0


class TestTargetReplicas:
    def test_explicit_desired_capped_by_mem(self):
        ctx = _ctx(s3={"DASK_WORKER_REPLICAS": "32", "DASK_WORKER_MEMORY": "6Gi"},
                   mem_gib=64.0)
        desired = cat._desired_worker_sizing(ctx)
        # (64-8)/6 = 9
        target = cat._target_worker_replicas(ctx, desired, {"replicas": 4, "memory": "6Gi"})
        assert target == 9

    def test_pending_shrinks_to_running(self):
        ctx = _ctx(s3={"DASK_WORKER_REPLICAS": "8", "DASK_WORKER_MEMORY": "6Gi"},
                   mem_gib=256.0)
        desired = cat._desired_worker_sizing(ctx)
        live = {"replicas": 8, "memory": "6Gi"}
        target = cat._target_worker_replicas(
            ctx, desired, live, pending_count=5, non_pending_count=3)
        assert target == 3

    def test_no_explicit_does_not_scale_up(self):
        ctx = _ctx(s3={}, mem_gib=256.0)
        desired = cat._desired_worker_sizing(ctx)
        assert desired["replicas"] is None
        live = {"replicas": 2, "memory": "6Gi"}
        target = cat._target_worker_replicas(ctx, desired, live)
        assert target == 2  # preserve, don't force package default 4


class TestDrifts:
    def test_replicas_and_sizing(self):
        live = {"replicas": 1, "nthreads": "2", "cpu": "2", "memory": "6Gi",
                "mem_arg": "6Gi"}
        desired = {"replicas": 8, "nthreads": "4", "cpu": "4", "memory": "28Gi",
                   "mem_request": None}
        drifts = cat._worker_sizing_drifts(live, desired, target_replicas=8)
        assert any(d.startswith("replicas") for d in drifts)
        assert any("nthreads" in d for d in drifts)
        assert any(d.startswith("memory ") for d in drifts)

    def test_unset_sizing_not_forced(self):
        live = {"replicas": 4, "nthreads": "8", "cpu": "8", "memory": "28Gi"}
        desired = {"replicas": 4, "nthreads": None, "cpu": None, "memory": None}
        drifts = cat._worker_sizing_drifts(live, desired, target_replicas=4)
        assert drifts == []


class TestDesiredFromAliases:
    def test_full_set(self):
        ctx = _ctx({
            "DASK_WORKER_REPLICAS": "16",
            "DASK_WORKER_NTHREADS": "2",
            "DASK_WORKER_CPU": "2",
            "DASK_WORKER_MEM_LIMIT": "28Gi",
            "DASK_WORKER_MEM_REQUEST": "4Gi",
        })
        d = cat._desired_worker_sizing(ctx)
        assert d["replicas"] == 16
        assert d["replicas_explicit"] is True
        assert d["nthreads"] == "2"
        assert d["cpu"] == "2"
        assert d["memory"] == "28Gi"
        assert d["mem_request"] == "4Gi"
        assert ctx.s3["DASK_WORKER_MEMORY"] == "28Gi"


class TestPatchAndRem:
    def _live_bundle(self, replicas=1, nthreads="2", cpu="2", memory="6Gi"):
        args = ["dask-worker", "--nthreads", nthreads, "--memory-limit", memory]
        container = {
            "name": "worker",
            "args": args,
            "resources": {
                "limits": {"cpu": cpu, "memory": memory},
                "requests": {"cpu": "500m", "memory": "2Gi"},
            },
        }
        worker = {
            "replicas": replicas,
            "spec": {"containers": [container], "restartPolicy": "Always"},
        }
        cr = {"spec": {"worker": worker}}
        return {
            "replicas": replicas,
            "nthreads": nthreads,
            "cpu": cpu,
            "memory": memory,
            "mem_request": "2Gi",
            "mem_arg": memory,
            "_cr": cr,
            "_worker": worker,
            "_container": container,
            "_args": args,
        }

    def test_patch_applies_sizing(self):
        ctx = _ctx()
        ctx.k.return_value = SimpleNamespace(returncode=0, stderr="", stdout="")
        live = self._live_bundle(replicas=1)
        desired = {
            "replicas": 8,
            "nthreads": "4",
            "cpu": "4",
            "memory": "28Gi",
            "mem_request": "8Gi",
        }
        changed, detail, template_changed = cat._patch_daskcluster_worker_sizing(
            ctx, live, desired, target_replicas=8)
        assert changed is True
        assert template_changed is True
        assert "replicas→8" in detail
        # Inspect patch payload
        args, kwargs = ctx.k.call_args
        argv = args[0]
        assert "patch" in argv and "daskcluster" in argv
        payload = json.loads(argv[argv.index("-p") + 1])
        w = payload["spec"]["worker"]
        assert w["replicas"] == 8
        c = w["spec"]["containers"][0]
        assert cat._arg_after(c["args"], "--nthreads") == "4"
        assert cat._arg_after(c["args"], "--memory-limit") == "28Gi"
        assert c["resources"]["limits"]["cpu"] == "4"
        assert c["resources"]["limits"]["memory"] == "28Gi"
        assert c["resources"]["requests"]["memory"] == "8Gi"

    def test_patch_noop_when_matched(self):
        ctx = _ctx()
        live = self._live_bundle(replicas=4, nthreads="2", cpu="2", memory="6Gi")
        desired = {
            "replicas": 4,
            "nthreads": "2",
            "cpu": "2",
            "memory": "6Gi",
            "mem_request": None,
        }
        changed, detail, template_changed = cat._patch_daskcluster_worker_sizing(
            ctx, live, desired, target_replicas=4)
        assert changed is False
        assert template_changed is False
        ctx.k.assert_not_called()


class TestVersion:
    def test_engine_version(self):
        from converge import __version__
        assert __version__.startswith("0.5.")
