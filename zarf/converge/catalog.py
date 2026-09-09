"""The invariant catalog — the single source of truth for both remediation
(``converge --apply``) and verification (``converge --verify``).

Each invariant is one target-state fact with a read-only ``detect`` and, for
Layer-B only, an idempotent ``remediate``. Ordered T0 (node) → T6 (ingress) with
explicit ``depends_on`` edges. Layer-A invariants are detect-only (CONSERVATION).

Remediations reuse the bundle's own logic where it's proven: component (re)deploys
shell to ``zarf package deploy --components=…`` (idempotent); surgical repairs
(force-finalize, claimRef PV, default-SC annotation, worker cap) are inline kubectl.
If ``zarf``/the package isn't on the host (e.g. an in-cluster Job), zarf-backed
remediations degrade to a precise manual hint instead of failing.

Every invariant carries a ``manual_hint`` from ``manual.HINTS``: copy-paste **DISCOVER**
(in-situ read-only commands) + **FIX** (the intervention). The engine report prints an
IN SITU section on MANUAL/FAILED/WOULD_FIX so operators never guess the next command.
"""
from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path
from typing import List, Optional, Tuple

from .discovery import APP_NAMESPACES, DASK_CRD_KINDS
from .kube import Ctx, _mem_to_gib
from .model import Cost, Fix, Invariant, Layer, Probe
from . import manual as _manual
from . import platform as _platform

# Package defaults (zarf.yaml) — used when env is unset on first deploy / capacity math.
_WORKER_DEFAULT_REPLICAS = 4
_WORKER_DEFAULT_NTHREADS = "2"
_WORKER_DEFAULT_CPU = "2"
_WORKER_DEFAULT_MEMORY = "6Gi"
_WORKER_DEFAULT_MEM_REQUEST = "2Gi"
# Reserve RAM for kubelet + system + scheduler + panel + hub so workers do not
# strand otel-navigator. Capacity target = floor((total − headroom) / worker_mem).
_WORKER_MEM_HEADROOM_GIB = 8.0

# Registry hostPath — non-root registry container; fsGroup does NOT chown hostPath.
REGISTRY_HOSTPATH = "/var/lib/zarf-registry"
REGISTRY_PVC_NAME = "zarf-docker-registry"
REGISTRY_PV_NAME = "zarf-registry-pv"
ZARF_NS = "zarf"

# --------------------------------------------------------------------------- #
# Layer-B remediation primitives (kubectl-only, idempotent, NEVER touch Layer A)
# --------------------------------------------------------------------------- #

REGISTRY_PV_YAML = """\
apiVersion: v1
kind: PersistentVolume
metadata:
  name: zarf-registry-pv
spec:
  capacity:
    storage: {size}
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: "{sc}"
  hostPath:
    path: /var/lib/zarf-registry
    type: DirectoryOrCreate
  claimRef:
    namespace: zarf
    name: zarf-docker-registry
"""


def _registry_pv_yaml(ctx: Ctx, storage_class: str = "") -> str:
    """The static claimRef hostPath registry PV, with storageClassName set to match the
    class the registry PVC requests ("" by default — the resilient no-default-SC path).
    Setting it to the PVC's actual class lets the PV bind statically with NO provisioner."""
    return REGISTRY_PV_YAML.format(size=ctx.registry_pv_size, sc=storage_class)


def _force_finalize_ns(ctx: Ctx, ns: str) -> bool:
    """Clear a namespace's spec.finalizers via the /finalize subresource — the only
    thing that releases a namespace wedged in Terminating. Layer-B only."""
    obj = ctx.get("namespace", ns)
    if not obj:
        return False
    obj["spec"] = {"finalizers": []}
    r = ctx.run(ctx.kubectl + ["replace", "--raw",
                f"/api/v1/namespaces/{ns}/finalize", "-f", "-"],
                input_=json.dumps(obj))
    return r.returncode == 0


def _ensure_namespace(ctx: Ctx, ns: str) -> bool:
    """Create ``ns`` if missing. Required for split-brain: force-finalize removed the
    Namespace object while PVC/Service etcd keys survive; namespaced writes then fail
    with ``namespaces \"X\" not found`` until the ns is recreated (husks re-surface —
    expected; caller must drain). Returns True if it created the ns."""
    if ctx.exists("namespace", ns):
        return False
    r = ctx.k(["create", "namespace", ns])
    return r.returncode == 0


def _release_pvc_mounts(ctx: Ctx, ns: str, pvc_name: str) -> List[str]:
    """Stop controllers/pods that keep ``kubernetes.io/pvc-protection`` alive.
    Field: deleting the PVC while the registry pod still mounts it → Terminating forever."""
    actions: List[str] = []
    # Controllers first so they stop recreating mount pods.
    for kind in ("deploy", "sts", "ds", "job"):
        r = ctx.k(["delete", kind, "--all", "-n", ns, "--wait=false", "--ignore-not-found"])
        if r.returncode == 0 and (r.stdout or "").strip():
            actions.append(f"deleted {kind} in {ns} (release PVC mounts)")
    holders = []
    for p in ctx.items("pods", ns=ns):
        for v in p.get("spec", {}).get("volumes") or []:
            claim = (v.get("persistentVolumeClaim") or {}).get("claimName")
            if claim == pvc_name:
                holders.append(p["metadata"]["name"])
                break
    for pod in holders:
        ctx.k(["delete", "pod", pod, "-n", ns,
               "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
        actions.append(f"force-deleted pod {ns}/{pod} holding PVC {pvc_name}")
    if not holders:
        # Belt: nuke all pods in ns if any still running (partial init)
        pods = ctx.items("pods", ns=ns)
        if pods:
            ctx.k(["delete", "pods", "--all", "-n", ns,
                   "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
            actions.append(f"force-deleted all pods in {ns} (PVC release)")
    return actions


def _force_delete_pvc(ctx: Ctx, ns: str, name: str) -> bool:
    """Delete a PVC and, if it lingers on the kubernetes.io/pvc-protection finalizer,
    clear the finalizer so it actually goes. A PVC's storageClassName is IMMUTABLE, so a
    registry PVC the cluster-default SC captured onto a dead provisioner can never be
    salvaged in place — it must be removed so `zarf init` recreates it clean. Layer-B:
    the registry's hostPath DATA is conserved by the Retain PV, so images survive.

    Anticipates: (1) mounts holding the protection finalizer, (2) ns absent (split-brain)
    → re-create ns so the patch can address the object, (3) merge+json finalizer strip."""
    if not ctx.exists("namespace", ns):
        _ensure_namespace(ctx, ns)
    _release_pvc_mounts(ctx, ns, name)
    ctx.k(["delete", "pvc", name, "-n", ns, "--ignore-not-found", "--wait=false"])
    pvc = ctx.get("pvc", name, ns=ns)
    if pvc is not None:
        # merge null, then JSON remove if still present
        ctx.k(["patch", "pvc", name, "-n", ns, "--type=merge",
               "-p", '{"metadata":{"finalizers":null}}'])
        if ctx.get("pvc", name, ns=ns):
            ctx.k(["patch", "pvc", name, "-n", ns, "--type=json",
                   "-p", '[{"op":"remove","path":"/metadata/finalizers"}]'])
        # last resort: replace object with finalizers cleared (namespaced API; ns must exist)
        if ctx.get("pvc", name, ns=ns):
            obj = ctx.get("pvc", name, ns=ns)
            if obj:
                obj.setdefault("metadata", {})["finalizers"] = []
                ctx.run(ctx.kubectl + ["replace", "-f", "-"], input_=json.dumps(obj))
    return ctx.get("pvc", name, ns=ns) is None


def _drain_namespace(ctx: Ctx, ns: str, *, strip_finalizers: bool = True) -> List[str]:
    """Empty a namespace's contents (workloads, services, config, PVCs) without
    deleting the Namespace object. Used for: Terminating ns (must drain before
    finalize), Active husk ns (34d zarf-injector Service with no Ready registry),
    and pre-init cleanup of partial seed-registry installs.

    Order: controllers → pods (force) → services/cm/secret → PVC (with mount release).
    NEVER touches hostPath registry data. Layer-B only."""
    if not ctx.exists("namespace", ns):
        return []
    actions: List[str] = []
    for kind in ("deployments", "replicasets", "statefulsets", "daemonsets",
                 "jobs", "cronjobs"):
        ctx.k(["delete", kind, "--all", "-n", ns, "--wait=false", "--ignore-not-found"])
    ctx.k(["delete", "pods", "--all", "-n", ns,
           "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
    for kind in ("services", "endpoints", "configmaps", "secrets", "roles",
                 "rolebindings", "serviceaccounts"):
        # keep default SA; deleting all SAs is fine — k8s recreates default
        ctx.k(["delete", kind, "--all", "-n", ns, "--wait=false", "--ignore-not-found"])
    # PVCs last among namespaced objects
    for pvc in list(ctx.items("pvc", ns=ns)):
        name = pvc.get("metadata", {}).get("name")
        if not name:
            continue
        _release_pvc_mounts(ctx, ns, name)
        if _force_delete_pvc(ctx, ns, name):
            actions.append(f"drained PVC {ns}/{name}")
        else:
            actions.append(f"PVC {ns}/{name} still present after force-delete attempt")
    if strip_finalizers:
        for kind in _NS_CONTENT_KINDS + ("pods",):
            for it in ctx.items(kind, ns=ns):
                if (it.get("metadata", {}) or {}).get("finalizers"):
                    n = it["metadata"]["name"]
                    ctx.k(["patch", kind, n, "-n", ns, "--type=merge",
                           "-p", '{"metadata":{"finalizers":null}}'])
    leftovers = []
    for kind in ("deployments", "pods", "services", "pvc"):
        n = len(ctx.items(kind, ns=ns))
        if n:
            leftovers.append(f"{kind}={n}")
    if leftovers:
        actions.append(f"drain {ns} incomplete: {','.join(leftovers)}")
    else:
        actions.append(f"drained ns {ns} (empty)")
    return actions


def _ensure_registry_hostpath() -> List[str]:
    """Make registry hostPath exist and world-writable (platform helper)."""
    return _platform.ensure_registry_hostpath()

def _registry_ready_count(ctx: Ctx) -> tuple:
    ready, total = ctx.pods_ready(ZARF_NS, "app=docker-registry")
    if ready < 1 and total == 0:
        ready, total = ctx.pods_ready(
            ZARF_NS, "app.kubernetes.io/name=zarf-docker-registry")
    return ready, total


def _zarf_ns_husk_detail(ctx: Ctx) -> Optional[str]:
    """When the ``zarf`` ns should be drained before re-init.

    Returns a detail string if: Terminating; or Active with no Ready registry AND
    (bad/missing PVC or only husk leftovers). Returns None if healthy or if storage
    is already correct (Bound PVC on sc \"\") — pod not Ready yet is *not* a husk
    (would destroy an in-progress init).
    """
    ns = ctx.get("namespace", ZARF_NS)
    if not ns:
        return None
    if ns.get("status", {}).get("phase") == "Terminating":
        return "zarf ns Terminating (drain+finalize required)"
    ready, total = _registry_ready_count(ctx)
    if ready >= 1:
        return None
    pvc = ctx.get("pvc", REGISTRY_PVC_NAME, ns=ZARF_NS)
    if pvc is not None:
        phase = pvc.get("status", {}).get("phase")
        sc = (pvc.get("spec", {}) or {}).get("storageClassName")
        sc = "" if sc is None else sc
        del_ts = (pvc.get("metadata", {}) or {}).get("deletionTimestamp")
        if del_ts or phase == "Terminating":
            return (f"registry PVC Terminating (deleting={bool(del_ts)}) — "
                    "mount/finalizer wedge")
        if phase == "Bound" and sc == "":
            # Correct resilient bind; wait for registry pods — do NOT drain.
            return None
        if phase == "Pending" or sc != "":
            return (f"registry PVC phase={phase} sc={sc!r} (capture or unbound) — "
                    "delete before re-init")
    bits = []
    for kind in ("services", "deployments", "statefulsets", "secrets", "pvc", "pods"):
        items = ctx.items(kind, ns=ZARF_NS)
        if not items:
            continue
        created = (items[0].get("metadata", {}) or {}).get("creationTimestamp", "")
        bits.append(f"{kind}={len(items)}" + (f"@{created[:10]}" if created else ""))
    if bits:
        return (f"zarf husk/partial: registry ready {ready}/{total}; leftovers "
                + ", ".join(bits))
    return None

def _unwedge_failed_seed_registry(ctx: Ctx) -> List[str]:
    """After a failed ``zarf init`` Helm install of zarf-seed-registry (context
    deadline exceeded), clear pending helm secrets and partial chart objects so the
    next init is a clean install, not an upgrade of a broken release."""
    actions: List[str] = []
    actions += _unwedge_pending_helm(ctx)
    if not ctx.exists("namespace", ZARF_NS):
        return actions
    # seed chart objects without a Ready registry
    ready, _ = _registry_ready_count(ctx)
    if ready >= 1:
        return actions
    for kind in ("deploy", "sts", "job"):
        ctx.k(["delete", kind, "--all", "-n", ZARF_NS,
               "--wait=false", "--ignore-not-found"])
    ctx.k(["delete", "pods", "--all", "-n", ZARF_NS,
           "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
    actions.append("cleared partial seed-registry workloads in zarf ns")
    # Best-effort: zarf package remove for init seed component (syntax varies by version)
    if ctx.have_zarf():
        for args in (
            ["package", "remove", "init", "--confirm",
             "--components=zarf-seed-registry"],
            ["package", "remove", "zarf-seed-registry", "--confirm"],
        ):
            r = ctx.zarf(args, timeout=120)
            if r.returncode == 0:
                actions.append(f"zarf {' '.join(args[:3])} ok")
                break
    return actions


_HELM_PENDING = ("pending-install", "pending-upgrade", "pending-rollback")

# A ref the zarf agent MUST rewrite at admission. Never pulled — server dry-run only.
_CANARY_REF = "ghcr.io/zarf-canary/agent-check:v1"
_IMAGE_WAIT_BAD = ("ImagePullBackOff", "ErrImagePull", "ErrImageNeverPull")


def _poisoned_app_ns(ctx: Ctx) -> List[str]:
    """App namespaces carrying ``zarf.dev/agent=ignore`` — the SILENT killer of image
    rewriting. Zarf's agent mutates pod image refs at admission (the helm manifests
    keep upstream refs like ghcr.io by design), and its webhook EXCLUDES namespaces
    labeled ignore/skip. ``zarf init`` labels every PRE-EXISTING namespace ignore (so
    it won't disturb prior workloads) — correct on first init, but a RE-RUN of init
    over an existing deployment finds the app namespaces already present and poisons
    them all. Latent until any pod churn: the recreated pod keeps its upstream ref and
    ImagePullBackOffs forever in the closed world. Field-proven on the sandbox."""
    out = []
    for ns in APP_NAMESPACES:
        obj = ctx.get("namespace", ns)
        if not obj:
            continue
        labels = (obj.get("metadata", {}) or {}).get("labels", {}) or {}
        if labels.get("zarf.dev/agent") in ("ignore", "skip"):
            out.append(ns)
    return out


def _strip_agent_ignore(ctx: Ctx) -> List[str]:
    """Remove the ``zarf.dev/agent=ignore`` label from OUR app namespaces so the agent
    mutates their pods again. Only the package's own namespaces — never cluster/system
    namespaces, where the ignore label is correct and deliberate. Idempotent."""
    actions: List[str] = []
    for ns in _poisoned_app_ns(ctx):
        if ctx.k(["label", "namespace", ns, "zarf.dev/agent-", "--overwrite"]).returncode == 0:
            actions.append(f"stripped zarf.dev/agent=ignore from ns {ns} "
                           "(re-init had disabled image rewriting there)")
    return actions


def _webhook_mutating(ctx: Ctx) -> "bool | None":
    """Does the zarf agent ACTUALLY rewrite an upstream image ref in an APP namespace
    right now? A SERVER-side dry-run exercises the full admission chain (webhook
    selectors included), persists nothing and pulls nothing. Runs against the first
    EXISTING app namespace — zarf deliberately ignores pre-init namespaces like
    ``default``, so probing there would be a permanent false negative.
    True=mutating, False=bypassed, None=no verdict (no app ns yet / probe failed)."""
    ns = next((n for n in APP_NAMESPACES if ctx.exists("namespace", n)), None)
    if ns is None:
        return None
    r = ctx.k(["-n", ns, "run", "zarf-agent-canary",
               f"--image={_CANARY_REF}", "--restart=Never", "--dry-run=server",
               "-o", "jsonpath={.spec.containers[0].image}"])
    if r.returncode != 0 or not r.stdout.strip():
        return None
    return _CANARY_REF not in r.stdout


_DASK_ENV_KEYS = ("S3_ENDPOINT", "AWS_REGION", "AWS_ACCESS_KEY_ID",
                  "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")


def _unwedge_broken_dask_cluster(ctx: Ctx) -> List[str]:
    """The dask operator (kopf) creates the scheduler Deployment only on the CR's
    CREATION event — it neither propagates CR env changes to existing children nor
    recreates a deleted child (both field-proven). So two wedge states can only be
    fixed at the CR level: (a) scheduler/worker Deployments carrying S3/AWS env that
    DRIFTED from the CR (a redeploy fixed the CR's creds; pods keep the old — possibly
    empty — env forever, and the bucket-ensure action execs the scheduler using ITS
    env), and (b) the scheduler Deployment MISSING under a live CR (zarf re-applying
    the unchanged CR is a server-side no-op → no create event → it never returns).
    Remediation: delete the DaskCluster CR (clearing kopf's finalizer if it lingers) —
    the imminent component deploy re-applies it as a fresh CREATE and the operator
    builds scheduler+workers from it. Layer-B; no data lives in these pods."""
    cr = ctx.get("daskcluster", "cybersec-dask", ns="dask")
    if not cr:
        return []

    def env_map(spec: dict) -> dict:
        m = {}
        for c in (spec or {}).get("containers", []) or []:
            for e in c.get("env", []) or []:
                m[e.get("name")] = e.get("value", "") or ""
        return m

    want = {
        "scheduler": env_map(cr.get("spec", {}).get("scheduler", {}).get("spec", {})),
        "worker": env_map(cr.get("spec", {}).get("worker", {}).get("spec", {})),
    }
    scheds = ctx.items("deployments", ns="dask", selector="dask.org/component=scheduler")
    drifted = []
    for d in ctx.items("deployments", ns="dask"):
        role = (d.get("metadata", {}).get("labels", {}) or {}).get("dask.org/component", "")
        if role not in want:
            continue
        have = env_map(d.get("spec", {}).get("template", {}).get("spec", {}))
        keys = [k for k in _DASK_ENV_KEYS if k in want[role]]
        if any(have.get(k, "") != want[role].get(k, "") for k in keys):
            drifted.append(d["metadata"]["name"])
    if scheds and not drifted:
        return []
    reason = ("scheduler Deployment missing under a live CR" if not scheds
              else f"S3 env drifted from the CR on {drifted}")
    ctx.k(["delete", "daskcluster", "cybersec-dask", "-n", "dask",
           "--ignore-not-found", "--wait=false"])
    if ctx.get("daskcluster", "cybersec-dask", ns="dask"):   # kopf finalizer lingering
        ctx.k(["patch", "daskcluster", "cybersec-dask", "-n", "dask", "--type=merge",
               "-p", '{"metadata":{"finalizers":null}}'])
    return [f"deleted DaskCluster CR ({reason}) — the deploy re-creates it fresh "
            "(the operator only builds children on CR creation)"]


def _unwedge_unmutated_pods(ctx: Ctx) -> List[str]:
    """Pods admitted while the webhook was bypassed carry UPSTREAM image refs and
    ImagePullBackOff forever in the closed world — and a no-diff helm upgrade will NOT
    recreate them, so the deploy's --wait times out again and again. Delete them; their
    controllers re-create them through the (by now effective) webhook. Precise: only
    ImagePull-stuck pods whose ref is NOT the internal registry — an internal-ref pod
    stuck pulling is a T2 registry-content problem, not an admission one. Layer-B."""
    actions: List[str] = []
    for ns in APP_NAMESPACES:
        for p in ctx.items("pods", ns=ns):
            name = p.get("metadata", {}).get("name", "")
            stuck = any(
                ((cs.get("state", {}) or {}).get("waiting") or {}).get("reason") in _IMAGE_WAIT_BAD
                for cs in (p.get("status", {}).get("containerStatuses", []) or []))
            if not stuck:
                continue
            imgs = [c.get("image", "") for c in p.get("spec", {}).get("containers", [])]
            if any(i and not i.startswith("127.0.0.1:") for i in imgs):
                if ctx.k(["delete", "pod", name, "-n", ns, "--wait=false"]).returncode == 0:
                    actions.append(f"deleted unmutated ImagePull-stuck pod {ns}/{name} "
                                   "(upstream ref — re-admits via the webhook)")
    return actions


def _unwedge_pending_helm(ctx: Ctx) -> List[str]:
    """A converge/zarf killed mid-deploy leaves its Helm release pending-install/
    pending-upgrade — after which EVERY retry of that chart fails with 'another
    operation (install/upgrade/rollback) is in progress'. Deleting the LATEST
    (pending) release secret reverts Helm's view to the previous deployed revision so
    the next deploy proceeds. Only the newest revision per release matters (Helm reads
    release status from it); older secrets are history and are left alone. Layer-B —
    pure Helm bookkeeping, no workload/image is touched."""
    obj = ctx.kjson(["get", "secrets", "-A", "-l", "owner=helm"]) or {}
    latest: dict = {}   # (ns, release) -> (version, secret_name, status)
    for s in obj.get("items", []):
        md = s.get("metadata", {}) or {}
        lab = md.get("labels", {}) or {}
        try:
            ver = int(lab.get("version", 0))
        except (TypeError, ValueError):
            continue
        key = (md.get("namespace"), lab.get("name"))
        if key not in latest or ver > latest[key][0]:
            latest[key] = (ver, md.get("name"), lab.get("status"))
    actions: List[str] = []
    for (ns, rel), (ver, name, status) in latest.items():
        if status in _HELM_PENDING and ns and name:
            if ctx.k(["delete", "secret", name, "-n", ns]).returncode == 0:
                actions.append(f"unwedged pending Helm release {ns}/{rel} "
                               f"(deleted stuck rev {ver}: {status})")
    return actions


_NS_CONTENT_KINDS = ("deployments", "replicasets", "statefulsets", "daemonsets",
                     "services", "configmaps", "secrets", "pvc", "jobs",
                     "ingresses", "networkpolicies", "endpointslices")


def _unwedge_terminating_app_ns(ctx: Ctx) -> List[str]:
    """App-namespace wedges (also covered by discovery.sweep_vestiges each pass).

    TERMINATING — drain contents (incl. ingress/finalizers) then finalize when empty.
    ACTIVE HUSKS — controllers/services with zero pods, or all-junk pods: recycle ns.
    Layer-B only; package redeploy recreates everything.
    """
    from .discovery import _drain_ns, _is_app_husk, _force_finalize_ns as _ff

    actions: List[str] = []
    for ns in APP_NAMESPACES:
        obj = ctx.get("namespace", ns)
        if not obj:
            continue
        phase = obj.get("status", {}).get("phase")
        if phase == "Terminating":
            actions.extend(_drain_ns(ctx, ns))
            leftovers = any(ctx.items(k, ns=ns) for k in ("deployments", "pods", "pvc"))
            if not leftovers:
                if _ff(ctx, ns) or _force_finalize_ns(ctx, ns):
                    actions.append(f"force-finalized Terminating ns {ns} (contents cleared first)")
            else:
                actions.append(f"clearing contents of Terminating ns {ns} (finalize next pass)")
        elif phase == "Active":
            reason = _is_app_husk(ctx, ns)
            if reason:
                actions.extend(_drain_ns(ctx, ns))
                ctx.k(["delete", "namespace", ns, "--wait=false"])
                actions.append(f"deleted husk ns {ns} ({reason})")
    return actions


# S3 secrets → ZARF_CONFIG tmpfs [package.deploy.set]. Non-secrets → --set-variables.
# Bare ZARF_VAR_* env does NOT template in zarf v0.70.1 (field-proven empty renders).
_S3_SECRET_KEYS = {"S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_SESSION_TOKEN"}
# Components whose manifests template a non-empty S3_BUCKET into a configMap. Deploying
# them with a blank bucket silently bricks the app at runtime, so we refuse instead.
_S3_DEPENDENT_COMPONENTS = ("panel-viz", "navigator-engine")


def _zarf_deploy_retries(components: str) -> int:
    """Zarf ``--retries`` — permanent failures (OOM, ImagePull) must not burn 10×.

    converge-24/25: navigator-engine OOM drove ``--retries 10`` + 7200s wall with
    no progress visibility. Prefer few retries; engine rem/abort handles terminal.
    """
    c = {x.strip() for x in components.lower().split(",") if x.strip()}
    heavy = {"jupyterhub", "sample-notebooks", "dask-operator", "dask-cluster"}
    if c & heavy:
        return 3
    if "cybersec-images" in c:
        return 3
    # Light chart-only redeploys (engine/panel already imaged)
    return 2


def _zarf_deploy_timeout(components: str) -> int:
    """Seconds for ``zarf package deploy --components=…`` wall-clock kill.

    Scaled down from blanket 7200s (converge-24). Heavy sets still get room for
    air-gap image push + helm; light remediations fail fast and re-detect.
    """
    c = components.lower()
    if any(x in c for x in ("jupyterhub", "sample-notebooks")):
        return 3600  # hub chart is the slow path
    if any(x in c for x in ("dask-operator", "dask-cluster")):
        return 2400
    if "cybersec-images" in c:
        # Push can be long on first install; registry already populated → abort
        # check / early success short-circuits well under this.
        return 1800
    if any(x in c for x in ("panel-viz", "navigator-engine")):
        return 900
    return 1200


def _deploy_abort_signal(ctx: Ctx, components: str, *, started: float) -> Optional[str]:
    """Return a short reason to kill an in-flight zarf deploy, or None.

    Only arms after a grace period so we do not abort before pods are created.
    """
    import time as _time
    if _time.monotonic() - started < 45.0:
        return None
    c = components.lower()
    watch: List[tuple] = []  # (ns, selector, label)
    if any(x in c for x in ("panel-viz", "cybersec-images")):
        watch.append((_PANEL_NS, "app=otel-navigator", "otel-navigator"))
    if any(x in c for x in ("navigator-engine", "cybersec-images")):
        watch.append((_PANEL_NS, "app=navigator-engine", "navigator-engine"))
    if any(x in c for x in ("dask-cluster", "dask-operator", "cybersec-images")):
        watch.append(("dask", "dask.org/component=scheduler", "dask-scheduler"))
    for ns, sel, label in watch:
        for p in ctx.items("pods", ns=ns, selector=sel):
            for cs in ((p.get("status") or {}).get("containerStatuses") or []):
                waiting = ((cs.get("state") or {}).get("waiting") or {})
                reason = waiting.get("reason") or ""
                if reason in _IMAGE_WAIT_BAD:
                    return f"{label} {reason} mid-deploy — fix images, not more zarf retries"
                if reason == "CrashLoopBackOff":
                    last = ((cs.get("lastState") or {}).get("terminated") or {})
                    if last.get("reason") == "OOMKilled":
                        return (f"{label} CrashLoop/OOM mid-deploy — raise memory "
                                f"limits (not zarf retries)")
    return None


def _pre_deploy_feasibility(ctx: Ctx, components: str) -> List[str]:
    """Knowable preconditions for package deploys that embed hard waits.

    Converge-09 class: committing to ``dask-cluster`` while the scheduler is
    already Pending/not Ready under pressure converts a long deploy into a
    guaranteed after-action timeout and mints a failed helm revision.
    """
    issues: List[str] = []
    c = components.lower()
    node = _node_schedulability_census(ctx)
    if not node["schedulable"]:
        issues.append(f"node not schedulable: {node['summary']}")
    issues.extend(_disk_pressure_issues(ctx))
    # Components with in-package Ready waits (zarf.yaml after actions)
    # Registry v2 is SoR for pullability (alongside kubectl / helm secrets).
    reg = _registry_census(ctx)
    if any(x in c for x in (
        "cybersec-images", "dask-cluster", "dask-operator",
        "jupyterhub", "panel-viz", "navigator-engine",
    )):
        if reg.get("registry_ready", 0) < 1 and reg.get("registry_total", 0) > 0:
            issues.append("zarf registry pods not Ready")
        if reg.get("catalog_has_app") is False:
            issues.append("registry catalog missing cybersec-dask — push images first")
        if reg.get("partial_push"):
            issues.append(
                f"registry PARTIAL_PUSH for cybersec-dask:{reg.get('target_tag')} "
                f"(catalog has repo, manifest HEAD 404) — re-push cybersec-images "
                f"before deploy that would ImagePullBackOff"
            )
        elif reg.get("manifest_ok") is False and reg.get("target_tag"):
            issues.append(
                f"registry missing pullable manifest cybersec-dask:{reg.get('target_tag')} "
                f"— push cybersec-images first"
            )
    if any(x in c for x in ("dask-cluster", "dask-operator")):
        sched = _pod_terminal_census(ctx, "dask", "dask.org/component=scheduler")
        if "dask-cluster" in c and sched["pending"] and not node["schedulable"]:
            issues.append(
                "scheduler already Pending and node not schedulable — "
                "resolve schedule before dask-cluster deploy (embedded wait)"
            )
        if "dask-cluster" in c and sched["image_pull"]:
            issues.append(
                f"scheduler ImagePull before deploy: {sched['image_pull'][:2]} — "
                "push cybersec-images first"
            )
    return issues


def _zarf_deploy_components(ctx: Ctx, components: str) -> Fix:
    missing = []
    if not ctx.have_zarf():
        missing.append(f"zarf binary missing (zarf_bin={ctx.zarf_bin!r})")
    if not ctx.package_path:
        missing.append("package path not passed to engine (--package / converge-node.sh arg)")
    elif not Path(ctx.package_path).is_file():
        missing.append(f"package file not found: {ctx.package_path}")
    if missing:
        reason = "; ".join(missing)
        return Fix(False, _manual.zarf_deploy_manual_line(components, reason=reason))
    # Refuse long zarf deploys under disk pressure unless FSM can resolve it:
    # df >= hard → clear taint + wait for condition; df < hard → MANUAL free disk.
    dp = _disk_pressure_issues(ctx)
    if dp:
        resolved = _platform.rem_resolve_disk_pressure(ctx)
        if not resolved.changed and _disk_pressure_issues(ctx):
            return Fix(False, _manual.join_detail(
                f"refusing zarf deploy --components={components} — "
                f"{resolved.detail}",
                _manual.hint_for("T0.no-disk-pressure"),
            ))
        # Pressure cleared — fall through to deploy
        print(f"    disk pressure resolved: {resolved.detail}", flush=True)
    # Feasibility from *live* API/registry/node state only (no out-of-band journal).
    # Converge-09 class: refuse deploys whose embedded waits are already doomed.
    feas = _pre_deploy_feasibility(ctx, components)
    if feas:
        return Fix(False, _manual.join_detail(
            f"refusing zarf deploy --components={components} — pre-deploy feasibility "
            f"failed: {'; '.join(feas)}",
            _manual.zarf_deploy_recipe(components),
        ))
    # Fail LOUD rather than render an empty S3_BUCKET. An S3-dependent component
    # deployed with a blank bucket renders OTEL_DATA_PATH=s3:/// and bricks the app
    # ("Invalid bucket name 's3:'"), so refuse instead of silently breaking it.
    if any(c in components for c in _S3_DEPENDENT_COMPONENTS) and not ctx.s3.get("S3_BUCKET"):
        recipe = _manual.zarf_deploy_recipe(
            components, needs_s3=True,
            note="S3_BUCKET required — blank bucket renders OTEL_DATA_PATH=s3:/// "
                 "(runtime 'Invalid bucket name s3:'). Export S3_* or use --creds-file.",
        )
        return Fix(False, _manual.join_detail(
            f"MANUAL: refusing to deploy {components} — S3_BUCKET not provided to converge",
            recipe))
    # Unwind deploy-blocking wedges BEFORE zarf. Stamp detected INGRESS_CLASS so
    # redeploys never re-introduce traefik-on-RKE2 silent 404s.
    _platform.ensure_ingress_class_in_ctx(ctx)
    # Worker sizing for package templates (canonical names). Aliases folded first;
    # T4.workers-capacity still capacity-caps and surgically patches a live CR.
    _normalize_worker_aliases(ctx)
    if not _s3_lookup(ctx, "DASK_WORKER_REPLICAS"):
        # Multi-core lab / air-gap baseline; capacity cap (T4) still trims Pending
        ctx.s3["DASK_WORKER_REPLICAS"] = str(_WORKER_DEFAULT_REPLICAS)
    if not _s3_lookup(ctx, "DASK_WORKER_NTHREADS"):
        ctx.s3["DASK_WORKER_NTHREADS"] = _WORKER_DEFAULT_NTHREADS
    if not _s3_lookup(ctx, "DASK_WORKER_CPU"):
        ctx.s3["DASK_WORKER_CPU"] = _WORKER_DEFAULT_CPU
    if not _s3_lookup(ctx, "DASK_WORKER_MEMORY"):
        ctx.s3["DASK_WORKER_MEMORY"] = _WORKER_DEFAULT_MEMORY
    unwound = (_unwedge_pending_helm(ctx) + _unwedge_terminating_app_ns(ctx)
               + _strip_agent_ignore(ctx) + _unwedge_unmutated_pods(ctx)
               + _unwedge_broken_dask_cluster(ctx))
    pre = f"  [unwound: {'; '.join(unwound)}]" if unwound else ""
    retries = _zarf_deploy_retries(components)
    args = ["package", "deploy", ctx.package_path, "--confirm",
            f"--components={components}", "--retries", str(retries)]
    if not ctx.registry_pvc_enabled:
        args.append("--set-variables=REGISTRY_PVC_ENABLED=false")
    # Non-sensitive vars → --set-variables; secrets → ZARF_CONFIG tmpfs.
    env = {}
    secrets = {}
    for k, v in ctx.s3.items():
        if not v:
            continue
        if k.upper() in _S3_SECRET_KEYS:
            secrets[k.upper()] = v
        else:
            args.append(f"--set-variables={k.upper()}={v}")
    cfg_path = None
    if secrets:
        import tempfile
        d = "/dev/shm" if Path("/dev/shm").is_dir() else None
        fd, cfg_path = tempfile.mkstemp(prefix=".zarf-cfg-", suffix=".toml", dir=d)
        with open(fd, "w") as f:   # mkstemp: 0600
            f.write("[package.deploy.set]\n")
            for k, v in secrets.items():
                esc = v.replace("\\", "\\\\").replace('"', '\\"')
                f.write(f'{k} = "{esc}"\n')
        env["ZARF_CONFIG"] = cfg_path
    deploy_timeout = _zarf_deploy_timeout(components)
    print(
        f"    $ zarf {' '.join(args)}  (timeout={deploy_timeout}s retries={retries} stream+abort)",
        flush=True,
    )
    import time as _time
    t0 = _time.monotonic()

    def _abort() -> Optional[str]:
        return _deploy_abort_signal(ctx, components, started=t0)

    try:
        # Stream zarf so progress is visible; abort on terminal cluster signals
        # (ImagePull/OOM loop) instead of burning the full wall-clock (converge-24).
        r = ctx.zarf_stream(
            args, env=env, timeout=deploy_timeout,
            abort_check=_abort, abort_every=12.0,
        )

        # DEAD HELM RELEASE — field-proven (2026-07-15): a chart whose FIRST install
        # failed leaves a release with only `failed` revisions; every later
        # `helm upgrade` refuses with "has no deployed releases" — forever. Because
        # required components ride every deploy, ONE dead release (dask-cluster-cr in
        # the field) blocks EVERY component deploy. The pending-* unwedge cannot see
        # this state (status `failed`, not pending-*). Remedy is zarf's own
        # recommendation: `zarf package remove` the failing COMPONENT (named in the
        # error), then a fresh deploy INSTALLS instead of upgrading. One retry.
        # Normalize streams: TimeoutExpired / some zarf builds can leave bytes.
        err = Ctx.out_text(r.stderr) + Ctx.out_text(r.stdout)
        if r.returncode != 0 and "no deployed releases" in err and ctx.package_path:
            # Parse component name whether zarf quotes with " or '.
            m = re.search(
                r'unable to deploy component ["\']([^"\']+)["\']', err
            )
            dead_comp = m.group(1) if m else components.split(",")[0]
            # Always also try removing the requested component list (field:
            # jupyterhub/sample-notebooks blocked by a dead sibling rider).
            remove_set = []
            for c in [dead_comp] + components.split(","):
                c = c.strip()
                if c and c not in remove_set:
                    remove_set.append(c)
            notes = []
            for dead in remove_set:
                rm = ctx.zarf(
                    ["package", "remove", ctx.package_path, "--confirm",
                     f"--components={dead}"],
                    timeout=600,
                )
                notes.append(f"{dead}:rc={rm.returncode}")
            note = (
                f"removed dead-release component(s) [{', '.join(notes)}] "
                f"(helm 'has no deployed releases')"
            )
            r2 = ctx.zarf_stream(
                args, env=env, timeout=deploy_timeout,
                abort_check=_abort, abort_every=12.0,
            )
            if r2.returncode == 0:
                return Fix(True, f"zarf deploy {components}: rc=0 "
                                 f"(retry after dead-release removal)  [{note}]{pre}")
            pre += f"  [{note}]"
            r = r2
    finally:
        if cfg_path:
            try:
                Path(cfg_path).write_bytes(b"\0" * 256)   # best-effort scrub (tmpfs)
                Path(cfg_path).unlink()
            except OSError:
                pass
    if r.returncode == 0:
        return Fix(True, f"zarf deploy {components}: rc=0{pre}")
    # Surface the actual zarf/Helm error tail, not a bare rc=1 — the deploy failures
    # (dask-operator/jupyterhub) only showed "rc=1" all afternoon. Last lines tend to
    # carry the cause (chart timeout, image pull, CRD hook); S3 secrets ride env, not
    # stdout, so this stays clean. Always attach in-situ DISCOVER/FIX so operators can
    # re-run the same ``zarf package deploy --components=…`` that unblocks installs.
    # Failure leaves *cluster* evidence (helm failed-with-history, Pending pods,
    # Deployment available=0) — next discover() reads that API state, not a journal.
    tail = (Ctx.out_text(r.stderr) or Ctx.out_text(r.stdout)).strip().splitlines()[-3:]
    suffix = f" — {' / '.join(s.strip() for s in tail)}" if tail else ""
    head = f"zarf deploy {components}: rc={r.returncode}{suffix}{pre}"
    return Fix(False, _manual.join_detail(
        head,
        _manual.zarf_deploy_recipe(
            components,
            pkg=ctx.package_path or "$PKG",
            note=f"Engine deploy failed — re-run in situ with --components={components}",
        )))


def _ensure_default_sc(ctx: Ctx, sc: str = "local-path") -> bool:
    r = ctx.k(["patch", "sc", sc, "-p",
               '{"metadata":{"annotations":'
               '{"storageclass.kubernetes.io/is-default-class":"true"}}}'])
    return r.returncode == 0


def _apply_bundled_local_path(ctx: Ctx) -> Fix:
    """Apply the BUNDLED local-path-provisioner manifest with kubectl — registry-
    INDEPENDENT (the image is node-preloaded, pulled with IfNotPresent), so it
    bootstraps the default StorageClass with NO zarf registry and NO egress. This is
    what breaks the SC↔registry chicken-egg on a fresh cluster. Degrades to a precise
    MANUAL hint if the manifest wasn't staged next to the engine (--manifests-dir)."""
    path = Path(ctx.manifests_dir) / "local-path-provisioner.yaml" if ctx.manifests_dir else None
    if not path or not path.exists():
        return Fix(False, _manual.join_detail(
            "MANUAL: kubectl apply local-path-provisioner.yaml "
            "(bundled manifest not staged — pass --manifests-dir)",
            _manual.hint_for("T0.5.provisioner") or _manual.block(
                ["kc get sc; kc -n local-path-storage get pods 2>/dev/null"],
                ["kc apply -f manifests/local-path-provisioner.yaml",
                 "kc patch sc local-path -p "
                 '\'{"metadata":{"annotations":'
                 '{"storageclass.kubernetes.io/is-default-class":"true"}}}\''],
            )))
    r = ctx.apply_yaml(path.read_text())
    return Fix(r.returncode == 0,
               f"applied bundled local-path-provisioner.yaml (node image): rc={r.returncode}")


# --------------------------------------------------------------------------- #
# T0 — node / closure
# --------------------------------------------------------------------------- #

def _det_layer_a_zarf_tools(ctx: Ctx) -> Probe:
    """Layer-A CLOSURE: zarf binary executable + zarf-init package discoverable.
    Field: ``zarf init rc=127`` and 'requires a zarf-init package' both surface here
    before T1 burns an EXPENSIVE remediation cycle."""
    if not ctx.have_zarf():
        return Probe(False, "zarf binary not available on PATH / --zarf")
    r = ctx.zarf(["version"], timeout=30)
    if r.returncode == 127:
        return Probe(False, "zarf binary not executable (rc=127)")
    if r.returncode != 0:
        return Probe(False, f"zarf version failed rc={r.returncode}")
    # init package: beside deploy package, /var/tmp, or cwd
    search: List[Path] = []
    if ctx.package_path:
        search.append(Path(ctx.package_path).resolve().parent)
    search += [Path("/var/tmp"), Path.cwd()]
    found = None
    for d in search:
        try:
            matches = sorted(d.glob("zarf-init-*.tar.zst"))
        except OSError:
            continue
        if matches:
            found = matches[0]
            break
    if not found:
        return Probe(False,
                     "zarf-init-*.tar.zst not found beside package or in /var/tmp "
                     "(Layer A — transport the release init package)")
    return Probe(True, f"zarf ok; init package {found.name}")


def _det_api(ctx: Ctx) -> Probe:
    r = ctx.k(["get", "--raw", "/healthz"])
    return Probe(r.returncode == 0 and "ok" in r.stdout.lower(),
                 r.stdout.strip() or r.stderr.strip())


def _det_node_ready(ctx: Ctx) -> Probe:
    nodes = ctx.items("nodes")
    if not nodes:
        return Probe(False, "no nodes returned")
    bad = []
    for n in nodes:
        name = n["metadata"]["name"]
        conds = {c["type"]: c["status"] for c in n.get("status", {}).get("conditions", [])}
        if conds.get("Ready") != "True":
            bad.append(f"{name}=NotReady")
        elif n.get("spec", {}).get("unschedulable"):
            bad.append(f"{name}=cordoned")
    return Probe(not bad, ", ".join(bad) or f"{len(nodes)} node(s) Ready+schedulable")


def _rem_node_ready(ctx: Ctx) -> Fix:
    changed = False
    for n in ctx.items("nodes"):
        if n.get("spec", {}).get("unschedulable"):
            ctx.k(["uncordon", n["metadata"]["name"]])
            changed = True
    return Fix(changed, "uncordoned cordoned node(s)" if changed else "nothing to uncordon")


def _disk_pressure_issues(ctx: Ctx) -> List[str]:
    """Node DiskPressure *condition* and/or disk-pressure *taint*.

    Field: operators may ``kubectl taint … disk-pressure-`` while the condition is
    still True — kubelet re-taints and pods stay unschedulable. Long zarf deploys
    under DiskPressure=True fill imagefs further and wedge hub/proxy Pending.
    """
    issues: List[str] = []
    for n in ctx.items("nodes"):
        name = (n.get("metadata") or {}).get("name") or "?"
        conds = {c.get("type"): c for c in (n.get("status") or {}).get("conditions") or []}
        dp = conds.get("DiskPressure") or {}
        if str(dp.get("status", "")).lower() == "true":
            msg = (dp.get("message") or dp.get("reason") or "DiskPressure=True").strip()
            issues.append(f"{name}: condition DiskPressure=True ({msg})")
        for t in n.get("spec", {}).get("taints", []) or []:
            if t.get("key") == "node.kubernetes.io/disk-pressure":
                effect = t.get("effect") or "NoSchedule"
                issues.append(f"{name}: taint node.kubernetes.io/disk-pressure:{effect}")
    return issues


def _det_no_disk_pressure(ctx: Ctx) -> Probe:
    """DiskPressure vs df floors — NoSchedule taint is the scheduling gate.

    When free >= soft and only the condition bit is sticky (no disk-pressure
    taint), treat as OK so rem does not block multi-minute waits (converge-19).
    """
    issues = _disk_pressure_issues(ctx)
    free = _platform.disk_free_census()
    min_free = free.get("min_free_gib")
    soft = free.get("soft_gib")
    hard = free.get("hard_gib")
    if free.get("below_hard"):
        return Probe(
            False,
            f"df below hard eviction floor (min_free={min_free}Gi < hard={hard}Gi); "
            f"{free['summary']}",
        )
    # Any disk-pressure NoSchedule taint → not OK (pods cannot schedule)
    taint_only = [i for i in issues if "disk-pressure taint" in i or "NoSchedule" in i]
    cond_only = [i for i in issues if "DiskPressure=True" in i or "condition DiskPressure" in i]
    if taint_only:
        detail = "; ".join(taint_only + ([free["summary"]] if free.get("summary") else []))
        if min_free is not None and soft is not None and float(min_free) >= float(soft):
            detail += f" — df >= soft ({soft}Gi); rem will clear taint without long wait"
        elif issues and not free.get("below_hard"):
            detail += f" — df above hard; rem will clear taint / short-wait"
        return Probe(False, detail)
    # Condition True but no taint: if df >= soft, scheduling works — OK
    if cond_only and min_free is not None and soft is not None \
            and float(min_free) >= float(soft):
        return Probe(
            True,
            f"DiskPressure condition lag with df >= soft ({free['summary']}) — "
            f"no NoSchedule taint; not blocking",
        )
    if not issues and not free.get("below_hard"):
        return Probe(True, f"no DiskPressure; {free['summary']}")
    detail_parts = list(issues) if issues else []
    if issues:
        detail_parts.append(free["summary"])
    return Probe(False, "; ".join(detail_parts) if detail_parts else free["summary"])


def _rem_no_disk_pressure(ctx: Ctx) -> Fix:
    """See platform.rem_resolve_disk_pressure — MANUAL only when df < hard GiB."""
    return _platform.rem_resolve_disk_pressure(ctx)


def _node_schedulability_census(ctx: Ctx) -> dict:
    """Single-node (or multi) knowable schedulability: Ready, pressure, taints, cordon.

    Air-gap single-node field path: DiskPressure/MemoryPressure and imagefs pressure
    explain almost all Pending after a successful package apply (wait actions timeout
    while pods cannot schedule). No external APIs — pure kubectl inventory.
    """
    blockers: List[str] = []
    notes: List[str] = []
    schedulable = True
    for n in ctx.items("nodes"):
        name = (n.get("metadata") or {}).get("name") or "?"
        conds = {c.get("type"): c for c in (n.get("status") or {}).get("conditions") or []}
        ready = str((conds.get("Ready") or {}).get("status", "")).lower() == "true"
        if not ready:
            blockers.append(f"{name}: NotReady")
            schedulable = False
        for ptype in ("DiskPressure", "MemoryPressure", "PIDPressure"):
            c = conds.get(ptype) or {}
            if str(c.get("status", "")).lower() == "true":
                msg = (c.get("message") or c.get("reason") or ptype).strip()
                blockers.append(f"{name}: {ptype}=True ({msg})")
                schedulable = False
        if n.get("spec", {}).get("unschedulable"):
            blockers.append(f"{name}: cordoned")
            schedulable = False
        for t in n.get("spec", {}).get("taints", []) or []:
            key = t.get("key") or ""
            effect = t.get("effect") or ""
            if effect not in ("NoSchedule", "NoExecute"):
                continue
            if "control-plane" in key or "master" in key:
                notes.append(f"{name}: system taint {key}:{effect} (tolerated by system pods)")
                continue
            blockers.append(f"{name}: taint {key}:{effect}")
            if any(x in key for x in (
                "disk-pressure", "memory-pressure", "pid-pressure",
                "unreachable", "not-ready", "unschedulable",
            )):
                schedulable = False
        # Allocatable vs capacity (informational)
        alloc = (n.get("status") or {}).get("allocatable") or {}
        if alloc.get("memory"):
            notes.append(f"{name}: allocatable mem={alloc.get('memory')} cpu={alloc.get('cpu')}")
    return {
        "schedulable": schedulable,
        "blockers": blockers,
        "notes": notes,
        "disk_pressure": any("DiskPressure" in b for b in blockers),
        "summary": (
            "node_schedulable=True" if schedulable
            else "node_schedulable=False [" + "; ".join(blockers[:5]) + "]"
        ),
    }


def _pod_failed_scheduling_msgs(pod: dict) -> List[str]:
    """Extract FailedScheduling / wait reasons from pod status (knowable locally)."""
    msgs: List[str] = []
    st = pod.get("status") or {}
    for c in st.get("conditions") or []:
        if c.get("type") == "PodScheduled" and c.get("status") == "False":
            reason = c.get("reason") or "Unschedulable"
            msg = (c.get("message") or "").strip()
            msgs.append(f"{reason}: {msg}" if msg else reason)
    for cs in (st.get("containerStatuses") or []) + (st.get("initContainerStatuses") or []):
        waiting = ((cs.get("state") or {}).get("waiting") or {})
        if waiting.get("reason"):
            wmsg = (waiting.get("message") or "").strip()
            msgs.append(
                f"{waiting.get('reason')}"
                + (f": {wmsg[:160]}" if wmsg else "")
            )
        term = ((cs.get("state") or {}).get("terminated") or {})
        if term.get("reason") in ("OOMKilled", "Error"):
            msgs.append(f"terminated:{term.get('reason')}")
    return msgs


def _pod_terminal_census(ctx: Ctx, ns: str, selector: str) -> dict:
    """Census one workload: phases, FailedScheduling, ImagePull, CrashLoop, images."""
    pods = ctx.items("pods", ns=ns, selector=selector)
    ready, total = ctx.pods_ready(ns, selector)
    pending: List[dict] = []
    image_pull: List[str] = []
    crash: List[str] = []
    images: List[str] = []
    _PULL = ("ImagePullBackOff", "ErrImagePull", "ErrImageNeverPull")
    _CRASH = ("CrashLoopBackOff", "CreateContainerConfigError",
              "RunContainerError", "OOMKilled")
    for p in pods:
        name = (p.get("metadata") or {}).get("name") or "?"
        phase = (p.get("status") or {}).get("phase") or "?"
        node = (p.get("spec") or {}).get("nodeName") or ""
        msgs = _pod_failed_scheduling_msgs(p)
        for m in msgs:
            low = m.lower()
            if any(x.lower() in low for x in _PULL):
                image_pull.append(f"{name}:{m[:100]}")
            if any(x.lower() in low for x in _CRASH) or "oomkilled" in low:
                crash.append(f"{name}:{m[:100]}")
        for c in (p.get("spec") or {}).get("containers") or []:
            img = c.get("image") or ""
            if img and img not in images:
                images.append(img)
        if phase == "Pending" or not node:
            pending.append({
                "name": name, "phase": phase, "node": node or None, "msgs": msgs,
            })
    return {
        "ready": ready,
        "total": total,
        "pending": pending,
        "image_pull": image_pull,
        "crash": crash,
        "images": images,
        "summary": (
            f"{ns}/{selector}: ready={ready}/{total} pending={len(pending)} "
            f"image_pull={len(image_pull)} crash={len(crash)}"
        ),
    }


def _registry_v2_bases(ctx: Ctx) -> List[str]:
    """In-cluster registry HTTP bases (distribution v2) — part of the SoR set.

    Prefer ClusterIP/NodePort of zarf-docker-registry; fall back to common NodePort.
    Fully in-cluster; no external registry.
    """
    bases: List[str] = []
    svc = (
        ctx.get("svc", "zarf-docker-registry", ns=ZARF_NS)
        or ctx.get("service", "zarf-docker-registry", ns=ZARF_NS)
    )
    if svc:
        spec = svc.get("spec") or {}
        ports = spec.get("ports") or []
        port = 5000
        for p in ports:
            if p.get("name") in ("http", "registry", None) or p.get("port"):
                port = int(p.get("port") or 5000)
                np = p.get("nodePort")
                if np:
                    bases.append(f"http://127.0.0.1:{int(np)}")
                break
        cip = spec.get("clusterIP")
        if cip and cip not in ("None", "none", ""):
            bases.append(f"http://{cip}:{port}")
    # Common zarf internal registry NodePort on single-node RKE2
    for np in (31999, 30001):
        b = f"http://127.0.0.1:{np}"
        if b not in bases:
            bases.append(b)
    return bases


def _registry_manifest_head(ctx: Ctx, repo: str, tag: str) -> dict:
    """HEAD /v2/<repo>/manifests/<tag> against the zarf registry.

    Distinguishes:
      * 200 — manifest present (pullable)
      * 404 — tag/manifest missing (partial push if catalog lists repo)
      * 401/403 — auth; treat as unknown (not a false partial)
      * other/unreachable — unknown

    This is the only in-cluster place "blobs present, manifest absent" is knowable.
    """
    import urllib.error
    import urllib.request

    if not tag or not repo:
        return {"ok": None, "status": None, "detail": "no target tag/repo"}
    path = f"/v2/{repo}/manifests/{tag}"
    accept = (
        "application/vnd.docker.distribution.manifest.v2+json,"
        "application/vnd.oci.image.manifest.v1+json,"
        "application/vnd.docker.distribution.manifest.list.v2+json"
    )
    last_err = ""
    for base in _registry_v2_bases(ctx):
        url = base.rstrip("/") + path
        try:
            req = urllib.request.Request(
                url, method="HEAD",
                headers={"Accept": accept},
            )
            with urllib.request.urlopen(req, timeout=8) as resp:
                code = getattr(resp, "status", None) or resp.getcode()
                if code == 200:
                    return {
                        "ok": True,
                        "status": 200,
                        "detail": f"manifest HEAD 200 {repo}:{tag} via {base}",
                        "base": base,
                    }
                last_err = f"{base} → HTTP {code}"
        except urllib.error.HTTPError as e:
            if e.code == 200:
                return {
                    "ok": True, "status": 200,
                    "detail": f"manifest HEAD 200 {repo}:{tag} via {base}",
                    "base": base,
                }
            if e.code == 404:
                return {
                    "ok": False,
                    "status": 404,
                    "detail": (
                        f"manifest HEAD 404 {repo}:{tag} via {base} — "
                        f"tag not pullable (partial push or never pushed)"
                    ),
                    "base": base,
                }
            if e.code in (401, 403):
                return {
                    "ok": None,
                    "status": e.code,
                    "detail": f"manifest HEAD {e.code} (auth) via {base} — unknown",
                    "base": base,
                }
            last_err = f"{base} → HTTP {e.code}"
        except Exception as e:  # noqa: BLE001 — census best-effort
            last_err = f"{base} → {type(e).__name__}: {e}"
            continue
    return {
        "ok": None,
        "status": None,
        "detail": f"manifest HEAD unreachable ({last_err})",
    }


def _registry_census(ctx: Ctx) -> dict:
    """Knowable Layer-A registry state (in-cluster SoR alongside kubectl/helm secrets).

    - registry Deployment pods Ready
    - ``zarf tools registry catalog`` lists cybersec-dask (repo presence)
    - distribution v2 HEAD of the *package target tag* (manifest pullable)

    Catalog-has-repo + HEAD 404 ⇒ partial push — the state that survives a
    feasibility probe based only on catalog and then ImagePullBackOffs on deploy.
    """
    reg_ready, reg_total = ctx.pods_ready(ZARF_NS, "app=docker-registry")
    if reg_total == 0:
        reg_ready, reg_total = ctx.pods_ready(ZARF_NS, "app.kubernetes.io/name=docker-registry")
    catalog_ok = None  # None = unknown, True/False known
    catalog_detail = ""
    if ctx.have_zarf():
        r = ctx.zarf(["tools", "registry", "catalog"], timeout=60)
        out = Ctx.out_text(r.stdout) + Ctx.out_text(r.stderr)
        if r.returncode == 0:
            catalog_ok = "cybersec-dask" in out
            catalog_detail = (
                "catalog has cybersec-dask" if catalog_ok
                else "catalog reachable; cybersec-dask absent"
            )
        else:
            catalog_detail = f"catalog query rc={r.returncode}"

    tag = _target_cybersec_tag(ctx)
    # _target_cybersec_tag is defined later in this module — available at runtime
    manifest = _registry_manifest_head(ctx, "cybersec-dask", tag) if tag else {
        "ok": None, "status": None, "detail": "no package target tag in artifacts.manifest",
    }
    partial = (
        catalog_ok is True
        and manifest.get("ok") is False
        and manifest.get("status") == 404
    )
    bits = [f"registry pods {reg_ready}/{reg_total}"]
    if catalog_detail:
        bits.append(catalog_detail)
    if tag:
        bits.append(manifest.get("detail") or f"manifest {tag}=?")
    if partial:
        bits.append("PARTIAL_PUSH (repo in catalog, target manifest 404)")

    # healthy: pods up, not known-absent catalog, not partial push, manifest not 404
    healthy = (
        reg_ready >= 1
        and catalog_ok is not False
        and not partial
        and manifest.get("ok") is not False
    )
    return {
        "registry_ready": reg_ready,
        "registry_total": reg_total,
        "catalog_has_app": catalog_ok,
        "manifest_ok": manifest.get("ok"),
        "manifest_status": manifest.get("status"),
        "target_tag": tag,
        "partial_push": partial,
        "detail": "; ".join(bits),
        "healthy": healthy,
    }


def _cluster_orient_summary(ctx: Ctx, *pod_censuses: dict) -> str:
    """One-line orientation for detect/rem logs (single-node air-gap)."""
    node = _node_schedulability_census(ctx)
    reg = _registry_census(ctx)
    parts = [node["summary"], reg["detail"]]
    for pc in pod_censuses:
        if pc:
            parts.append(pc.get("summary") or "")
    return " | ".join(p for p in parts if p)


def _det_layer_a_images(ctx: Ctx) -> Probe:
    """CLOSURE: the bootstrap images must be present in the closed world. Proxy via
    pod health — an ImagePullBackOff means the image isn't there. (Never pulls.)"""
    missing = ctx.pod_image_missing("local-path-storage", "app=local-path-provisioner")
    if missing is True:
        return Probe(False, "local-path-provisioner ImagePullBackOff — bootstrap image absent")
    # If the provisioner isn't deployed yet we can't judge from pods; treat as OK at
    # T0 and let the T0.5 provisioner invariant surface a real pull failure.
    return Probe(True, "no image-pull failure observed for bootstrap images")


# --------------------------------------------------------------------------- #
# T0.5 — storage
# --------------------------------------------------------------------------- #

def _det_sc_default(ctx: Ctx) -> Probe:
    scs = ctx.items("storageclass")
    default = [s["metadata"]["name"] for s in scs
               if (s["metadata"].get("annotations", {}) or {})
               .get("storageclass.kubernetes.io/is-default-class") == "true"]
    return Probe(bool(default), f"default SC: {default}" if default
                 else f"no default StorageClass (have: {[s['metadata']['name'] for s in scs]})")


def _rem_sc_default(ctx: Ctx) -> Fix:
    if ctx.exists("storageclass", "local-path"):
        ok = _ensure_default_sc(ctx)
        return Fix(ok, "marked local-path default" if ok else "patch failed")
    # No StorageClass yet: apply the BUNDLED manifest via kubectl (registry-
    # independent — node-preloaded image), then mark it default. Registry-free, so
    # it can run BEFORE zarf init (T1) — this is the cycle-break.
    fix = _apply_bundled_local_path(ctx)
    if not fix.changed:
        return fix  # MANUAL / failed — propagate the hint
    _ensure_default_sc(ctx)
    return Fix(True, f"{fix.detail}; marked local-path default")


def _det_provisioner(ctx: Ctx) -> Probe:
    if ctx.pod_image_missing("local-path-storage", "app=local-path-provisioner") is True:
        return Probe(False, "provisioner ImagePullBackOff — bootstrap image missing (CLOSURE)")
    ready, total = ctx.pods_ready("local-path-storage", "app=local-path-provisioner")
    return Probe(ready >= 1, f"provisioner ready {ready}/{total}")


def _rem_provisioner(ctx: Ctx) -> Fix:
    # Apply the bundled manifest via kubectl (node-preloaded image), NOT a zarf
    # component deploy — the provisioner must come up before the registry exists,
    # and re-applying the same manifest the SC bootstrap used is idempotent.
    return _apply_bundled_local_path(ctx)


def _det_registry_pv(ctx: Ctx) -> Probe:
    """RESILIENT default: the Zarf registry binds storage WITHOUT a default
    StorageClass — a claimRef-prebound hostPath PV satisfies its PVC directly, so the
    whole SC↔registry chicken-egg (and the provisioner + its bootstrap images) simply
    don't exist. OK if the registry PVC is already Bound (e.g. a resourced cluster's
    pre-existing default SC handled it) OR the prebound PV is present so a fresh
    ``zarf init``'s PVC binds on creation."""
    # Reclaim policy FIRST — a Bound-but-Delete static PV converges "healthy"
    # while primed to erase the registry data on the next PVC churn (matrix
    # case 15). Retain is the conservation contract; reclaim is mutable on a
    # Bound PV, so this is always normalizable in place.
    pv = ctx.get("pv", REGISTRY_PV_NAME)
    if pv is not None:
        pol = (pv.get("spec") or {}).get("persistentVolumeReclaimPolicy")
        if pol != "Retain":
            return Probe(False,
                         f"registry PV reclaim={pol!r} (want Retain — conservation)")
    if any(p.get("status", {}).get("phase") == "Bound"
           for p in ctx.items("pvc", ns="zarf")):
        return Probe(True, "zarf registry PVC already Bound")
    if pv is not None:
        return Probe(True, "claimRef registry PV present (PVC will bind on init)")
    return Probe(False, "no Bound registry PVC and no prebound registry PV")


def _rem_registry_pv(ctx: Ctx) -> Fix:
    # Reclaim drift on an EXISTING PV: merge-patch just the policy — never
    # re-apply the full object over a Bound PV (claimRef/uid conflicts).
    pv = ctx.get("pv", REGISTRY_PV_NAME)
    if pv is not None and \
            (pv.get("spec") or {}).get("persistentVolumeReclaimPolicy") != "Retain":
        r = ctx.k(["patch", "pv", REGISTRY_PV_NAME, "--type", "merge",
                   "-p", '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'])
        return Fix(r.returncode == 0,
                   "normalized registry PV reclaim → Retain (conservation)"
                   if r.returncode == 0 else
                   f"failed to patch registry PV reclaim: rc={r.returncode}")
    # Apply the claimRef-prebound hostPath PV so the registry PVC binds with NO default
    # StorageClass. Idempotent; Layer-B (a disposable PV, not a transported artifact).
    # This is the single move that lets the resilient path skip the provisioner entirely.
    r = ctx.apply_yaml(_registry_pv_yaml(ctx))
    return Fix(r.returncode == 0,
               f"applied claimRef registry PV ({ctx.registry_pv_size}): rc={r.returncode}")


# --------------------------------------------------------------------------- #
# T1 — zarf init / registry
# --------------------------------------------------------------------------- #

def _det_registry_running(ctx: Ctx) -> Probe:
    if not ctx.exists("namespace", "zarf"):
        return Probe(False, "zarf namespace absent (not initialized)")
    ready, total = ctx.pods_ready("zarf", "app=docker-registry")
    if ready < 1 and total == 0:
        ready, total = ctx.pods_ready("zarf", "app.kubernetes.io/name=zarf-docker-registry")
    if ready < 1:
        return Probe(False, f"zarf-docker-registry ready {ready}/{total}")
    # A Running registry is NOT a completed init. The agent-hook mutating webhook
    # rewrites image refs to the internal registry at admission — with it dead or
    # absent (e.g. a partial init, or a force-finalized ns that took the agent with
    # it), every later deploy's pods ImagePullBackOff against ghcr.io/quay.io and the
    # loop stalls WITHOUT naming the cause. Same remediation either way: re-run
    # `zarf init` (idempotent — redeploys agent + webhook). Matched by pod-name
    # prefix, not a label guess.
    agents = [p for p in ctx.items("pods", ns="zarf")
              if p.get("metadata", {}).get("name", "").startswith("agent-hook")]
    a_ready = sum(
        1 for p in agents
        if {c["type"]: c["status"]
            for c in p.get("status", {}).get("conditions", [])}.get("Ready") == "True")
    if a_ready < 1:
        return Probe(False,
                     f"registry Ready but zarf agent-hook {a_ready}/{len(agents)} — init "
                     "incomplete (image refs won't be rewritten); re-init required")
    # Ready agents are NOT enough — the agent must be BEHAVIORALLY effective for OUR
    # namespaces. The field killer: a RE-RUN `zarf init` labels the (now pre-existing)
    # app namespaces zarf.dev/agent=ignore, silently disabling image rewriting; any
    # later pod churn then ImagePullBackOffs on upstream refs in the closed world.
    poisoned = _poisoned_app_ns(ctx)
    if poisoned:
        return Probe(False,
                     f"app namespaces labeled zarf.dev/agent=ignore: {poisoned} — a re-run "
                     "`zarf init` disabled image rewriting there (pods created later keep "
                     "upstream refs → ImagePullBackOff air-gapped); label strip required")
    # End-to-end proof: a canary server dry-run in an app namespace must come back
    # rewritten (exercises the webhook + selectors; persists nothing, pulls nothing).
    mut = _webhook_mutating(ctx)
    if mut is False:
        return Probe(False,
                     "registry+agent Ready, app namespaces unlabeled, but the agent did NOT "
                     "rewrite a canary in an app namespace — admission not reaching the agent; "
                     "re-init required")
    return Probe(True, f"registry ready {ready}/{total}, agent-hook {a_ready}/{len(agents)}"
                       + (", agent rewriting (canary)" if mut else ""))


def _default_storage_classes(ctx: Ctx) -> List[str]:
    """Names of StorageClasses currently marked cluster-default."""
    obj = ctx.kjson(["get", "storageclass"]) or {}
    out = []
    for sc in obj.get("items", []):
        ann = sc.get("metadata", {}).get("annotations") or {}
        if ann.get("storageclass.kubernetes.io/is-default-class") == "true":
            name = sc.get("metadata", {}).get("name")
            if name:
                out.append(name)
    return out


def _undefault_sc(ctx: Ctx, name: str) -> bool:
    """Strip the default-class annotation from a StorageClass — the inverse of
    _ensure_default_sc — so a PVC that omits a class falls through to "" instead of
    waiting on this SC's provisioner."""
    r = ctx.k(["patch", "storageclass", name, "-p",
               '{"metadata":{"annotations":'
               '{"storageclass.kubernetes.io/is-default-class":"false"}}}'])
    return r.returncode == 0


def _pre_init_cleanup(ctx: Ctx) -> List[str]:
    """Exhaustively unwind every registry/storage state that makes ``zarf init`` fail
    or hang (rc=1, rc=124 context deadline, rc=127 missing binary), so a plain init can
    succeed. Idempotent + CONSERVATION-safe: never deletes hostPath registry DATA.

    Generalized from field sessions (anticipatory catalog — every special case is one
    row of this procedure):

      • hostPath /var/lib/zarf-registry not writable     → mkdir+chmod 0777
      • zarf ns Terminating                              → drain contents, then finalize
      • zarf ns absent + orphaned PVC/husk (split-brain) → create ns, drain husks
      • zarf ns Active husk (injector Service, no registry) → full drain
      • partial seed-registry Helm (deadline exceeded)   → pending-helm + partial workloads
      • cluster-default StorageClass                     → un-default (hygiene)
      • registry PVC Terminating / Pending / wrong class → release mounts + force-delete
      • static PV Released/Failed/class-drift            → reset object (data retained)
      • app ns agent=ignore poison                       → strip labels
    Pairs with `zarf init --storage-class -` in `_rem_registry_running`.
    """
    actions: List[str] = []

    # 0. HostPath first — seed-registry Helm waits on a registry that cannot write.
    for a in _ensure_registry_hostpath():
        if not a.startswith("hostPath ") or "ready" not in a:
            actions.append(a)
        elif "MANUAL" in a or "not writable" in a or "failed" in a:
            actions.append(a)

    # A. Namespace topology: Terminating | absent (split-brain) | Active husk | healthy
    ns = ctx.get("namespace", ZARF_NS)
    if ns and ns.get("status", {}).get("phase") == "Terminating":
        actions += _drain_namespace(ctx, ZARF_NS)
        leftovers = any(ctx.items(k, ns=ZARF_NS)
                        for k in ("deployments", "pods", "pvc", "services"))
        if not leftovers:
            if _force_finalize_ns(ctx, ZARF_NS):
                actions.append("force-finalized Terminating zarf ns (contents cleared first)")
        else:
            actions.append("zarf ns Terminating — contents still draining (next pass)")
    elif ns is None:
        # Split-brain path: PVC/Service may reappear when ns is recreated.
        if _ensure_namespace(ctx, ZARF_NS):
            actions.append("recreated absent zarf ns (split-brain re-home for husk objects)")
        husk = _zarf_ns_husk_detail(ctx)
        if husk or ctx.items("pvc", ns=ZARF_NS) or ctx.items("services", ns=ZARF_NS):
            actions += _drain_namespace(ctx, ZARF_NS)
            if husk:
                actions.append(f"drained after recreate: {husk}")
    else:
        husk = _zarf_ns_husk_detail(ctx)
        if husk:
            actions += _drain_namespace(ctx, ZARF_NS)
            actions.append(f"drained zarf husk/partial ({husk})")

    # A2. Failed seed chart / pending helm — before another init races the same release.
    actions += _unwedge_failed_seed_registry(ctx)

    # A3. Agent poison on app namespaces (re-init labels pre-existing ns ignore).
    actions += _strip_agent_ignore(ctx)

    # Storage unwind is RESILIENT-path only (static claimRef PV).
    if not (ctx.registry_pvc_enabled and not ctx.dynamic_provisioning):
        return actions

    # B. Un-default cluster-default StorageClasses (hygiene; may be zero SCs — fine).
    for sc in _default_storage_classes(ctx):
        if _undefault_sc(ctx, sc):
            actions.append(f"un-defaulted StorageClass {sc!r}")

    # C. Registry PVC: Terminating, Pending, wrong class, or deletingTimestamp —
    #    never salvage in place (class immutable). Healthy Bound + sc "" → keep.
    if not ctx.exists("namespace", ZARF_NS):
        _ensure_namespace(ctx, ZARF_NS)
    pvc = ctx.get("pvc", REGISTRY_PVC_NAME, ns=ZARF_NS)
    if pvc is not None:
        phase = pvc.get("status", {}).get("phase")
        cur = (pvc.get("spec", {}) or {}).get("storageClassName")
        cur = "" if cur is None else cur
        del_ts = (pvc.get("metadata", {}) or {}).get("deletionTimestamp")
        finals = (pvc.get("metadata", {}) or {}).get("finalizers") or []
        bad = bool(del_ts) or phase in ("Terminating", "Pending", "Lost") \
            or phase != "Bound" or cur != ""
        if bad:
            # CONSERVATION backstop for LEGACY layouts (pre-v1.6.1 init): a Bound
            # PVC on a dynamically provisioned PV (reclaimPolicy Delete) would
            # take the registry DATA with it on delete. Patch the bound PV to
            # Retain first — a mistaken delete then leaves the blobs on disk.
            vol = (pvc.get("spec", {}) or {}).get("volumeName")
            if vol and vol != REGISTRY_PV_NAME:
                r = ctx.k(["patch", "pv", vol, "-p",
                           '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'])
                if r.returncode == 0:
                    actions.append(f"reclaim=Retain backstop on bound PV {vol} "
                                   "(legacy-layout conservation)")
            if _force_delete_pvc(ctx, ZARF_NS, REGISTRY_PVC_NAME):
                actions.append(
                    f"deleted registry PVC (was phase={phase} sc={cur!r} "
                    f"deleting={bool(del_ts)} finals={finals})")
            else:
                actions.append(
                    f"registry PVC STILL present after force-delete "
                    f"(phase={phase} sc={cur!r}) — will block init")

    # D. Static claimRef hostPath PV on storageClass "" (never adopt local-path).
    pv = ctx.get("pv", REGISTRY_PV_NAME)
    if pv is None:
        ctx.apply_yaml(_registry_pv_yaml(ctx, ""))
        actions.append('created static registry PV (storageClass="")')
    elif pv.get("status", {}).get("phase") != "Bound":
        phase = pv.get("status", {}).get("phase")
        cur_sc = (pv.get("spec", {}) or {}).get("storageClassName") or ""
        claim = (pv.get("spec", {}) or {}).get("claimRef") or {}
        if phase in ("Released", "Failed") or claim.get("uid") or cur_sc != "":
            ctx.k(["delete", "pv", REGISTRY_PV_NAME, "--ignore-not-found"])
            ctx.apply_yaml(_registry_pv_yaml(ctx, ""))
            actions.append(f'reset static registry PV → storageClass="" '
                           f"(was phase={phase} storageClass={cur_sc!r})")
    return actions


def _diagnose_registry(ctx: Ctx) -> str:
    """Inspect live registry/storage so a failed ``zarf init`` names the unmet
    condition (PVC Terminating, SC capture, hostPath, husk, rc=127, …)."""
    bits: List[str] = []
    if not ctx.have_zarf():
        bits.append("zarf binary unavailable (rc=127 class — install Layer-A binary)")
    ns = ctx.get("namespace", ZARF_NS)
    if not ns:
        bits.append("zarf ns absent (init created nothing, rolled back, or split-brain)")
    else:
        bits.append(f"zarf ns phase={ns.get('status', {}).get('phase', '?')}")
    husk = _zarf_ns_husk_detail(ctx)
    if husk:
        bits.append(husk)
    pvc = ctx.get("pvc", REGISTRY_PVC_NAME, ns=ZARF_NS) if ns else None
    if pvc:
        sc = (pvc.get("spec", {}) or {}).get("storageClassName")
        sc = "" if sc is None else sc
        phase = pvc.get("status", {}).get("phase", "?")
        del_ts = (pvc.get("metadata", {}) or {}).get("deletionTimestamp")
        finals = (pvc.get("metadata", {}) or {}).get("finalizers") or []
        vol = (pvc.get("spec", {}) or {}).get("volumeName") or ""
        bits.append(f"registry PVC phase={phase} sc={sc!r} vol={vol!r} "
                    f"deleting={bool(del_ts)} finals={finals}")
        if del_ts or phase == "Terminating":
            bits.append("PVC Terminating — release mounts + strip finalizers "
                        "(ns must exist for patch)")
        if sc != "":
            bits.append(f"registry PVC on {sc!r} not '' — default-SC capture (immutable); "
                        "delete PVC; init with --storage-class -")
        if phase == "Pending":
            pv = ctx.get("pv", REGISTRY_PV_NAME)
            if pv is None:
                bits.append("no static registry PV present to bind it")
            else:
                pvsc = (pv.get("spec", {}) or {}).get("storageClassName") or ""
                pvp = pv.get("status", {}).get("phase", "?")
                bits.append(f"static PV {pvp} sc={pvsc!r}"
                            + ("" if pvsc == sc else f" (≠ PVC sc {sc!r} → won't bind)"))
    elif ns:
        bits.append("no registry PVC")
    pv = ctx.get("pv", REGISTRY_PV_NAME)
    if pv:
        bits.append(
            f"static PV phase={pv.get('status', {}).get('phase')} "
            f"sc={(pv.get('spec') or {}).get('storageClassName')!r}")
    defs = _default_storage_classes(ctx)
    if defs:
        bits.append(f"default StorageClass {defs} present — resilient path wants none")
    if not defs and not ctx.items("storageclass"):
        bits.append("no StorageClasses (OK for resilient path)")
    # hostPath
    hp = Path(REGISTRY_HOSTPATH)
    if not hp.is_dir():
        bits.append(f"hostPath {REGISTRY_HOSTPATH} missing")
    else:
        try:
            mode = stat.S_IMODE(hp.stat().st_mode)
            if mode != 0o777:
                bits.append(f"hostPath mode={oct(mode)} (want 0777; registry is non-root)")
        except OSError as e:
            bits.append(f"hostPath stat failed: {e}")
    if ctx.pod_image_missing(ZARF_NS, "app=docker-registry"):
        bits.append("registry pod cannot pull its image (absent in the closed world)")
    # pod events (FailedMount / deadline clues)
    for p in ctx.items("pods", ns=ZARF_NS)[:3]:
        phase = p.get("status", {}).get("phase")
        name = p.get("metadata", {}).get("name", "?")
        if phase and phase != "Running":
            bits.append(f"pod {name} phase={phase}")
    return "; ".join(bits) or (
        "registry not Ready — inspect `kubectl -n zarf get pvc,pv,pods` + events")


def _rem_registry_running(ctx: Ctx) -> Fix:
    """Unwind platform wedges, then ``zarf init --storage-class -``. On failure,
    diagnose + second-pass cleanup for seed-registry deadline (partial Helm)."""
    actions = _pre_init_cleanup(ctx)
    if not ctx.have_zarf():
        head = (
            "MANUAL: zarf binary missing (rc=127 class) — install Layer-A "
            f"v0.70.x to /usr/local/bin/zarf  [unwound: {'; '.join(actions)}]"
            if actions else
            "MANUAL: zarf binary missing — install Layer-A to /usr/local/bin/zarf"
        )
        return Fix(False, _manual.join_detail(head, _manual.hint_for("T0.layer-a-zarf-tools")))

    # Prefer running init from the package directory so zarf-init-*.tar.zst is found.
    pkg_dir = None
    if ctx.package_path:
        pkg_dir = str(Path(ctx.package_path).resolve().parent)

    args = ["init", "--confirm", f"--set=REGISTRY_PVC_SIZE={ctx.registry_pv_size}"]
    if ctx.registry_pvc_enabled:
        # "-" sentinel → storageClassName:"" EXPLICITLY (binds static PV; no SC race).
        args += ["--storage-class=-"]
    else:
        args.append("--set=REGISTRY_PVC_ENABLED=false")

    def _run_init() -> "object":
        # cwd via env is insufficient; use run with explicit chdir in subprocess
        if pkg_dir and Path(pkg_dir).is_dir():
            import subprocess as _sp
            argv = [str(ctx.zarf_bin)] + args
            try:
                return _sp.run(
                    argv, capture_output=True, text=True, timeout=1800,
                    cwd=pkg_dir, env={**os.environ})
            except FileNotFoundError as e:
                return _sp.CompletedProcess(argv, 127, "", str(e))
            except _sp.TimeoutExpired as e:
                return _sp.CompletedProcess(
                    argv, 124, Ctx.out_text(e.stdout),
                    Ctx.out_text(e.stderr) or "timeout")
        return ctx.zarf(args)

    r = _run_init()
    actions += _strip_agent_ignore(ctx)
    tail = f"  [unwound: {'; '.join(actions)}]" if actions else ""

    if r.returncode == 0:
        return Fix(True, f"zarf init: rc=0{tail}")

    # Second pass: seed-registry deadline / partial install often leaves recoverable state
    if r.returncode in (1, 124) or "deadline" in Ctx.out_text(r.stderr).lower():
        more = _unwedge_failed_seed_registry(ctx)
        more += _pre_init_cleanup(ctx)
        if more:
            actions += more
            r2 = _run_init()
            actions += _strip_agent_ignore(ctx)
            tail = f"  [unwound: {'; '.join(actions)}]"
            if r2.returncode == 0:
                return Fix(True, f"zarf init (retry after seed unwedge): rc=0{tail}")
            r = r2

    if r.returncode == 127:
        head = (f"zarf init rc=127 (command not found / not executable): "
                f"{_diagnose_registry(ctx)}{tail}")
        return Fix(False, _manual.join_detail(
            head, _manual.hint_for("T0.layer-a-zarf-tools")))
    head = f"zarf init rc={r.returncode}: {_diagnose_registry(ctx)}{tail}"
    return Fix(False, _manual.join_detail(head, _manual.hint_for("T1.registry-running")))

# --------------------------------------------------------------------------- #
# T2 — images pushed to the internal registry (expensive)
# --------------------------------------------------------------------------- #

def _det_images_pushed(ctx: Ctx) -> Probe:
    """App image pullable from in-cluster registry (catalog + v2 manifest HEAD)."""
    for ns, sel in (("dask-operator", "app.kubernetes.io/name=dask-kubernetes-operator"),
                    ("dask", "dask.org/component=scheduler")):
        miss = ctx.pod_image_missing(ns, sel)
        if miss is True:
            return Probe(False, f"{ns} pods in ImagePullBackOff — images not in registry")
    reg = _registry_census(ctx)
    if reg.get("partial_push"):
        return Probe(
            False,
            f"PARTIAL_PUSH — {reg['detail']}",
        )
    if reg.get("manifest_ok") is True:
        return Probe(True, reg["detail"])
    if reg.get("catalog_has_app") is False:
        return Probe(False, f"registry catalog missing cybersec-dask — {reg['detail']}")
    if reg.get("manifest_ok") is False:
        return Probe(False, reg["detail"])
    # Catalog/HEAD unknown: defer unless pods prove pull failure (above)
    return Probe(True, reg["detail"] or "no image-pull failures observed")


def _rem_images_pushed(ctx: Ctx) -> Fix:
    return _zarf_deploy_components(ctx, "cybersec-images")


# --------------------------------------------------------------------------- #
# T3-T6 — components
# --------------------------------------------------------------------------- #

def _det_operator(ctx: Ctx) -> Probe:
    crd = ctx.exists("crd", "daskclusters.kubernetes.dask.org")
    ready, total = ctx.pods_ready("dask-operator", "app.kubernetes.io/name=dask-kubernetes-operator")
    return Probe(crd and ready >= 1, f"operator {ready}/{total}, CRD={'yes' if crd else 'no'}")


def _rem_operator(ctx: Ctx) -> Fix:
    return _zarf_deploy_components(ctx, "dask-operator")


def _det_scheduler(ctx: Ctx) -> Probe:
    """Scheduler Ready on *target* image — census node/registry/pod when not.

    Ready-on-stale-image is a FAIL (dask operator does not roll children when the
    CR image is rewritten by helm — converge-09 class upgrade silence).
    """
    pods = _pod_terminal_census(ctx, "dask", "dask.org/component=scheduler")
    drift = _image_drift(ctx, "dask", "dask.org/component=scheduler")
    if pods["ready"] >= 1:
        if drift:
            return Probe(
                False,
                f"scheduler Ready but {drift} — CR/helm advanced without rolling "
                f"children; recycle scheduler pods or redeploy dask-cluster",
            )
        return Probe(True, f"scheduler ready {pods['ready']}/{pods['total']}")
    orient = _cluster_orient_summary(ctx, pods)
    if drift:
        orient = f"{orient} | {drift}"
    has_cr = bool(ctx.items("daskcluster", ns="dask")) or bool(
        ctx.items("daskclusters", ns="dask"))
    if pods["image_pull"]:
        return Probe(
            False,
            f"scheduler ImagePull — registry/image rewrite. census: {orient} "
            f"pull={pods['image_pull'][:3]} cr={'yes' if has_cr else 'no'}",
        )
    if pods["crash"]:
        log_bits = _crash_log_census(ctx, "dask", "dask.org/component=scheduler")
        log_s = ("\n" + "\n".join(log_bits)) if log_bits else ""
        return Probe(
            False,
            f"scheduler CrashLoop/OOM. census: {orient} crash={pods['crash'][:3]}"
            f"{log_s}",
        )
    node = _node_schedulability_census(ctx)
    if pods["pending"] and not node["schedulable"]:
        return Probe(
            False,
            f"scheduler Pending — node not schedulable (DiskPressure/taint/cordon). "
            f"do NOT redeploy. census: {orient}",
        )
    if pods["pending"] and node["schedulable"]:
        return Probe(
            False,
            f"scheduler Pending but node schedulable — recycle pod. census: {orient}",
        )
    if pods["total"] == 0:
        return Probe(
            False,
            f"scheduler absent (0 pods) cr={'yes' if has_cr else 'no'}. census: {orient}",
        )
    return Probe(False, f"scheduler not Ready. census: {orient}")


def _rem_scheduler(ctx: Ctx) -> Fix:
    """Census-driven scheduler rem (single-node air-gap).

    Order:
      1. Node not schedulable → MANUAL (DiskPressure) — no zarf
      2. ImagePull → push cybersec-images (registry)
      3. CrashLoop → recycle scheduler pod once; if still bad, redeploy dask-cluster
      4. Pending + schedulable → recycle scheduler pod (operator recreates)
      5. No pods / no CR → zarf package deploy dask-cluster
      6. CR present, operator OK, still no Ready → redeploy dask-cluster
    """
    actions: List[str] = []
    pods = _pod_terminal_census(ctx, "dask", "dask.org/component=scheduler")
    node = _node_schedulability_census(ctx)
    reg = _registry_census(ctx)
    orient = _cluster_orient_summary(ctx, pods)

    # 1) Node pressure / taint — FSM resolve when df >= hard; MANUAL only if df short
    if not node["schedulable"] and (pods["pending"] or pods["total"] == 0 or pods["ready"] < 1):
        if node.get("disk_pressure") or _disk_pressure_issues(ctx):
            cleared = _platform.rem_resolve_disk_pressure(ctx)
            actions.append(cleared.detail)
            if cleared.changed:
                node = _node_schedulability_census(ctx)
            elif not node["schedulable"] and _disk_pressure_issues(ctx):
                return Fix(False, cleared.detail)
        if not _node_schedulability_census(ctx)["schedulable"]:
            return Fix(bool(actions), _manual.join_detail(
                f"scheduler blocked by node pressure/taint — {orient}. "
                f"actions={actions}",
                _manual.hint_for("T0.no-disk-pressure"),
            ))

    # 2) Image pull / catalog → ensure images in registry
    if pods["image_pull"] or reg.get("catalog_has_app") is False:
        fix = _zarf_deploy_components(ctx, "cybersec-images")
        actions.append(fix.detail)
        # recycle so kubelet re-pulls after push
        ctx.k(["delete", "pod", "-n", "dask", "-l", "dask.org/component=scheduler",
               "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
        actions.append("recycled scheduler pods after image push")
        again = _pod_terminal_census(ctx, "dask", "dask.org/component=scheduler")
        if again["ready"] >= 1 and not _image_drift(
                ctx, "dask", "dask.org/component=scheduler"):
            return Fix(True, f"images+recycle → scheduler Ready — "
                             f"{_cluster_orient_summary(ctx, again)}")
        # continue toward cluster redeploy if still missing

    # 2b) Image drift — NEVER recycle-only (operator recreates from CR with the
    # OLD tag → Ready-but-wrong forever; converge-10 class loop).
    # Order: push target layers → retarget CR+Deployments → recycle pods →
    # optional dask-cluster deploy → refuse success while drift remains.
    drift = _image_drift(ctx, "dask", "dask.org/component=scheduler")
    if drift and node["schedulable"]:
        target = _target_cybersec_tag(ctx)
        actions.append(f"image drift: {drift}")
        # Layers must exist in the in-cluster registry before retarget
        img_fix = _zarf_deploy_components(ctx, "cybersec-images")
        actions.append(img_fix.detail)
        actions.extend(_retarget_dask_workload_images(ctx, target))
        ctx.k(["delete", "pod", "-n", "dask", "-l", "dask.org/component=scheduler",
               "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
        ctx.k(["delete", "pod", "-n", "dask", "-l", "dask.org/component=worker",
               "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
        actions.append("recycled dask pods after image retarget")
        # If still drifted, full dask-cluster package path (templates new image)
        if _image_drift(ctx, "dask", "dask.org/component=scheduler"):
            fix = _zarf_deploy_components(ctx, "cybersec-images,dask-cluster")
            actions.append(fix.detail)
            actions.extend(_retarget_dask_workload_images(ctx, target))
            ctx.k(["delete", "pod", "-n", "dask",
                   "-l", "dask.org/component=scheduler",
                   "--force", "--grace-period=0", "--wait=false",
                   "--ignore-not-found"])
            ctx.k(["delete", "pod", "-n", "dask",
                   "-l", "dask.org/component=worker",
                   "--force", "--grace-period=0", "--wait=false",
                   "--ignore-not-found"])
            actions.append("post-deploy recycle for image pickup")
        again = _pod_terminal_census(ctx, "dask", "dask.org/component=scheduler")
        still = _image_drift(ctx, "dask", "dask.org/component=scheduler")
        if again["ready"] >= 1 and not still:
            return Fix(True, f"image-drift rem → scheduler Ready on {target} — "
                             f"{_cluster_orient_summary(ctx, again)}  "
                             f"[{'; '.join(actions)}]")
        # Dual-package / package≠engine-manifest skew: MANUAL not infinite loop
        hint = (
            "Image drift remains after push+retarget+recycle. Common cause: "
            "engine artifacts.manifest target tag is not in the staged "
            "zarf-package (two 1.6.6 tarballs with different content tags). "
            "Keep ONE package that matches the engine, re-transport if needed, "
            "then: zarf package deploy $PKG --confirm "
            "--components=cybersec-images,dask-cluster"
        )
        return Fix(bool(actions), _manual.join_detail(
            f"image drift NOT cleared — still: {still or drift}; "
            f"running={_running_cybersec_ref(ctx, 'dask', 'dask.org/component=scheduler')}; "
            f"actions=[{'; '.join(actions)}]",
            hint,
        ))

    # 3/4) Recycle on CrashLoop or Pending-while-schedulable
    if pods["crash"] or (pods["pending"] and node["schedulable"]) or (
            pods["total"] > 0 and pods["ready"] < 1 and node["schedulable"]
            and not pods["image_pull"]):
        r = ctx.k([
            "delete", "pod", "-n", "dask", "-l", "dask.org/component=scheduler",
            "--force", "--grace-period=0", "--wait=false", "--ignore-not-found",
        ])
        if r.returncode == 0:
            actions.append("recycled scheduler pod(s) for reschedule/restart")
        # Give operator a moment is next pass; also try worker cap if OOM/insufficient
        hints = " ".join(
            m for p in pods["pending"] for m in (p.get("msgs") or [])
        ).lower()
        if any(x in hints for x in ("insufficient", "memory", "cpu")) or pods["crash"]:
            cap = _rem_workers_capacity(ctx)
            if cap.changed:
                actions.append(cap.detail)
        again = _pod_terminal_census(ctx, "dask", "dask.org/component=scheduler")
        # Do not declare success while image drift remains (Ready ≠ correct tag)
        if again["ready"] >= 1 and not _image_drift(
                ctx, "dask", "dask.org/component=scheduler"):
            return Fix(True, f"recycle → scheduler Ready — "
                             f"{_cluster_orient_summary(ctx, again)}  "
                             f"[unwound: {'; '.join(actions)}]")
        if again["pending"] and not _node_schedulability_census(ctx)["schedulable"]:
            return Fix(bool(actions), _manual.join_detail(
                f"recycled; still blocked by node — {_cluster_orient_summary(ctx, again)}",
                _manual.hint_for("T0.no-disk-pressure"),
            ))
        # fall through to deploy if CR/pods still broken

    # 5/6) Package path — create/reconcile DaskCluster
    fix = _zarf_deploy_components(ctx, "dask-cluster")
    detail = fix.detail
    if actions:
        detail = f"{detail}  [prior: {'; '.join(actions)}]"
    # After deploy, if wait action timed out but CR exists, recycle once more
    final = _pod_terminal_census(ctx, "dask", "dask.org/component=scheduler")
    if final["ready"] < 1 and final["total"] >= 1 and node["schedulable"]:
        ctx.k(["delete", "pod", "-n", "dask", "-l", "dask.org/component=scheduler",
               "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
        detail += "  [post-deploy recycle scheduler for readiness]"
        # Progress, not success — next pass re-detects Ready/drift
        return Fix(True, detail)
    if final["ready"] >= 1 and not _image_drift(
            ctx, "dask", "dask.org/component=scheduler"):
        return Fix(True, f"dask-cluster rem → scheduler Ready — "
                         f"{_cluster_orient_summary(ctx, final)}")
    if final["ready"] >= 1 and _image_drift(
            ctx, "dask", "dask.org/component=scheduler"):
        # Deploy "succeeded" but wrong content tag still running — do not loop
        # as success; force image-drift path next pass via failed detect
        return Fix(True, detail + "  [deployed but image drift remains — "
                   "next pass will retarget CR/Deployments]")
    return Fix(fix.changed or bool(actions), detail)


# --------------------------------------------------------------------------- #
# T4.workers-capacity — surgical worker REPLICAS + SIZING (engine ≥ 0.5.0)
# --------------------------------------------------------------------------- #
# Package wire (zarf.yaml / dask-cluster.yaml):
#   DASK_WORKER_REPLICAS  → spec.worker.replicas
#   DASK_WORKER_NTHREADS  → container args --nthreads
#   DASK_WORKER_CPU       → resources.limits.cpu  (≥ nthreads)
#   DASK_WORKER_MEMORY    → resources.limits.memory + args --memory-limit
# Aliases (v1.6.5 docs; not package vars — engine folds them):
#   DASK_WORKER_MEM_LIMIT   → DASK_WORKER_MEMORY
#   DASK_WORKER_MEM_REQUEST → resources.requests.memory (optional)
#
# The dask-kubernetes operator often only propagates *replicas* on a live CR;
# template/sizing changes need a worker pod (or Deployment) bounce. We never
# re-push images for scale/size — patch CR + recycle workers only.


def _s3_lookup(ctx: Ctx, *keys: str) -> str:
    """First non-empty ctx.s3 value among keys (exact then upper/lower)."""
    for k in keys:
        for cand in (k, k.upper(), k.lower()):
            v = ctx.s3.get(cand)
            if v is not None and str(v).strip():
                return str(v).strip()
    return ""


def _normalize_worker_aliases(ctx: Ctx) -> None:
    """Fold DASK_WORKER_MEM_LIMIT / MEM_REQUEST into canonical keys on ctx.s3.

    Package honors DASK_WORKER_MEMORY only; operators following older docs may
    still export MEM_LIMIT / MEM_REQUEST. Mutates ctx.s3 in place so subsequent
    zarf deploys also get the canonical names.
    """
    mem = _s3_lookup(ctx, "DASK_WORKER_MEMORY")
    if not mem:
        alt = _s3_lookup(ctx, "DASK_WORKER_MEM_LIMIT")
        if not alt:
            alt = _s3_lookup(ctx, "DASK_WORKER_MEM_REQUEST")
        if alt:
            ctx.s3["DASK_WORKER_MEMORY"] = alt


def _normalize_k8s_qty(v: Optional[str]) -> str:
    """Loose equality for CPU/memory quantities ('2'=='2.0', '6Gi'=='6gi')."""
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    # CPU millicores
    if s.endswith("m") and s[:-1].replace(".", "", 1).isdigit():
        try:
            return f"{float(s[:-1]) / 1000:g}"
        except ValueError:
            return s.lower()
    # Bare number (CPU cores)
    try:
        return f"{float(s):g}"
    except ValueError:
        pass
    # Memory → GiB then back to a canonical "Xgi" string
    gib = _mem_to_gib(s)
    if gib > 0:
        return f"{gib:g}gi"
    return s.lower()


def _arg_after(args: list, flag: str) -> Optional[str]:
    """Value following ``flag`` in a container args list, or None."""
    for i, a in enumerate(args):
        if a == flag and i + 1 < len(args):
            return str(args[i + 1])
        # also accept --flag=value
        if isinstance(a, str) and a.startswith(flag + "="):
            return a.split("=", 1)[1]
    return None


def _set_arg(args: list, flag: str, value: str) -> list:
    """Return a copy of args with ``flag value`` set (insert near dask-worker)."""
    out = list(args or [])
    for i, a in enumerate(out):
        if a == flag and i + 1 < len(out):
            out[i + 1] = str(value)
            return out
        if isinstance(a, str) and a.startswith(flag + "="):
            out[i] = f"{flag}={value}"
            return out
    # Insert after binary name when present
    insert_at = 1 if out and not str(out[0]).startswith("-") else len(out)
    out[insert_at:insert_at] = [flag, str(value)]
    return out


def _desired_worker_sizing(ctx: Ctx) -> dict:
    """Desired worker sizing from ctx.s3 (after alias fold).

    ``replicas`` is int when DASK_WORKER_REPLICAS is set, else None (do not
    force scale-up — only cap oversubscription). Sizing fields are None when
    unset so we do not thrash a manually sized CR back to package defaults.
    """
    _normalize_worker_aliases(ctx)
    rep_s = _s3_lookup(ctx, "DASK_WORKER_REPLICAS")
    replicas = None
    if rep_s:
        try:
            replicas = max(1, int(float(rep_s)))
        except ValueError:
            replicas = _WORKER_DEFAULT_REPLICAS
    return {
        "replicas": replicas,
        "replicas_explicit": bool(rep_s),
        "nthreads": _s3_lookup(ctx, "DASK_WORKER_NTHREADS") or None,
        "cpu": _s3_lookup(ctx, "DASK_WORKER_CPU") or None,
        "memory": _s3_lookup(ctx, "DASK_WORKER_MEMORY") or None,
        "mem_request": _s3_lookup(ctx, "DASK_WORKER_MEM_REQUEST") or None,
    }


def _live_worker_sizing(ctx: Ctx) -> Optional[dict]:
    """Read live DaskCluster/cybersec-dask worker sizing. None if CR absent."""
    cr = ctx.get("daskcluster", "cybersec-dask", ns="dask")
    if not cr:
        return None
    w = (cr.get("spec") or {}).get("worker") or {}
    replicas = w.get("replicas")
    try:
        replicas_i = int(replicas) if replicas is not None else None
    except (TypeError, ValueError):
        replicas_i = None
    containers = ((w.get("spec") or {}).get("containers") or [])
    c = next((x for x in containers if x.get("name") == "worker"),
             containers[0] if containers else {})
    args = list(c.get("args") or [])
    nthreads = _arg_after(args, "--nthreads")
    mem_arg = _arg_after(args, "--memory-limit")
    limits = ((c.get("resources") or {}).get("limits") or {})
    requests = ((c.get("resources") or {}).get("requests") or {})
    mem_limit = limits.get("memory") or mem_arg
    return {
        "replicas": replicas_i,
        "nthreads": str(nthreads) if nthreads is not None else None,
        "cpu": str(limits["cpu"]) if limits.get("cpu") is not None else None,
        "memory": str(mem_limit) if mem_limit is not None else None,
        "mem_request": str(requests["memory"]) if requests.get("memory") is not None else None,
        "mem_arg": str(mem_arg) if mem_arg is not None else None,
        "_cr": cr,
        "_worker": w,
        "_container": c,
        "_args": args,
    }


def _mem_fit_workers(ctx: Ctx, per_worker_memory: str) -> int:
    """Max workers that fit total allocatable RAM after headroom."""
    cap = ctx.node_capacity()
    total = float(cap.get("total_mem_gib") or 0.0)
    wgib = _mem_to_gib(per_worker_memory) or _mem_to_gib(_WORKER_DEFAULT_MEMORY) or 6.0
    if wgib <= 0:
        wgib = 6.0
    usable = total - _WORKER_MEM_HEADROOM_GIB
    if usable < wgib:
        return 1
    return max(1, int(usable // wgib))


def _target_worker_replicas(ctx: Ctx, desired: dict, live: Optional[dict],
                            pending_count: int = 0,
                            non_pending_count: int = 0) -> int:
    """Capacity-capped replica target.

    * Explicit ``DASK_WORKER_REPLICAS`` → min(desired, mem_fit), then pending shrink.
    * Unset → keep live count (min 1), only shrink for mem_fit / Pending.
    Replaces the old ``schedulable_nodes − 1`` heuristic that pinned fat single
    nodes to one worker.
    """
    per_mem = (desired.get("memory")
               or (live or {}).get("memory")
               or _WORKER_DEFAULT_MEMORY)
    mem_fit = _mem_fit_workers(ctx, per_mem)
    if desired.get("replicas") is not None:
        want = int(desired["replicas"])
    else:
        want = int((live or {}).get("replicas") or _WORKER_DEFAULT_REPLICAS)
        # Without an explicit desired, never scale *up* — only preserve/cap.
        if live and live.get("replicas") is not None:
            want = min(want, int(live["replicas"]))
    target = max(1, min(want, mem_fit))
    # Pending oversubscription: do not keep asking for pods that cannot schedule.
    if pending_count > 0 and non_pending_count >= 1:
        target = min(target, non_pending_count)
    elif pending_count > 0 and non_pending_count == 0:
        target = 1
    return max(1, target)


def _worker_sizing_drifts(live: dict, desired: dict, target_replicas: int) -> list:
    """Human-readable field drifts (replicas + explicit sizing fields only)."""
    drifts = []
    live_rep = live.get("replicas")
    if live_rep is None or int(live_rep) != int(target_replicas):
        drifts.append(f"replicas {live_rep}→{target_replicas}")
    for key, label in (("nthreads", "nthreads"), ("cpu", "cpu"),
                       ("memory", "memory"), ("mem_request", "mem_request")):
        want = desired.get(key)
        if not want:
            continue  # unset → do not force package default onto live CR
        have = live.get(key)
        if key == "memory" and not have:
            have = live.get("mem_arg")
        if _normalize_k8s_qty(have) != _normalize_k8s_qty(want):
            drifts.append(f"{label} {have or '∅'}→{want}")
    # --memory-limit arg can drift from limits.memory even when desired matches limits
    if desired.get("memory") and live.get("mem_arg"):
        if _normalize_k8s_qty(live["mem_arg"]) != _normalize_k8s_qty(desired["memory"]):
            msg = f"memory-arg {live['mem_arg']}→{desired['memory']}"
            if msg not in drifts and not any(d.startswith("memory ") for d in drifts):
                drifts.append(msg)
    return drifts


def _stamp_worker_sizing(ctx: Ctx, desired: dict, target_replicas: int) -> None:
    """Write effective sizing into ctx.s3 so later zarf deploys preserve it."""
    ctx.s3["DASK_WORKER_REPLICAS"] = str(target_replicas)
    if desired.get("nthreads"):
        ctx.s3["DASK_WORKER_NTHREADS"] = str(desired["nthreads"])
    if desired.get("cpu"):
        ctx.s3["DASK_WORKER_CPU"] = str(desired["cpu"])
    if desired.get("memory"):
        ctx.s3["DASK_WORKER_MEMORY"] = str(desired["memory"])
    if desired.get("mem_request"):
        ctx.s3["DASK_WORKER_MEM_REQUEST"] = str(desired["mem_request"])


def _patch_daskcluster_worker_sizing(ctx: Ctx, live: dict, desired: dict,
                                     target_replicas: int) -> tuple:
    """Merge-patch DaskCluster worker replicas + template sizing.

    Returns (changed: bool, detail: str, template_changed: bool).
    ``template_changed`` means nthreads/cpu/memory changed → must bounce workers.
    """
    w = dict(live.get("_worker") or {})
    w["replicas"] = int(target_replicas)
    # Deep-ish copy of pod template so we do not mutate the cached live dict oddly
    spec = dict(w.get("spec") or {})
    containers = [dict(c) for c in (spec.get("containers") or [])]
    if not containers:
        containers = [{"name": "worker", "args": ["dask-worker"]}]
    # Prefer the container named worker
    idx = next((i for i, c in enumerate(containers) if c.get("name") == "worker"), 0)
    c = dict(containers[idx])
    args = list(c.get("args") or live.get("_args") or ["dask-worker"])
    template_changed = False

    if desired.get("nthreads"):
        before = _arg_after(args, "--nthreads")
        args = _set_arg(args, "--nthreads", str(desired["nthreads"]))
        if _normalize_k8s_qty(before) != _normalize_k8s_qty(desired["nthreads"]):
            template_changed = True
    if desired.get("memory"):
        before = _arg_after(args, "--memory-limit")
        args = _set_arg(args, "--memory-limit", str(desired["memory"]))
        if _normalize_k8s_qty(before) != _normalize_k8s_qty(desired["memory"]):
            template_changed = True
    c["args"] = args

    resources = dict(c.get("resources") or {})
    limits = dict(resources.get("limits") or {})
    requests = dict(resources.get("requests") or {})
    if desired.get("cpu"):
        if _normalize_k8s_qty(limits.get("cpu")) != _normalize_k8s_qty(desired["cpu"]):
            template_changed = True
        limits["cpu"] = str(desired["cpu"])
    if desired.get("memory"):
        if _normalize_k8s_qty(limits.get("memory")) != _normalize_k8s_qty(desired["memory"]):
            template_changed = True
        limits["memory"] = str(desired["memory"])
    if desired.get("mem_request"):
        if _normalize_k8s_qty(requests.get("memory")) != _normalize_k8s_qty(desired["mem_request"]):
            template_changed = True
        requests["memory"] = str(desired["mem_request"])
    if limits:
        resources["limits"] = limits
    if requests:
        resources["requests"] = requests
    if resources:
        c["resources"] = resources
    containers[idx] = c
    spec["containers"] = containers
    w["spec"] = spec

    rep_changed = live.get("replicas") != int(target_replicas)
    if not rep_changed and not template_changed:
        return False, f"DaskCluster worker already at target {target_replicas}", False

    r = ctx.k(["patch", "daskcluster", "cybersec-dask", "-n", "dask", "--type", "merge",
               "-p", json.dumps({"spec": {"worker": w}})])
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "").strip()[:300]
        return False, f"daskcluster patch failed: {err}", False
    parts = []
    if rep_changed:
        parts.append(f"replicas→{target_replicas}")
    if template_changed:
        fields = [k for k in ("nthreads", "cpu", "memory", "mem_request") if desired.get(k)]
        parts.append("sizing " + ",".join(fields))
    return True, "patched DaskCluster worker: " + ", ".join(parts), template_changed


def _reap_excess_worker_deployments(ctx: Ctx, target: int) -> tuple:
    """Delete excess worker Deployments (Pending/least-ready first). (reaped, detail)."""
    deps = ctx.items("deployments", ns="dask", selector="dask.org/component=worker")
    excess = len(deps) - target
    if excess <= 0:
        return 0, f"{len(deps)} worker deployment(s) ≤ target {target}"
    deps.sort(key=lambda d: d.get("status", {}).get("readyReplicas") or 0)
    reaped = 0
    for d in deps[:excess]:
        if ctx.k(["delete", "deployment", d["metadata"]["name"], "-n", "dask",
                  "--wait=false"]).returncode == 0:
            reaped += 1
    return reaped, f"reaped {reaped}/{excess} excess worker deployment(s)"


def _recycle_worker_pods(ctx: Ctx) -> str:
    """Force worker pods to recreate so they pick up template sizing/image."""
    r = ctx.k(["delete", "pod", "-n", "dask", "-l", "dask.org/component=worker",
               "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
    if r.returncode == 0:
        return "recycled worker pods (template pickup)"
    return f"worker pod recycle rc={r.returncode}"


def _det_workers_capacity(ctx: Ctx) -> Probe:
    """Workers match desired sizing (when set), fit RAM capacity, none Pending.

    Detects:
      * Pending workers (oversubscription stranding panel memory)
      * CR / Deployment count drift vs capacity-capped target
      * Explicit DASK_WORKER_* sizing drift (nthreads / cpu / memory)
    """
    live = _live_worker_sizing(ctx)
    if live is None:
        # CR absent is T4.scheduler's job — not a capacity failure. Returning
        # FAIL here thrashes rem every pass while scheduler Deployment may
        # already be healthy (converge-11/12: "missing" spam + false progress).
        return Probe(True, "DaskCluster CR absent — capacity N/A until T4.scheduler "
                           "creates it (not a worker oversubscription failure)")
    desired = _desired_worker_sizing(ctx)
    pods = ctx.items("pods", ns="dask", selector="dask.org/component=worker")
    pending = [p for p in pods
               if (p.get("status") or {}).get("phase") == "Pending"]
    non_pending = len(pods) - len(pending)
    target = _target_worker_replicas(ctx, desired, live, len(pending), non_pending)
    drifts = _worker_sizing_drifts(live, desired, target)
    deps = ctx.items("deployments", ns="dask", selector="dask.org/component=worker")
    issues = []
    if pending:
        issues.append(f"{len(pending)} worker(s) Pending (oversubscribed)")
    if drifts:
        issues.append("sizing drift: " + ", ".join(drifts))
    if len(deps) > target:
        issues.append(f"{len(deps)} worker Deployments > target {target}")
    # Under-count when operator asked for more and capacity allows (no Pending)
    if (desired.get("replicas") is not None
            and not pending
            and (live.get("replicas") or 0) < target):
        issues.append(f"under-provisioned: CR replicas {live.get('replicas')} < target {target}")
    if issues:
        return Probe(False, "; ".join(issues)
                     + f"  [target={target} live={live.get('replicas')} "
                     f"pods={len(pods)} pending={len(pending)} "
                     f"mem_fit={_mem_fit_workers(ctx, desired.get('memory') or live.get('memory') or _WORKER_DEFAULT_MEMORY)}]")
    size_bits = []
    if live.get("nthreads"):
        size_bits.append(f"nthreads={live['nthreads']}")
    if live.get("cpu"):
        size_bits.append(f"cpu={live['cpu']}")
    if live.get("memory"):
        size_bits.append(f"mem={live['memory']}")
    extra = (" " + " ".join(size_bits)) if size_bits else ""
    return Probe(True, f"{len(pods)} worker(s), none Pending, "
                       f"replicas={live.get('replicas')} target={target}{extra}")


def _rem_workers_capacity(ctx: Ctx) -> Fix:
    """Surgical worker scale + sizing — no zarf re-push, no CR recreate.

    1. Fold MEM_LIMIT/MEM_REQUEST aliases → canonical DASK_WORKER_* on ctx.s3.
    2. Compute capacity-capped target (RAM headroom, Pending shrink).
    3. Merge-patch DaskCluster worker.replicas + template (nthreads/cpu/memory).
    4. If template sizing changed: recycle worker pods (operator often skips roll).
    5. Reap orphaned/excess worker Deployments (least-ready first).
    6. Stamp effective sizing into ctx.s3 so later component deploys preserve it.
    """
    live = _live_worker_sizing(ctx)
    if live is None:
        return Fix(False, "no DaskCluster cybersec-dask — T4.scheduler must create it first")
    desired = _desired_worker_sizing(ctx)
    pods = ctx.items("pods", ns="dask", selector="dask.org/component=worker")
    pending = [p for p in pods
               if (p.get("status") or {}).get("phase") == "Pending"]
    non_pending = len(pods) - len(pending)
    target = _target_worker_replicas(ctx, desired, live, len(pending), non_pending)
    drifts = _worker_sizing_drifts(live, desired, target)
    deps = ctx.items("deployments", ns="dask", selector="dask.org/component=worker")
    needs_work = bool(drifts) or len(deps) > target or bool(pending)
    if not needs_work and (live.get("replicas") or 0) >= target:
        _stamp_worker_sizing(ctx, desired, target)
        return Fix(False, f"workers already at target {target} "
                          f"(replicas={live.get('replicas')}, pods={len(pods)}, "
                          f"deploys={len(deps)}); nothing to do")

    actions = []
    # Always stamp before patch so concurrent zarf paths see the cap
    _stamp_worker_sizing(ctx, desired, target)

    changed, detail, template_changed = _patch_daskcluster_worker_sizing(
        ctx, live, desired, target)
    if detail:
        actions.append(detail)
    if not changed and "failed" in detail:
        return Fix(False, detail)

    if template_changed or any(
            d.startswith(("nthreads", "cpu", "memory", "mem_request", "memory-arg"))
            for d in drifts):
        actions.append(_recycle_worker_pods(ctx))

    reaped, reap_detail = _reap_excess_worker_deployments(ctx, target)
    if reaped > 0:
        actions.append(reap_detail)
    elif len(deps) > target:
        actions.append(reap_detail)

    cap = ctx.node_capacity()
    summary = (f"target={target} (desired={desired.get('replicas') or 'live'}, "
               f"mem_fit={_mem_fit_workers(ctx, desired.get('memory') or live.get('memory') or _WORKER_DEFAULT_MEMORY)}, "
               f"nodes={cap.get('schedulable_nodes')} mem={cap.get('total_mem_gib')}Gi)")
    if not actions:
        return Fix(False, f"no worker changes applied; {summary}")
    return Fix(True, f"{' | '.join(actions)}  [{summary}]")


# --- image-drift detection (so `apply` rolls a content/tag change) ----------
# converge's workload detects are otherwise readiness-based, so they no-op on a
# code change. These compare the RUNNING pod's cybersec-dask tag vs the TARGET tag
# in artifacts.manifest: a mismatch is drift -> re-deploy. Paired with a per-build
# tag (content-derived; the build flow's job) this makes a redeploy a single
# idempotent `converge apply`, retiring the manual forced component deploy.

def _image_tag(ref: str) -> str:
    """Original tag from a (possibly zarf-rewritten) image ref:
    '<reg>/cybersec-dask:2025.2.1-zarf-HASH' -> '2025.2.1'; digest-only -> ''."""
    ref = ref.split("@", 1)[0]
    last = ref.rsplit("/", 1)[-1]
    tag = ref.rsplit(":", 1)[-1] if ":" in last else ""
    return tag.split("-zarf-", 1)[0]


def _retarget_image_ref(ref: str, target_tag: str) -> str:
    """Rewrite a (possibly zarf-rewritten) cybersec-dask ref to ``target_tag``.

    Preserves the registry host (``127.0.0.1:31999`` etc.) so pulls stay
    in-cluster. Drops any ``-zarf-HASH`` suffix — that hash is content-bound to
    the *old* push; the new tag is pullable as ``host/cybersec-dask:TARGET``
    after ``cybersec-images`` re-push (zarf may rewrite again on next create).

    '127.0.0.1:31999/cybersec-dask:2025.2.0-OLD-zarf-ABC' + '2025.2.0-NEW'
      → '127.0.0.1:31999/cybersec-dask:2025.2.0-NEW'
    """
    if not ref or not target_tag or "cybersec-dask" not in ref:
        return ref
    base, _at, _digest = ref.partition("@")
    if ":" not in base.rsplit("/", 1)[-1]:
        return ref
    prefix, _, _tag = base.rpartition(":")
    return f"{prefix}:{target_tag}"


def _target_cybersec_tag(ctx: Ctx) -> str:
    for img in ctx.manifest.get("package_images", {}).get("images", []):
        if "cybersec-dask" in img.get("ref", ""):
            return _image_tag(img["ref"])
    return ""


def _target_cybersec_ref(ctx: Ctx) -> str:
    """Preferred full ref from artifacts.manifest (pre-zarf-rewrite)."""
    for img in ctx.manifest.get("package_images", {}).get("images", []):
        if "cybersec-dask" in img.get("ref", ""):
            return img["ref"]
    return ""


def _image_drift(ctx: Ctx, ns: str, selector: str) -> "str | None":
    """A drift message if the running cybersec-dask pod isn't on the target tag,
    else None. Conservative: unknown target / no image -> no drift (never blocks)."""
    target = _target_cybersec_tag(ctx)
    if not target:
        return None
    for p in ctx.items("pods", ns=ns, selector=selector):
        for c in p.get("spec", {}).get("containers", []):
            img = c.get("image", "")
            if "cybersec-dask" in img:
                running = _image_tag(img)
                if running and running != target:
                    return f"image drift: running {running}, target {target}"
    return None


def _running_cybersec_ref(ctx: Ctx, ns: str, selector: str) -> str:
    for p in ctx.items("pods", ns=ns, selector=selector):
        for c in p.get("spec", {}).get("containers", []):
            img = c.get("image", "")
            if "cybersec-dask" in img:
                return img
    return ""


def _retarget_deployments_cybersec(ctx: Ctx, ns: str,
                                   selector: Optional[str] = None,
                                   target_tag: str = "") -> List[str]:
    """Patch Deployments in ``ns`` whose containers use cybersec-dask → target_tag.

    Used for Dask children and panel-viz (otel-navigator, navigator-engine).
    Caller must recycle pods after — Deployment template change alone is not
    enough under IfNotPresent + stale RS.
    """
    actions: List[str] = []
    target_tag = target_tag or _target_cybersec_tag(ctx)
    if not target_tag:
        return actions
    deps = (ctx.items("deployments", ns=ns, selector=selector) if selector
            else ctx.items("deployments", ns=ns))
    for dep in deps:
        name = (dep.get("metadata") or {}).get("name")
        if not name:
            continue
        tpl = (((dep.get("spec") or {}).get("template") or {}).get("spec") or {})
        containers = tpl.get("containers") or []
        new_containers = []
        dep_changed = False
        pairs: List[str] = []
        for c in containers:
            c = dict(c)
            img = c.get("image") or ""
            if "cybersec-dask" in img:
                new = _retarget_image_ref(img, target_tag)
                if new != img:
                    c["image"] = new
                    dep_changed = True
                    pairs.append(f"{c.get('name', 'app')}={new}")
            new_containers.append(c)
        if not dep_changed:
            continue
        r = ctx.k(["set", "image", f"deployment/{name}", "-n", ns, *pairs])
        if r.returncode != 0:
            patch = {"spec": {"template": {"spec": {"containers": new_containers}}}}
            r = ctx.k(["patch", "deployment", name, "-n", ns,
                       "--type", "strategic", "-p", json.dumps(patch)])
        actions.append(
            f"retargeted {ns}/Deployment/{name} → {target_tag}"
            if r.returncode == 0 else
            f"{ns}/Deployment/{name} retarget rc={r.returncode}")
    return actions


def _retarget_dask_workload_images(ctx: Ctx, target_tag: str) -> List[str]:
    """Patch DaskCluster CR + Deployments so children recreate on target tag.

    Operator is creation-only for many fields and will not roll image-only CR
    updates — we patch both the CR (source of truth for next CREATE) and live
    Deployments, then the caller must delete pods. Never touches Layer-A.
    """
    actions: List[str] = []
    if not target_tag:
        return actions

    # --- DaskCluster CR ---
    cr = ctx.get("daskcluster", "cybersec-dask", ns="dask")
    if cr:
        w = (cr.get("spec") or {}).get("worker") or {}
        s = (cr.get("spec") or {}).get("scheduler") or {}
        changed = False
        for role, block in (("scheduler", s), ("worker", w)):
            containers = ((block.get("spec") or {}).get("containers") or [])
            for c in containers:
                img = c.get("image") or ""
                if "cybersec-dask" not in img:
                    continue
                new = _retarget_image_ref(img, target_tag)
                if new != img:
                    c["image"] = new
                    changed = True
        if changed:
            patch = {"spec": {"scheduler": s, "worker": w}}
            r = ctx.k(["patch", "daskcluster", "cybersec-dask", "-n", "dask",
                       "--type", "merge", "-p", json.dumps(patch)])
            actions.append(
                f"patched DaskCluster images → {target_tag}"
                if r.returncode == 0 else
                f"DaskCluster image patch rc={r.returncode}")

    # --- Live Deployments ---
    for sel in ("dask.org/component=scheduler", "dask.org/component=worker"):
        actions.extend(_retarget_deployments_cybersec(ctx, "dask", sel, target_tag))
    return actions


def _rem_panel_image_drift(ctx: Ctx, app: str, components: str) -> List[str]:
    """Clear cybersec-dask image drift on a panel-viz app (converge-11/12 class).

    Same pathology as scheduler: zarf/helm updates leave Deployment on old tag;
    recycle-only recreates from the old template. Push → retarget Deployment →
    recycle; package redeploy if still drifted.
    """
    actions: List[str] = []
    sel = f"app={app}"
    drift = _image_drift(ctx, _PANEL_NS, sel)
    if not drift:
        return actions
    target = _target_cybersec_tag(ctx)
    actions.append(f"{app} {drift}")
    if ctx.have_zarf() and ctx.package_path:
        fix = _zarf_deploy_components(ctx, "cybersec-images")
        actions.append(fix.detail)
    actions.extend(_retarget_deployments_cybersec(ctx, _PANEL_NS, sel, target))
    actions.extend(_recycle_panel_pods(ctx, sel))
    if _image_drift(ctx, _PANEL_NS, sel) and ctx.have_zarf() and ctx.package_path:
        fix = _zarf_deploy_components(ctx, components)
        actions.append(fix.detail)
        actions.extend(_retarget_deployments_cybersec(ctx, _PANEL_NS, sel, target))
        actions.extend(_recycle_panel_pods(ctx, sel))
    return actions


# --------------------------------------------------------------------------- #
# Panel stack (otel-navigator + navigator-engine + shared S3 config)
# --------------------------------------------------------------------------- #
# Field modes that leave otel-navigator "Ready" in the naive sense but broken
# for operators, or not Ready at all:
#   • blank S3_BUCKET / OTEL_DATA_PATH=s3:///  (TCP probe still green)
#   • ImagePullBackOff (stale tag, registry empty, agent poison)
#   • CrashLoopBackOff / OOMKilled
#   • 1/2 containers (panel up, pty-proxy down — or reverse)
#   • Pending (4Gi request stranded by worker oversubscription)
#   • image tag drift vs package
#   • ns absent / deploy missing (never installed)
# Rem always prefers ``zarf package deploy --components=…`` (in-situ unblock),
# then surgical recycle when deploy is healthy but pods stuck.

_PANEL_NS = "panel-viz"
_PANEL_CM = "otel-navigator-config"
_PANEL_SECRET = "otel-navigator-credentials"
_IMAGE_WAIT_BAD = ("ImagePullBackOff", "ErrImagePull", "ErrImageNeverPull")
_WAIT_BAD = _IMAGE_WAIT_BAD + ("CrashLoopBackOff", "CreateContainerConfigError",
                               "InvalidImageName", "RunContainerError")


def _panel_s3_config_issue(ctx: Ctx) -> Optional[str]:
    """Non-None if shared panel ConfigMap/Secret would brick the apps at runtime.

    TCP readiness probes do NOT catch this — pods can be Ready with S3_BUCKET=''
    and the UI dies on first data load ('Invalid bucket name s3:').
    """
    if not ctx.exists("namespace", _PANEL_NS):
        return None  # not installed yet — deploy path handles create
    cm = ctx.get("configmap", _PANEL_CM, ns=_PANEL_NS)
    if not cm:
        return f"{_PANEL_CM} ConfigMap absent (panel never fully deployed or wiped)"
    data = cm.get("data") or {}
    bucket = (data.get("S3_BUCKET") or "").strip()
    path = (data.get("OTEL_DATA_PATH") or "").strip()
    if not bucket or "ZARF_VAR" in bucket:
        return (f"S3_BUCKET empty/unrendered in {_PANEL_CM} ({bucket!r}) — "
                "OTEL_DATA_PATH becomes s3:/// and bricks the app")
    if path in ("", "s3://", "s3:///") or path.rstrip("/").endswith("s3:"):
        return f"OTEL_DATA_PATH={path!r} (blank bucket rendered into ConfigMap)"
    # ctx.s3 is authoritative when provided — detect deploy/config drift.
    want = (ctx.s3.get("S3_BUCKET") or "").strip()
    if want and bucket != want:
        return (f"S3_BUCKET drift: cluster={bucket!r} converge={want!r} — "
                "redeploy panel-viz with current --creds-file / S3_*")
    sec = ctx.get("secret", _PANEL_SECRET, ns=_PANEL_NS)
    if not sec:
        return f"{_PANEL_SECRET} Secret absent"
    sdata = sec.get("data") or {}
    # presence only — never surface values
    if not sdata.get("AWS_ACCESS_KEY_ID") or not sdata.get("AWS_SECRET_ACCESS_KEY"):
        return f"{_PANEL_SECRET} missing AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY material"
    return None


# --------------------------------------------------------------------------- #
# In-situ crash log census (logs never leave the closed world)
# --------------------------------------------------------------------------- #
# Operators cannot exfiltrate pod logs; when CrashLoop/high restartCount is
# visible, the engine pulls a short tail and classifies high-signal findings so
# remediations and LIVE STATE name the next action without kubectl folklore.

_LOG_TAIL_LINES = 100
_RESTART_LOG_THRESHOLD = 3  # sample logs at ≥ this restartCount when not ready

# (regex, finding_id, operator/engine hint) — first match wins per line; we keep
# unique finding_ids. Never capture secrets (no patterns on KEY=value tokens).
_LOG_FINDINGS: Tuple[Tuple[re.Pattern, str, str], ...] = (
    (re.compile(r"ModuleNotFoundError|ImportError|No module named", re.I),
     "import_error",
     "image missing Python deps — rebuild/push cybersec-dask (no pip air-gap)"),
    (re.compile(r"Invalid bucket name|NoSuchBucket|S3.*AccessDenied|403 Forbidden.*[Ss]3|"
                r"Unable to locate credentials|Could not connect to the endpoint", re.I),
     "s3_auth_or_bucket",
     "S3 path/creds — check ConfigMap S3_BUCKET + Secret keys; config-only rem"),
    (re.compile(r"Address already in use|EADDRINUSE|bind.*98", re.I),
     "port_in_use",
     "port conflict — recycle pod; if persists, check hostNetwork/NodePort clash"),
    (re.compile(r"OOM|MemoryError|Cannot allocate memory|heap out of memory", re.I),
     "oom",
     "memory pressure — cap workers (T4.workers-capacity) or raise limits"),
    (re.compile(r"Permission denied|EACCES|Read-only file system", re.I),
     "permission",
     "filesystem perms — hostPath/spill/registry mode (chmod 0777 registry path)"),
    (re.compile(r"Connection refused|Name or service not known|Temporary failure in name|"
                r"nodename nor servname|Failed to resolve", re.I),
     "dns_or_connect",
     "in-cluster DNS/service — check endpoints + coredns; wait for deps Ready"),
    (re.compile(r"SSL|certificate verify failed|x509|TLS", re.I),
     "tls",
     "TLS/cert issue — air-gap often needs verify=false or correct CA bundle"),
    (re.compile(r"Panel|Bokeh|tornado|WebSocket|ghostty", re.I),
     "panel_ui",
     "Panel/Bokeh/WS stack — check BOKEH_RESOURCES=server and /ws ingress"),
    (re.compile(r"Traceback \(most recent call last\)", re.I),
     "python_traceback",
     "Python crash — see IN SITU LOG excerpt below for the exception type"),
    (re.compile(r"FATAL|panic:|runtime error:|segmentation fault", re.I),
     "fatal",
     "process fatal — excerpt below; often OOM or bad binary/arch"),
    (re.compile(r"Error:|Exception:|FAILED|critical", re.I),
     "generic_error",
     "error line in logs — see IN SITU LOG excerpt"),
)


def _container_crashy(cs: dict) -> bool:
    waiting = ((cs.get("state") or {}).get("waiting") or {})
    if waiting.get("reason") in ("CrashLoopBackOff", "CreateContainerConfigError",
                                   "RunContainerError"):
        return True
    term = ((cs.get("state") or {}).get("terminated") or {})
    if term.get("reason") in ("OOMKilled", "Error", "ContainerCannotRun"):
        return True
    last = ((cs.get("lastState") or {}).get("terminated") or {})
    if last.get("reason") in ("OOMKilled", "Error"):
        return True
    rc = int(cs.get("restartCount") or 0)
    if rc >= _RESTART_LOG_THRESHOLD and not cs.get("ready"):
        return True
    return False


def _pod_needs_log_census(pod: dict) -> bool:
    for cs in (pod.get("status") or {}).get("containerStatuses") or []:
        if _container_crashy(cs):
            return True
    phase = (pod.get("status") or {}).get("phase")
    return phase in ("Failed", "Unknown")


def _fetch_container_log(ctx: Ctx, ns: str, pod: str, container: str,
                         *, previous: bool = False) -> str:
    args = ["logs", "-n", ns, pod, f"--tail={_LOG_TAIL_LINES}", f"-c={container}"]
    if previous:
        args.append("--previous")
    r = ctx.k(args, timeout=30)
    text = Ctx.out_text(r.stdout) if r.returncode == 0 else ""
    if not text.strip() and not previous:
        # CrashLoop often needs the previous terminated instance
        return _fetch_container_log(ctx, ns, pod, container, previous=True)
    return text


def _classify_log_text(text: str) -> List[dict]:
    """Return unique findings [{id, hint, evidence}] from a log tail."""
    found: List[dict] = []
    seen = set()
    if not text:
        return found
    for line in text.splitlines():
        s = line.strip()
        if not s or len(s) > 400:
            s = s[:400]
        # Never echo lines that look like secret material
        if re.search(r"(SECRET|PASSWORD|TOKEN|AWS_SECRET|AKIA[0-9A-Z]{16})\s*[=:]",
                     s, re.I):
            continue
        for pat, fid, hint in _LOG_FINDINGS:
            if fid in seen:
                continue
            if pat.search(s):
                seen.add(fid)
                found.append({
                    "id": fid,
                    "hint": hint,
                    "evidence": s[:200],
                })
                break
    return found


def _crash_log_census(ctx: Ctx, ns: str, selector: str) -> List[str]:
    """In-situ log facts for crashy pods under selector (closed-world only).

    Returns human lines for detect detail / LIVE STATE — never secret values.
    """
    lines: List[str] = []
    pods = ctx.items("pods", ns=ns, selector=selector)
    crashy = [p for p in pods if _pod_needs_log_census(p)]
    if not crashy:
        return lines
    lines.append("IN SITU LOG (engine-sampled; not exfiltrated):")
    for p in crashy[:3]:  # bound work
        pname = (p.get("metadata") or {}).get("name", "?")
        statuses = (p.get("status") or {}).get("containerStatuses") or []
        containers = [cs for cs in statuses if _container_crashy(cs)]
        if not containers:
            containers = statuses[:1]
        for cs in containers[:2]:
            cname = cs.get("name") or "main"
            waiting = ((cs.get("state") or {}).get("waiting") or {})
            reason = waiting.get("reason") or (
                ((cs.get("lastState") or {}).get("terminated") or {}).get("reason")
                or "crash")
            lines.append(
                f"  {ns}/{pname}:{cname} reason={reason} "
                f"restarts={cs.get('restartCount', 0)}")
            text = _fetch_container_log(ctx, ns, pname, cname)
            findings = _classify_log_text(text)
            if findings:
                for f in findings[:5]:
                    lines.append(f"    FINDING [{f['id']}]: {f['hint']}")
                    lines.append(f"      evidence: {f['evidence']}")
            else:
                # Last non-empty lines as raw evidence (scrubbed)
                raw = [ln.strip() for ln in text.splitlines() if ln.strip()]
                raw = [ln for ln in raw
                       if not re.search(
                           r"(SECRET|PASSWORD|TOKEN|AWS_SECRET)\s*[=:]", ln, re.I)]
                for ln in raw[-4:]:
                    lines.append(f"      log: {ln[:180]}")
            if not text.strip():
                lines.append("      log: (empty — try: kubectl -n %s logs %s -c %s "
                             "--previous --tail=80)" % (ns, pname, cname))
    return lines


def _pod_container_issues(pod: dict, *, expect_containers: int = 0) -> List[str]:
    """Per-container waiting/terminated reasons + ready-count mismatch."""
    issues: List[str] = []
    phase = (pod.get("status") or {}).get("phase") or "?"
    name = (pod.get("metadata") or {}).get("name", "?")
    if phase == "Pending":
        issues.append(f"{name} Pending (capacity/scheduling)")
    elif phase in ("Failed", "Unknown"):
        issues.append(f"{name} phase={phase}")
    statuses = list((pod.get("status") or {}).get("containerStatuses") or [])
    ready_n = sum(1 for cs in statuses if cs.get("ready"))
    if expect_containers and statuses and ready_n < expect_containers:
        issues.append(f"{name} containers ready {ready_n}/{len(statuses)} "
                      f"(want {expect_containers})")
    for cs in statuses:
        cname = cs.get("name", "?")
        waiting = ((cs.get("state") or {}).get("waiting") or {})
        reason = waiting.get("reason") or ""
        if reason in _WAIT_BAD:
            msg = (waiting.get("message") or "")[:100]
            issues.append(f"{cname}: {reason}" + (f" ({msg})" if msg else ""))
        term = ((cs.get("state") or {}).get("terminated") or {})
        if term.get("reason") == "OOMKilled":
            issues.append(f"{cname}: OOMKilled")
        last = ((cs.get("lastState") or {}).get("terminated") or {})
        # Historical OOM: only fail when flapping or not ready. A single past OOM
        # with Ready + low restarts was driving full zarf redeploys (converge-24/25:
        # ready 1/1 + lastState OOMKilled → --retries 10 / 7200s). Memory rem
        # still runs via _rem_engine_oom when limits are below target.
        if last.get("reason") == "OOMKilled":
            rc = int(cs.get("restartCount") or 0)
            if not cs.get("ready") or rc >= 3:
                issues.append(f"{cname}: lastState OOMKilled (restarts={rc})")
        if (cs.get("restartCount") or 0) >= 5 and not cs.get("ready"):
            issues.append(f"{cname}: restartCount={cs.get('restartCount')} not ready")
    # no statuses yet while Running → still starting
    if phase == "Running" and not statuses:
        issues.append(f"{name} Running but no containerStatuses yet")
    return issues


def _panel_workload_issues(ctx: Ctx, selector: str, *, expect_containers: int = 0,
                           sample_logs: bool = True) -> List[str]:
    issues: List[str] = []
    if not ctx.exists("namespace", _PANEL_NS):
        return [f"{_PANEL_NS} namespace absent"]
    pods = ctx.items("pods", ns=_PANEL_NS, selector=selector)
    if not pods:
        # Deployment present?
        app = selector.split("=", 1)[-1] if "=" in selector else selector
        if not ctx.exists("deploy", app, ns=_PANEL_NS) and not ctx.exists(
                "deployment", app, ns=_PANEL_NS):
            issues.append(f"no pods and no Deployment for {selector}")
        else:
            issues.append(f"Deployment present but no pods for {selector}")
        return issues
    if ctx.pod_image_missing(_PANEL_NS, selector) is True:
        issues.append("ImagePullBackOff/ErrImagePull — image not in closed-world registry")
    crashy = False
    for p in pods:
        issues.extend(_pod_container_issues(p, expect_containers=expect_containers))
        if _pod_needs_log_census(p):
            crashy = True
    drift = _image_drift(ctx, _PANEL_NS, selector)
    if drift:
        issues.append(drift)
    # Pull logs in-situ when crash/restart — facts stay on the operator console
    if sample_logs and crashy:
        issues.extend(_crash_log_census(ctx, _PANEL_NS, selector))
    return issues


def _diagnose_panel(ctx: Ctx, which: str = "otel-navigator") -> str:
    """Human-readable diagnosis for panel stack failures (surfaces in FAIL detail)."""
    bits: List[str] = []
    s3i = _panel_s3_config_issue(ctx)
    if s3i:
        bits.append(s3i)
    if which == "otel-navigator":
        bits.extend(_panel_workload_issues(ctx, "app=otel-navigator", expect_containers=2))
    elif which == "navigator-engine":
        bits.extend(_panel_workload_issues(ctx, "app=navigator-engine", expect_containers=1))
    else:
        bits.extend(_panel_workload_issues(ctx, "app=otel-navigator", expect_containers=2))
        bits.extend(_panel_workload_issues(ctx, "app=navigator-engine", expect_containers=1))
    # capacity context when Pending
    if any("Pending" in b for b in bits):
        cap = ctx.node_capacity()
        bits.append(f"capacity: schedulable_nodes={cap['schedulable_nodes']} "
                    f"mem_gib≈{cap['total_mem_gib']} — otel requests 4Gi; "
                    "cap workers (T4.workers-capacity) if stranded")
    return "; ".join(bits) if bits else "no panel issues detected"


def _panel_live_state(ctx: Ctx) -> str:
    """Compact multi-line snapshot of panel-viz for verify / MANUAL guidance.

    Always safe to print: never secret values — only bucket name, path, key
    presence, pod readiness, and the recommended intervention class.
    """
    lines: List[str] = ["LIVE STATE (panel-viz):"]
    if not ctx.exists("namespace", _PANEL_NS):
        lines.append("  namespace: ABSENT — need zarf package deploy --components=panel-viz")
        return "\n".join(lines)
    lines.append(f"  namespace: {_PANEL_NS} present")

    cm = ctx.get("configmap", _PANEL_CM, ns=_PANEL_NS)
    if not cm:
        lines.append(f"  configmap/{_PANEL_CM}: ABSENT")
    else:
        data = cm.get("data") or {}
        bucket = (data.get("S3_BUCKET") or "").strip()
        path = (data.get("OTEL_DATA_PATH") or "").strip()
        region = (data.get("AWS_REGION") or "").strip()
        lines.append(f"  configmap/{_PANEL_CM}:")
        lines.append(f"    S3_BUCKET={bucket!r}  OTEL_DATA_PATH={path!r}  AWS_REGION={region!r}")
        want = (ctx.s3.get("S3_BUCKET") or "").strip()
        if want:
            lines.append(f"    converge S3_BUCKET={want!r}  "
                         f"{'MATCH' if want == bucket else 'DRIFT — will patch/redeploy'}")
        elif not bucket or path in ("s3:///", "s3://", ""):
            lines.append("    ⚠ blank/unrendered S3 — provide --creds-file / S3_* then "
                         "converge --apply (config-only patch if deploys exist)")

    sec = ctx.get("secret", _PANEL_SECRET, ns=_PANEL_NS)
    if not sec:
        lines.append(f"  secret/{_PANEL_SECRET}: ABSENT")
    else:
        sdata = sec.get("data") or {}
        keys = sorted(sdata.keys())
        has_ak = bool(sdata.get("AWS_ACCESS_KEY_ID"))
        has_sk = bool(sdata.get("AWS_SECRET_ACCESS_KEY"))
        has_ep = bool(sdata.get("S3_ENDPOINT"))
        lines.append(
            f"  secret/{_PANEL_SECRET}: keys={keys}  "
            f"AKID={'yes' if has_ak else 'NO'} SECRET={'yes' if has_sk else 'NO'} "
            f"ENDPOINT={'yes' if has_ep else 'no'}"
        )

    for sel, label, n_expect in (
        ("app=otel-navigator", "otel-navigator", 2),
        ("app=navigator-engine", "navigator-engine", 1),
    ):
        ready, total = ctx.pods_ready(_PANEL_NS, sel)
        # sample_logs=False here — append census once below to avoid double fetch
        issues = _panel_workload_issues(
            ctx, sel, expect_containers=n_expect, sample_logs=False)
        short = [i for i in issues if not i.startswith("IN SITU") and not i.startswith("  ")]
        lines.append(f"  {label}: ready {ready}/{total}"
                     + (f"  issues=[{'; '.join(short[:3])}]" if short else "  OK"))
    # In-situ log census for crashy panel pods (facts stay on the operator console)
    for sel in ("app=otel-navigator", "app=navigator-engine"):
        log_lines = _crash_log_census(ctx, _PANEL_NS, sel)
        if log_lines:
            lines.extend(log_lines)

    s3i = _panel_s3_config_issue(ctx)
    if s3i:
        lines.append(f"  config verdict: FAIL — {s3i}")
        if ctx.s3.get("S3_BUCKET"):
            lines.append("  recommended: config-only rem "
                         "(patch CM/Secret from converge S3_* + rollout restart) "
                         "— no package required if Deployments already exist")
        else:
            lines.append("  recommended: supply S3_BUCKET/S3_ACCESS_KEY/S3_SECRET_KEY "
                         "(--creds-file) then converge --apply")
    else:
        lines.append("  config verdict: CM/Secret look populated")
        # Pod env may lag Secret (no restart) — lengths/host only, never values.
        pod_env = _pod_s3_env_census(ctx)
        if pod_env:
            lines.append(
                f"  pod S3 env: endpoint_set={pod_env.get('endpoint_set')} "
                f"host={pod_env.get('endpoint_host') or '—'} "
                f"akid_len={pod_env.get('akid_len')} "
                f"secret_len={pod_env.get('secret_len')} "
                f"tcp_ok={pod_env.get('tcp_ok')}"
                + (f" err={pod_env.get('error')}" if pod_env.get("error") else "")
            )
            if pod_env.get("endpoint_set") is False:
                lines.append("  ⚠ Secret has keys but pod S3_ENDPOINT empty — "
                             "rollout restart otel-navigator + navigator-engine")
            elif pod_env.get("tcp_ok") is False:
                lines.append("  ⚠ pod cannot TCP to S3_ENDPOINT host — "
                             "fix network / endpoint URL (not creds quoting)")
        lines.append("  next: bash zarf/scripts/verify-s3-datapath.sh  "
                     "(auth + marker + span parquet)")
    return "\n".join(lines)


def _pod_s3_env_census(ctx: Ctx) -> dict:
    """In-pod AWS_*/S3_ENDPOINT presence + TCP reachability (no secret values)."""
    target = None
    if ctx.pods_ready(_PANEL_NS, "app=otel-navigator")[0] >= 1:
        target = (_PANEL_NS, "deploy/otel-navigator", ["-c", "otel-navigator"])
    elif ctx.pods_ready("dask", "dask.org/component=scheduler")[0] >= 1:
        target = ("dask", "deploy/cybersec-dask-scheduler", [])
    if not target:
        return {}
    ns, res, extra = target
    r = ctx.run(
        ctx.kubectl + ["exec", "-n", ns, res] + extra + ["--", "python3", "-c", _S3_POD_ENV_PY],
        timeout=20,
    )
    raw = (r.stdout or "").strip()
    try:
        data = json.loads(raw.splitlines()[-1]) if raw else {}
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {"error": f"pod env census unparseable rc={r.returncode}"}


def _panel_deployments_exist(ctx: Ctx) -> bool:
    return bool(
        ctx.exists("deploy", "otel-navigator", ns=_PANEL_NS)
        or ctx.exists("deployment", "otel-navigator", ns=_PANEL_NS)
    )


def _patch_panel_s3_config(ctx: Ctx) -> Fix:
    """Config-only remediation: write S3 settings from ctx.s3 into the live
    ConfigMap + Secret and rollout-restart panel apps — **no zarf package**.

    Use when Deployments already exist and the failure is blank/drifted S3
    config (the common air-gap field case). Secrets never printed.
    """
    bucket = (ctx.s3.get("S3_BUCKET") or "").strip()
    if not bucket:
        return Fix(False, "config-only rem needs S3_BUCKET in converge context")
    if not _panel_deployments_exist(ctx):
        return Fix(False, "config-only rem needs existing otel-navigator Deployment "
                          "(use zarf package deploy --components=panel-viz)")
    if not ctx.exists("configmap", _PANEL_CM, ns=_PANEL_NS):
        return Fix(False, f"{_PANEL_CM} absent — need component deploy to create objects")

    region = (ctx.s3.get("S3_REGION") or "us-east-1").strip()
    endpoint = (ctx.s3.get("S3_ENDPOINT") or "").strip()
    path = f"s3://{bucket}/"
    actions: List[str] = []

    cm_patch = {
        "data": {
            "S3_BUCKET": bucket,
            "OTEL_DATA_PATH": path,
            "AWS_REGION": region,
        }
    }
    r = ctx.k(["patch", "configmap", _PANEL_CM, "-n", _PANEL_NS,
               "--type", "merge", "-p", json.dumps(cm_patch)])
    if r.returncode != 0:
        return Fix(False, f"patch {_PANEL_CM} failed: {(r.stderr or r.stdout or '')[:200]}")
    actions.append(f"patched {_PANEL_CM} S3_BUCKET={bucket} OTEL_DATA_PATH={path}")

    # stringData lets the API server base64-encode; never log values.
    string_data: dict = {}
    if ctx.s3.get("S3_ACCESS_KEY"):
        string_data["AWS_ACCESS_KEY_ID"] = ctx.s3["S3_ACCESS_KEY"]
    if ctx.s3.get("S3_SECRET_KEY"):
        string_data["AWS_SECRET_ACCESS_KEY"] = ctx.s3["S3_SECRET_KEY"]
    if ctx.s3.get("S3_SESSION_TOKEN") is not None:
        string_data["AWS_SESSION_TOKEN"] = ctx.s3.get("S3_SESSION_TOKEN") or ""
    string_data["S3_ENDPOINT"] = endpoint

    # Apply Secret via stdin (stringData) — never put key material on kubectl argv/ps.
    sec_obj = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": _PANEL_SECRET, "namespace": _PANEL_NS},
        "type": "Opaque",
        "stringData": string_data,
    }
    r = ctx.apply_yaml(json.dumps(sec_obj))
    if r.returncode != 0:
        return Fix(False,
                   f"apply {_PANEL_SECRET} failed: {(r.stderr or r.stdout or '')[:200]}  "
                   f"[{'; '.join(actions)}]")
    actions.append(
        f"{'updated' if ctx.exists('secret', _PANEL_SECRET, ns=_PANEL_NS) else 'created'} "
        f"{_PANEL_SECRET} via apply (AKID/SECRET/ENDPOINT; values not on argv)"
    )

    for dep in ("otel-navigator", "navigator-engine"):
        if ctx.exists("deploy", dep, ns=_PANEL_NS) or ctx.exists("deployment", dep, ns=_PANEL_NS):
            rr = ctx.k(["rollout", "restart", f"deployment/{dep}", "-n", _PANEL_NS])
            if rr.returncode == 0:
                actions.append(f"rollout restart deployment/{dep}")
            else:
                actions.extend(_recycle_panel_pods(ctx, f"app={dep}"))

    return Fix(True, "config-only: " + "; ".join(actions))


def _recycle_panel_pods(ctx: Ctx, selector: str) -> List[str]:
    """Force-delete pods so a stuck rollout/CrashLoop gets a clean schedule."""
    actions: List[str] = []
    r = ctx.k(["delete", "pod", "-n", _PANEL_NS, "-l", selector,
               "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
    if r.returncode == 0 and (r.stdout or r.stderr or "").strip():
        actions.append(f"force-deleted pods -l {selector}")
    return actions


def _det_otel_navigator(ctx: Ctx) -> Probe:
    """Target: otel-navigator 2/2 Ready AND S3 config non-brick AND no image drift.

    Ready-only was insufficient: field pods stayed Ready with blank S3_BUCKET
    (TCP :5006 up) while the UI/data path was dead. Probe detail always includes
    LIVE STATE so ``converge --verify`` guides manual intervention.
    """
    live = _panel_live_state(ctx)
    s3i = _panel_s3_config_issue(ctx)
    issues = _panel_workload_issues(ctx, "app=otel-navigator", expect_containers=2)
    ready, total = ctx.pods_ready(_PANEL_NS, "app=otel-navigator")
    if s3i:
        head = s3i + (f"; ready {ready}/{total}" if total else "")
        return Probe(False, f"{head}\n{live}")
    if issues:
        return Probe(False, f"{'; '.join(issues[:4])}\n{live}")
    if ready < 1:
        return Probe(False, f"otel-navigator ready {ready}/{total}\n{live}")
    return Probe(True, f"otel-navigator ready {ready}/{total} (2-container, S3 config OK)")


def _rem_otel_navigator(ctx: Ctx) -> Fix:
    """Heal panel-viz (otel-navigator + sidecar + shared CM/Secret).

    Order (prefer lightest change that unblocks):
      1. Refuse blank S3 if converge has no S3_BUCKET (would re-brick).
      2. **Config-only** when Deployments exist and the failure is S3 CM/Secret
         (patch from ctx.s3 + rollout restart — no package required).
      3. Capacity cap when Pending.
      4. ``zarf package deploy --components=cybersec-images,panel-viz`` when
         missing objects / image pull / drift / config-only insufficient.
      5. Force-delete stuck pods; co-deploy engine if absent.
    """
    actions: List[str] = []
    diag = _diagnose_panel(ctx, "otel-navigator")
    s3i = _panel_s3_config_issue(ctx)
    if s3i and not ctx.s3.get("S3_BUCKET"):
        return Fix(False, _manual.join_detail(
            f"MANUAL: panel S3 config broken and no S3_BUCKET given to converge — {s3i}\n"
            f"{_panel_live_state(ctx)}",
            _manual.hint_for("T5.otel-navigator")))

    # --- Config-only path (existing deployment + operator-supplied S3_*) -----
    # Covers: blank bucket, unrendered templates, bucket drift, missing secret keys.
    workload = _panel_workload_issues(ctx, "app=otel-navigator", expect_containers=2)
    needs_package = (
        not _panel_deployments_exist(ctx)
        or any(
            x in " ".join(workload)
            for x in ("ImagePull", "image drift", "no pods and no Deployment",
                      "namespace absent")
        )
    )
    if s3i and ctx.s3.get("S3_BUCKET") and not needs_package:
        cfg = _patch_panel_s3_config(ctx)
        actions.append(cfg.detail)
        if cfg.changed:
            # Give kubelet a moment is the reconcile loop's job; re-detect now.
            re = _det_otel_navigator(ctx)
            # Config issue cleared even if pod not Ready yet counts as progress;
            # still continue to recycle if workload issues remain.
            if _panel_s3_config_issue(ctx) is None and not workload:
                return Fix(True, " | ".join(actions))
            if _panel_s3_config_issue(ctx) is None and re.ok:
                return Fix(True, " | ".join(actions))
            if _panel_s3_config_issue(ctx) is None:
                # Config good; recycle for env pickup if still not ready
                actions.extend(_recycle_panel_pods(ctx, "app=otel-navigator"))
                re = _det_otel_navigator(ctx)
                if re.ok or _panel_s3_config_issue(ctx) is None and not any(
                        "CrashLoop" in w or "ImagePull" in w for w in workload):
                    # Config fixed — report success if only readiness is settling
                    if re.ok:
                        return Fix(True, " | ".join(actions))
                    # Config-only done; readiness may need another reconcile pass
                    return Fix(True, " | ".join(actions)
                               + f"  (config applied; pod settling: {re.detail.splitlines()[0]})")

    # Capacity: if Pending, try worker cap first (cheap) before expensive redeploy.
    pods = ctx.items("pods", ns=_PANEL_NS, selector="app=otel-navigator")
    if any((p.get("status") or {}).get("phase") == "Pending" for p in pods):
        cap_fix = _rem_workers_capacity(ctx)
        if cap_fix.changed:
            actions.append(cap_fix.detail)

    # Image drift — surgical retarget (do not recycle-only; converge-11/12)
    if any("image drift" in w for w in workload):
        actions.extend(_rem_panel_image_drift(
            ctx, "otel-navigator", "cybersec-images,panel-viz"))

    if needs_package or _panel_s3_config_issue(ctx) or not _det_otel_navigator(ctx).ok:
        if ctx.have_zarf() and ctx.package_path:
            # Skip full package if image-drift path already ran a deploy
            if not any("panel-viz" in a or "retargeted" in a for a in actions):
                fix = _zarf_deploy_components(ctx, "cybersec-images,panel-viz")
                actions.append(fix.detail)
                if any("image drift" in w for w in workload):
                    actions.extend(_retarget_deployments_cybersec(
                        ctx, _PANEL_NS, "app=otel-navigator"))
                    actions.extend(_recycle_panel_pods(ctx, "app=otel-navigator"))
        elif _panel_s3_config_issue(ctx) and ctx.s3.get("S3_BUCKET"):
            # No package — config-only is the only lever
            cfg = _patch_panel_s3_config(ctx)
            actions.append(cfg.detail)
        elif not (ctx.have_zarf() and ctx.package_path):
            return Fix(False, _manual.join_detail(
                "MANUAL: package/zarf unavailable and config-only insufficient — "
                f"{diag}\n{_panel_live_state(ctx)}",
                _manual.hint_for("T5.otel-navigator")))

    re = _det_otel_navigator(ctx)
    if not re.ok:
        if any("image drift" in w for w in _panel_workload_issues(
                ctx, "app=otel-navigator", expect_containers=2)):
            actions.extend(_rem_panel_image_drift(
                ctx, "otel-navigator", "cybersec-images,panel-viz"))
        else:
            actions.extend(_recycle_panel_pods(ctx, "app=otel-navigator"))
        if (ctx.have_zarf() and ctx.package_path
                and not any("panel-viz" in a for a in actions)):
            fix2 = _zarf_deploy_components(ctx, "panel-viz")
            actions.append(f"retry panel-viz-only: {fix2.detail}")

    # Co-deploy engine when completely absent (shared CM; terminal path).
    eng_pods = ctx.items("pods", ns=_PANEL_NS, selector="app=navigator-engine")
    if not eng_pods and ctx.s3.get("S3_BUCKET") and ctx.have_zarf() and ctx.package_path:
        eng = _zarf_deploy_components(ctx, "navigator-engine")
        actions.append(f"co-deploy navigator-engine: {eng.detail}")
    elif not eng_pods and ctx.s3.get("S3_BUCKET") and _panel_s3_config_issue(ctx) is None:
        # Config was patched; engine deploy still needs package — note it
        actions.append("navigator-engine absent — need zarf --components=navigator-engine "
                       "or re-run apply with package")

    re = _det_otel_navigator(ctx)
    detail = " | ".join(a for a in actions if a)
    if re.ok:
        return Fix(True, detail)
    return Fix(bool(actions),
               _manual.join_detail(
                   f"{detail}; still: {re.detail.splitlines()[0]}; diag=[{diag}]\n"
                   f"{_panel_live_state(ctx)}",
                   _manual.hint_for("T5.otel-navigator")))


# Engine OOM target (field: 2Gi/4Gi OOMs when binding Dask frames — converge-24/25).
_ENGINE_MEM_REQUEST = "4Gi"
_ENGINE_MEM_LIMIT = "8Gi"


def _parse_mem_to_mi(val: str) -> Optional[int]:
    """Parse K8s memory quantity to MiB (approx). None if unparseable."""
    if not val:
        return None
    s = str(val).strip()
    try:
        if s.endswith("Ki"):
            return int(float(s[:-2]) / 1024)
        if s.endswith("Mi"):
            return int(float(s[:-2]))
        if s.endswith("Gi"):
            return int(float(s[:-2]) * 1024)
        if s.endswith("Ti"):
            return int(float(s[:-2]) * 1024 * 1024)
        if s.endswith("m"):  # milli-bytes — ignore
            return None
        return int(float(s) / (1024 * 1024))
    except ValueError:
        return None


def _engine_had_oom(ctx: Ctx) -> bool:
    for p in ctx.items("pods", ns=_PANEL_NS, selector="app=navigator-engine"):
        for cs in ((p.get("status") or {}).get("containerStatuses") or []):
            term = ((cs.get("state") or {}).get("terminated") or {})
            last = ((cs.get("lastState") or {}).get("terminated") or {})
            if term.get("reason") == "OOMKilled" or last.get("reason") == "OOMKilled":
                return True
    return False


def _engine_memory_below_target(ctx: Ctx) -> Optional[str]:
    """None if deploy missing or already at/above target; else short reason."""
    dep = ctx.get("deploy", "navigator-engine", ns=_PANEL_NS) or ctx.get(
        "deployment", "navigator-engine", ns=_PANEL_NS)
    if not dep:
        return None
    containers = (
        ((dep.get("spec") or {}).get("template") or {}).get("spec") or {}
    ).get("containers") or []
    for c in containers:
        if c.get("name") != "navigator-engine":
            continue
        res = c.get("resources") or {}
        lim = ((res.get("limits") or {}).get("memory")) or ""
        req = ((res.get("requests") or {}).get("memory")) or ""
        lim_mi = _parse_mem_to_mi(lim) or 0
        req_mi = _parse_mem_to_mi(req) or 0
        want_lim = _parse_mem_to_mi(_ENGINE_MEM_LIMIT) or 8192
        want_req = _parse_mem_to_mi(_ENGINE_MEM_REQUEST) or 4096
        if lim_mi < want_lim or req_mi < want_req:
            return (f"navigator-engine memory req={req or '?'} lim={lim or '?'} "
                    f"(target {_ENGINE_MEM_REQUEST}/{_ENGINE_MEM_LIMIT})")
        return None
    return None


def _rem_engine_oom_memory(ctx: Ctx) -> Fix:
    """Surgical memory bump on navigator-engine Deployment — no zarf package."""
    if not (ctx.exists("deploy", "navigator-engine", ns=_PANEL_NS)
            or ctx.exists("deployment", "navigator-engine", ns=_PANEL_NS)):
        return Fix(False, "navigator-engine Deployment absent")
    patch = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": "navigator-engine",
                        "resources": {
                            "requests": {
                                "cpu": "500m",
                                "memory": _ENGINE_MEM_REQUEST,
                            },
                            "limits": {
                                "cpu": "2",
                                "memory": _ENGINE_MEM_LIMIT,
                            },
                        },
                    }]
                }
            }
        }
    }
    r = ctx.k([
        "patch", "deployment", "navigator-engine", "-n", _PANEL_NS,
        "--type", "strategic", "-p", json.dumps(patch),
    ])
    if r.returncode != 0:
        return Fix(False, f"patch memory failed: {(r.stderr or r.stdout or '')[:200]}")
    actions = [
        f"patched navigator-engine memory → {_ENGINE_MEM_REQUEST}/{_ENGINE_MEM_LIMIT}",
    ]
    rr = ctx.k(["rollout", "restart", "deployment/navigator-engine", "-n", _PANEL_NS])
    if rr.returncode == 0:
        actions.append("rollout restart deployment/navigator-engine")
    else:
        actions.extend(_recycle_panel_pods(ctx, "app=navigator-engine"))
    return Fix(True, "; ".join(actions))


def _det_engine(ctx: Ctx) -> Probe:
    live = _panel_live_state(ctx)
    s3i = _panel_s3_config_issue(ctx)
    issues = _panel_workload_issues(ctx, "app=navigator-engine", expect_containers=1)
    ready, total = ctx.pods_ready(_PANEL_NS, "app=navigator-engine")
    # Engine shares otel-navigator-config — blank S3 is also an engine failure mode
    # (dataset commands / S3 listing via Dask).
    if s3i and total > 0:
        return Probe(False, f"{s3i}; engine ready {ready}/{total}\n{live}")
    # OOM history with undersized limits → fail so rem bumps memory (even if Ready).
    mem = _engine_memory_below_target(ctx)
    if mem and _engine_had_oom(ctx):
        return Probe(False, f"OOM with undersized limits: {mem}\n{live}")
    if issues:
        return Probe(False, f"{'; '.join(issues[:4])}\n{live}")
    if ready < 1:
        return Probe(False, f"navigator-engine ready {ready}/{total}\n{live}")
    return Probe(True, f"navigator-engine ready {ready}/{total}")


def _rem_engine(ctx: Ctx) -> Fix:
    """Heal navigator-engine. Prefer lightest change:

    1. Shared S3 config-only patch
    2. **Surgical memory bump** on OOM (converge-24/25 — not full zarf redeploy)
    3. Image-drift retarget
    4. zarf deploy only if deploy missing / drift / above insufficient
    """
    actions: List[str] = []
    s3i = _panel_s3_config_issue(ctx)
    if s3i and not ctx.s3.get("S3_BUCKET"):
        return Fix(False, _manual.join_detail(
            f"MANUAL: shared panel S3 config broken — {s3i}\n{_panel_live_state(ctx)}",
            _manual.hint_for("T5.navigator-engine")))
    if s3i and ctx.s3.get("S3_BUCKET") and _panel_deployments_exist(ctx):
        cfg = _patch_panel_s3_config(ctx)
        actions.append(cfg.detail)
        if _panel_s3_config_issue(ctx) is None and _det_engine(ctx).ok:
            return Fix(True, " | ".join(actions))
    # OOM → raise limits before expensive package deploy
    if _engine_had_oom(ctx) and _engine_memory_below_target(ctx):
        oom = _rem_engine_oom_memory(ctx)
        actions.append(oom.detail)
        if oom.changed and _det_engine(ctx).ok:
            return Fix(True, " | ".join(actions))
    # Image drift first (cheap surgical) before full chart redeploy
    if _image_drift(ctx, _PANEL_NS, "app=navigator-engine"):
        actions.extend(_rem_panel_image_drift(
            ctx, "navigator-engine", "cybersec-images,navigator-engine"))
        if _det_engine(ctx).ok:
            return Fix(True, " | ".join(actions))
    # Deploy missing entirely
    eng_exists = (
        ctx.exists("deploy", "navigator-engine", ns=_PANEL_NS)
        or ctx.exists("deployment", "navigator-engine", ns=_PANEL_NS)
    )
    if not eng_exists and ctx.have_zarf() and ctx.package_path:
        # Images usually already present — try engine-only first (shorter timeout)
        fix = _zarf_deploy_components(ctx, "navigator-engine")
        actions.append(fix.detail)
        if not fix.changed or not _det_engine(ctx).ok:
            fix2 = _zarf_deploy_components(ctx, "cybersec-images,navigator-engine")
            actions.append(fix2.detail)
    elif not eng_exists and not actions:
        return Fix(False, _manual.join_detail(
            f"MANUAL: cannot deploy navigator-engine without package\n{_panel_live_state(ctx)}",
            _manual.hint_for("T5.navigator-engine")))
    elif not _det_engine(ctx).ok:
        # Still unhealthy: recycle; avoid full package redeploy for OOM/Ready cases
        if _engine_had_oom(ctx) and _engine_memory_below_target(ctx):
            actions.append(_rem_engine_oom_memory(ctx).detail)
        else:
            actions.extend(_recycle_panel_pods(ctx, "app=navigator-engine"))
    re = _det_engine(ctx)
    detail = " | ".join(a for a in actions if a)
    if re.ok:
        return Fix(True, detail or "navigator-engine already healthy")
    return Fix(bool(actions),
               _manual.join_detail(
                   f"{detail}; still: {re.detail.splitlines()[0]}; "
                   f"diag=[{_diagnose_panel(ctx, 'navigator-engine')}]\n"
                   f"{_panel_live_state(ctx)}",
                   _manual.hint_for("T5.navigator-engine")))


# --------------------------------------------------------------------------- #
# T5.s3-datapath — app can reach configured S3 and read generated spans
# --------------------------------------------------------------------------- #
# TCP readiness cannot see this. SSOT for the human/CI path is
# zarf/scripts/verify-s3-datapath.sh; detect shells to it when staged, else
# runs the same in-cluster probe via kubectl exec (no secrets on argv).

_S3_DATAPATH_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "verify-s3-datapath.sh"

# Compact in-pod probe (mirrors verify-s3-datapath.sh / otel-navigator loader).
# TCP precheck + short botocore timeouts — field hang was empty/wrong endpoint
# with 180s silent wait (converge-26).
_S3_DATAPATH_PY = r"""
import json, os, sys, socket, urllib.parse
bucket = (os.environ.get("S3_BUCKET") or "").strip()
akid = os.environ.get("AWS_ACCESS_KEY_ID") or ""
secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or ""
endpoint = (os.environ.get("S3_ENDPOINT") or "").strip()
region = (os.environ.get("AWS_REGION") or "us-east-1").strip()
out = {"ok": False, "bucket": bucket, "akid_len": len(akid),
       "endpoint_set": bool(endpoint),
       "endpoint_host": endpoint.split("://")[-1].split("/")[0] if endpoint else ""}
if not bucket:
    out["error"] = "S3_BUCKET empty in pod"; print(json.dumps(out)); sys.exit(1)
if not akid or not secret:
    out["error"] = "AWS keys empty in pod env"; print(json.dumps(out)); sys.exit(1)
if not endpoint:
    out["error"] = ("S3_ENDPOINT empty in pod env — Secret may have the key but "
                    "container was not restarted; rollout restart panel + engine")
    print(json.dumps(out)); sys.exit(1)
# Fail fast on network before s3fs can hang for minutes
try:
    u = urllib.parse.urlparse(endpoint)
    host = u.hostname or ""
    port = u.port or (443 if (u.scheme or "https") == "https" else 80)
    out["endpoint_host"] = host
    out["endpoint_port"] = port
    s = socket.create_connection((host, port), timeout=5)
    s.close()
    out["tcp_ok"] = True
except Exception as e:
    out["tcp_ok"] = False
    out["error"] = f"TCP to S3_ENDPOINT failed: {type(e).__name__}: {e}"
    print(json.dumps(out)); sys.exit(1)
import s3fs
# config_kwargs only — not client_kwargs["config"] (duplicate config → TypeError)
kw = {
    "key": akid, "secret": secret,
    "client_kwargs": {"endpoint_url": endpoint, "region_name": region},
    "config_kwargs": {
        "s3": {"addressing_style": "path"},
        "connect_timeout": 5, "read_timeout": 20,
        "retries": {"max_attempts": 2, "mode": "standard"},
    },
}
tok = os.environ.get("AWS_SESSION_TOKEN") or ""
if tok:
    kw["token"] = tok
socket.setdefaulttimeout(30)
fs = s3fs.S3FileSystem(**kw)
try:
    fs.ls(bucket)
except Exception as e:
    out["error"] = f"list bucket: {type(e).__name__}: {e}"; print(json.dumps(out)); sys.exit(1)
# Data root: marker dataset and/or OTEL_DATA_PATH. Parquet lives under
# {root}/spans/ with partitions (date=/hour= or shard=/date=) — never only
# at the top level of OTEL_DATA_PATH (field: s3://dhfo/otel-notebook/spans/…).
cfg = (os.environ.get("OTEL_DATA_PATH") or os.environ.get("CHECK_CFG_PATH") or "").strip()
roots = []
mk = f"{bucket}/_active_dataset.json"
if fs.exists(mk):
    with fs.open(mk, "r") as f:
        marker = json.load(f)
    ds = (marker.get("dataset") or marker.get("prefix") or "").strip().strip("/")
    if ds:
        roots.append(f"{bucket}/{ds}")
        out["dataset"] = ds
    else:
        out["marker_warning"] = "no dataset key"
else:
    out["marker_warning"] = f"missing {mk}"
if cfg.startswith("s3://"):
    root = cfg[5:].strip("/")
    if root and root != bucket and root not in roots:
        roots.append(root)
if not roots:
    out["error"] = "no data root (marker dataset and/or OTEL_DATA_PATH prefix)"
    print(json.dumps(out)); sys.exit(1)

def find_span_pq(data_root):
    spans = data_root.rstrip("/") + "/spans"
    for pat in (
        spans + "/shard=*/date=*/batch_*.parquet",
        spans + "/shard=*/date=*/*.parquet",
        spans + "/date=*/hour=*/*.parquet",
        spans + "/date=*/*.parquet",
    ):
        try:
            hits = [h for h in (fs.glob(pat) or []) if str(h).endswith(".parquet")]
        except Exception:
            hits = []
        if hits:
            return hits, spans, "glob:" + pat
    try:
        if fs.exists(spans):
            hits = [e for e in fs.find(spans) if str(e).endswith(".parquet")]
            if hits:
                return hits, spans, "find:" + spans
    except Exception:
        pass
    return [], spans, None

files, spans_path, method = [], None, None
for root in roots:
    hits, sp, method = find_span_pq(root)
    if hits:
        files, spans_path = hits, sp
        out["dataset_path"] = "s3://" + root.rstrip("/") + "/"
        out["spans_path"] = "s3://" + sp + "/"
        out["glob_pattern"] = method
        if "dataset" not in out:
            out["dataset"] = root[len(bucket):].lstrip("/") if root.startswith(bucket) else root
        break
out["parquet_count"] = len(files)
if not files:
    out["error"] = (
        f"no parquet under s3://{roots[0]}/spans/ "
        f"(need spans/date=*/hour=*/*.parquet or spans/shard=*/date=*/*.parquet; "
        f"top-level of OTEL_DATA_PATH is not enough)"
    )
    print(json.dumps(out)); sys.exit(1)
with fs.open(files[0], "rb") as f:
    f.read(64)
out["ok"] = True
out["sample"] = files[0]
print(json.dumps(out))
"""

# Fast pod env + TCP only (LIVE STATE / pre-s3fs gate). No secrets printed.
_S3_POD_ENV_PY = r"""
import json, os, socket, urllib.parse
ep = (os.environ.get("S3_ENDPOINT") or "").strip()
out = {
  "bucket": (os.environ.get("S3_BUCKET") or "").strip(),
  "endpoint_set": bool(ep),
  "endpoint_host": ep.split("://")[-1].split("/")[0] if ep else "",
  "endpoint_len": len(ep),
  "akid_len": len(os.environ.get("AWS_ACCESS_KEY_ID") or ""),
  "secret_len": len(os.environ.get("AWS_SECRET_ACCESS_KEY") or ""),
  "tcp_ok": None, "error": "",
}
if not ep:
    out["error"] = "S3_ENDPOINT empty in pod"
    print(json.dumps(out)); raise SystemExit(0)
try:
    u = urllib.parse.urlparse(ep)
    host = u.hostname or out["endpoint_host"]
    port = u.port or (443 if (u.scheme or "https") == "https" else 80)
    out["endpoint_host"] = host
    s = socket.create_connection((host, port), timeout=5)
    s.close()
    out["tcp_ok"] = True
except Exception as e:
    out["tcp_ok"] = False
    out["error"] = f"{type(e).__name__}: {e}"
print(json.dumps(out))
"""


def _parse_s3_datapath_json(raw: str) -> dict:
    """Extract probe JSON from script/kubectl stdout (tolerate warnings / pretty print).

    Field failure mode: host-side ``json.loads`` on empty or mixed output made
    T5.s3-datapath FAIL with a JSONDecodeError even when S3 was fine (script
    bug: kubectl exec without -i → empty RESULT).
    """
    text = (raw or "").strip()
    if not text:
        return {}
    # Prefer last complete JSON object line (script prints one; json.tool may
    # expand to multi-line — then fall through to full-text parse).
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                data = json.loads(line)
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                continue
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def _det_s3_datapath_inline(ctx: Ctx) -> Probe:
    """kubectl exec python3 -c probe (no bash script / no stdin heredoc)."""
    target = None  # (ns, resource, extra_args)
    if ctx.pods_ready(_PANEL_NS, "app=otel-navigator")[0] >= 1:
        target = (_PANEL_NS, "deploy/otel-navigator", ["-c", "otel-navigator"])
    elif ctx.pods_ready("dask", "dask.org/component=scheduler")[0] >= 1:
        target = ("dask", "deploy/cybersec-dask-scheduler", [])
    if not target:
        return Probe(False,
                     f"no Ready otel-navigator or dask scheduler to probe S3\n"
                     f"{_panel_live_state(ctx)}")
    ns, res, extra = target
    # 75s wall: TCP 5s + s3fs with botocore 20s reads — never wait 180s silent
    r = ctx.run(
        ctx.kubectl + ["exec", "-n", ns, res] + extra + ["--", "python3", "-c", _S3_DATAPATH_PY],
        timeout=75,
    )
    raw = (r.stdout or "").strip()
    data = _parse_s3_datapath_json(raw)
    if r.returncode == 0 and data.get("ok"):
        return Probe(
            True,
            f"S3 OK bucket={data.get('bucket')} dataset={data.get('dataset')} "
            f"parquet={data.get('parquet_count')} host={data.get('endpoint_host')}",
        )
    err = data.get("error") or (r.stderr or raw or f"rc={r.returncode}")[-300:]
    if r.returncode == 124 or (isinstance(err, str) and err.strip() == "timeout"):
        err = ("probe timed out after TCP/s3fs budgets — S3_ENDPOINT host unreachable "
               "or list hang; see pod S3 env in LIVE STATE")
    return Probe(False, f"S3 datapath: {err}\n{_panel_live_state(ctx)}")


def _det_s3_datapath(ctx: Ctx) -> Probe:
    """Configured bucket reachable from the app pod; marker + span parquet readable.

    Data is operator-provided (or notebook-generated) — engine never fetches.
    Failures are MANUAL (fix endpoint/creds, or seed spans under the marker path).

    Prefer staged verify-s3-datapath.sh; on empty/non-JSON or script timeout,
    fall back to inline kubectl exec so a script plumbing bug cannot brick
    converge when the datapath is actually healthy.
    """
    s3i = _panel_s3_config_issue(ctx)
    if s3i:
        return Probe(False, s3i)

    # Fast gate: pod env + TCP before multi-minute s3fs (converge-26 hang).
    pod_env = _pod_s3_env_census(ctx)
    if pod_env.get("error") and pod_env.get("endpoint_set") is False:
        return Probe(
            False,
            f"S3 datapath: {pod_env.get('error')} — Secret may be set but not in "
            f"container env; config-only rem + rollout restart\n{_panel_live_state(ctx)}",
        )
    if pod_env.get("tcp_ok") is False:
        return Probe(
            False,
            f"S3 datapath: pod cannot reach S3_ENDPOINT host="
            f"{pod_env.get('endpoint_host')!r} ({pod_env.get('error')}) — "
            f"fix network/URL from cluster, not creds-file quoting\n"
            f"{_panel_live_state(ctx)}",
        )

    # Prefer the staged script (full diagnosis, human + --json).
    if _S3_DATAPATH_SCRIPT.is_file():
        import subprocess
        env = {**os.environ}
        try:
            r = subprocess.run(
                ["bash", str(_S3_DATAPATH_SCRIPT), "--quiet", "--json"],
                capture_output=True, text=True, timeout=90, env=env,
            )
        except subprocess.TimeoutExpired:
            # Prefer inline (has TCP + botocore budgets) over bare timeout message
            fb = _det_s3_datapath_inline(ctx)
            if fb.ok:
                return Probe(True, f"{fb.detail} (inline after script timeout)")
            return Probe(
                False,
                f"S3 datapath: script timed out 90s; inline: "
                f"{fb.detail.split(chr(10))[0]}\n{_panel_live_state(ctx)}",
            )
        raw = (r.stdout or "").strip() or (r.stderr or "").strip()
        data = _parse_s3_datapath_json(raw)
        if r.returncode == 0 and data.get("ok"):
            m = data.get("marker") or {}
            return Probe(
                True,
                f"S3 OK bucket={data.get('bucket_configmap')} "
                f"dataset={m.get('dataset')} parquet={data.get('parquet_count')}",
            )
        # Empty / non-JSON / stdin-not-delivered: fall back to inline -c probe
        # so converge is not blocked by script plumbing (field: JSONDecodeError).
        plumbing = (
            not data
            or "empty probe output" in (data.get("error") or "")
            or "non-json probe output" in (data.get("error") or "")
            or "JSONDecodeError" in raw
            or "Expecting value" in raw
        )
        if plumbing:
            fb = _det_s3_datapath_inline(ctx)
            if fb.ok:
                return Probe(True, f"{fb.detail} (inline fallback; script output unusable)")
            script_err = data.get("error") or (raw[-200:] if raw else f"rc={r.returncode}")
            return Probe(
                False,
                f"S3 datapath: {fb.detail.split(chr(10))[0]} "
                f"[script also failed: {script_err[:160]}]\n"
                f"{_panel_live_state(ctx)}",
            )
        err = data.get("error") or raw[-300:] or f"verify-s3-datapath rc={r.returncode}"
        return Probe(False, f"S3 datapath: {err}\n{_panel_live_state(ctx)}")

    return _det_s3_datapath_inline(ctx)


# JupyterHub control-plane pods (hub + proxy). Singleuser servers are user-triggered.
_JH_NS = "jupyterhub"
_JH_SELECTORS = ("component=hub", "component=proxy")


def _jupyterhub_workload_census(ctx: Ctx) -> dict:
    """Census hub/proxy pods + node schedulability — informs rem without re-deploy.

    Returns keys:
      hub_ready, hub_total, proxy_ready, proxy_total,
      pending (list of {name, sel, phase, msgs}),
      deploys (list of deploy names present),
      node_schedulable (bool), node_blockers (list[str]),
      disk_pressure (bool), summary (str one-liner for logs)
    """
    hub_r, hub_t = ctx.pods_ready(_JH_NS, "component=hub")
    proxy_r, proxy_t = ctx.pods_ready(_JH_NS, "component=proxy")
    pending: List[dict] = []
    for sel in _JH_SELECTORS:
        for p in ctx.items("pods", ns=_JH_NS, selector=sel):
            phase = (p.get("status") or {}).get("phase") or "?"
            name = (p.get("metadata") or {}).get("name") or "?"
            if phase == "Pending" or (
                phase == "Running"
                and not any(
                    c.get("type") == "Ready" and c.get("status") == "True"
                    for c in (p.get("status") or {}).get("conditions") or []
                )
            ):
                # Include not-Ready Running only when never scheduled (no nodeName)
                node = (p.get("spec") or {}).get("nodeName")
                if phase == "Pending" or not node:
                    pending.append({
                        "name": name,
                        "sel": sel,
                        "phase": phase,
                        "msgs": _pod_failed_scheduling_msgs(p),
                    })
    deploys = []
    for d in ctx.items("deployments", ns=_JH_NS) or []:
        n = (d.get("metadata") or {}).get("name")
        if n:
            deploys.append(n)
    # Also try apps/v1 via kind deploy
    if not deploys:
        for d in ctx.items("deploy", ns=_JH_NS) or []:
            n = (d.get("metadata") or {}).get("name")
            if n:
                deploys.append(n)

    node_blockers: List[str] = []
    node_schedulable = True
    for n in ctx.items("nodes"):
        name = (n.get("metadata") or {}).get("name") or "?"
        conds = {c.get("type"): c for c in (n.get("status") or {}).get("conditions") or []}
        if str((conds.get("Ready") or {}).get("status", "")).lower() != "true":
            node_blockers.append(f"{name}: NotReady")
            node_schedulable = False
        if str((conds.get("DiskPressure") or {}).get("status", "")).lower() == "true":
            node_blockers.append(f"{name}: DiskPressure=True")
            node_schedulable = False
        if str((conds.get("MemoryPressure") or {}).get("status", "")).lower() == "true":
            node_blockers.append(f"{name}: MemoryPressure=True")
            node_schedulable = False
        if n.get("spec", {}).get("unschedulable"):
            node_blockers.append(f"{name}: cordoned")
            node_schedulable = False
        for t in n.get("spec", {}).get("taints", []) or []:
            key = t.get("key") or ""
            effect = t.get("effect") or ""
            if effect in ("NoSchedule", "NoExecute") and "control-plane" not in key:
                # control-plane tolerations usually present on system pods; hub may not
                node_blockers.append(f"{name}: taint {key}:{effect}")
                if "disk-pressure" in key or "memory-pressure" in key or "unschedulable" in key:
                    node_schedulable = False

    # Scheduling-message census (Insufficient cpu/memory, taints, etc.)
    sched_hints: List[str] = []
    for p in pending:
        for m in p.get("msgs") or []:
            if m not in sched_hints:
                sched_hints.append(m)

    parts = [
        f"hub {hub_r}/{hub_t} Ready",
        f"proxy {proxy_r}/{proxy_t} Ready",
        f"pending={len(pending)}",
        f"deploys={deploys or 'none'}",
        f"node_schedulable={node_schedulable}",
    ]
    if node_blockers:
        parts.append("blockers=[" + "; ".join(node_blockers[:4]) + "]")
    if sched_hints:
        parts.append("sched=[" + " | ".join(sched_hints[:3]) + "]")
    summary = "  ".join(parts)

    return {
        "hub_ready": hub_r,
        "hub_total": hub_t,
        "proxy_ready": proxy_r,
        "proxy_total": proxy_t,
        "pending": pending,
        "deploys": deploys,
        "node_schedulable": node_schedulable,
        "node_blockers": node_blockers,
        "disk_pressure": any("DiskPressure" in b for b in node_blockers),
        "sched_hints": sched_hints,
        "summary": summary,
    }


def _recycle_jupyterhub_pending(ctx: Ctx) -> List[str]:
    """Force-delete Pending (or unscheduled) hub/proxy pods so the scheduler retries."""
    actions: List[str] = []
    for sel in _JH_SELECTORS:
        r = ctx.k([
            "delete", "pod", "-n", _JH_NS, "-l", sel,
            "--field-selector=status.phase=Pending",
            "--force", "--grace-period=0", "--wait=false", "--ignore-not-found",
        ])
        if r.returncode == 0 and (Ctx.out_text(r.stdout) or Ctx.out_text(r.stderr)).strip():
            actions.append(f"recycled Pending pods -l {sel}")
        # Also pods with no nodeName stuck Running/unknown
        for p in ctx.items("pods", ns=_JH_NS, selector=sel):
            if (p.get("spec") or {}).get("nodeName"):
                continue
            name = (p.get("metadata") or {}).get("name")
            phase = (p.get("status") or {}).get("phase")
            if name and phase == "Pending":
                # already covered by field-selector delete; belt
                continue
            if name and not (p.get("spec") or {}).get("nodeName"):
                r2 = ctx.k([
                    "delete", "pod", "-n", _JH_NS, name,
                    "--force", "--grace-period=0", "--wait=false", "--ignore-not-found",
                ])
                if r2.returncode == 0:
                    actions.append(f"recycled unscheduled pod {name}")
    return actions


def _det_jupyterhub(ctx: Ctx) -> Probe:
    """Hub Ready + proxy present when deployed; PVC vestiges and Pending census.

    Package uses sqlite-memory + singleuser storage none (no PVC). Any jupyterhub
    PVC is a vestige. Pending hub/proxy under DiskPressure is a scheduling problem
    — not fixed by another zarf package deploy.
    """
    if not ctx.exists("namespace", _JH_NS):
        return Probe(False, "jupyterhub namespace absent — need package deploy")

    census = _jupyterhub_workload_census(ctx)
    pvcs = ctx.items("pvc", ns=_JH_NS)
    if pvcs:
        names = [p.get("metadata", {}).get("name") for p in pvcs]
        phases = [p.get("status", {}).get("phase") for p in pvcs]
        return Probe(
            False,
            f"jupyterhub unexpected PVC(s) {names} phases={phases} — "
            f"resilient package is sqlite-memory/storage none; "
            f"census: {census['summary']}",
        )

    hub_ok = census["hub_ready"] >= 1
    # Proxy is required when deploys exist; if only hub chart partial, still fail
    proxy_ok = census["proxy_ready"] >= 1 or (
        census["proxy_total"] == 0 and not any("proxy" in d for d in census["deploys"])
    )
    if hub_ok and proxy_ok and not census["pending"]:
        return Probe(
            True,
            f"jupyterhub OK — {census['summary']} (no PVC — resilient)",
        )

    # Structured failure: prefer scheduling diagnosis over bare ready counts
    if census["pending"] and not census["node_schedulable"]:
        return Probe(
            False,
            f"jupyterhub hub/proxy unscheduled — node not schedulable; "
            f"do NOT redeploy until clear. census: {census['summary']}",
        )
    if census["pending"] and census["node_schedulable"]:
        return Probe(
            False,
            f"jupyterhub hub/proxy Pending but node schedulable — recycle pods. "
            f"census: {census['summary']}",
        )
    if census["hub_total"] == 0 and not census["deploys"]:
        return Probe(False, f"jupyterhub hub absent (no pods/deploys). census: {census['summary']}")
    return Probe(
        False,
        f"jupyterhub not Ready — census: {census['summary']}",
    )


def _rem_jupyterhub(ctx: Ctx) -> Fix:
    """Resolve hub/proxy via census-driven procedure (not always zarf deploy).

    Order (cheapest first):
      1. Namespace/deploy missing → zarf package deploy jupyterhub,sample-notebooks
      2. PVC vestiges → delete PVC/PV, recycle pods
      3. Pending + node NOT schedulable (DiskPressure/taint/cordon) → MANUAL
         (refuse expensive redeploy that worsens imagefs)
      4. Pending + node schedulable → recycle Pending hub/proxy pods only
      5. Still not Ready / missing deploy → zarf package deploy
      6. Oversubscription hints in FailedScheduling → worker capacity rem hint
    """
    actions: List[str] = []
    ns_obj = ctx.get("namespace", _JH_NS)

    # --- 1/2 PVC vestiges (always safe) ------------------------------------
    for pvc in list(ctx.items("pvc", ns=_JH_NS)):
        name = pvc.get("metadata", {}).get("name", "")
        phase = pvc.get("status", {}).get("phase")
        sc = (pvc.get("spec", {}) or {}).get("storageClassName")
        sc = "" if sc is None else sc
        if name and _force_delete_pvc(ctx, _JH_NS, name):
            actions.append(f"deleted jupyterhub PVC {name} (phase={phase} sc={sc!r})")
    for pv in list(ctx.items("pv")):
        pname = (pv.get("metadata") or {}).get("name", "")
        claim = (pv.get("spec") or {}).get("claimRef") or {}
        if claim.get("namespace") == _JH_NS or "hub-db" in pname:
            phase = pv.get("status", {}).get("phase")
            if ctx.k(["delete", "pv", pname, "--ignore-not-found"]).returncode == 0:
                actions.append(f"deleted hub-related PV {pname} (phase={phase})")

    census = _jupyterhub_workload_census(ctx)

    # --- 3 Pending + node blocked → resolve DiskPressure via FSM when df OK
    if census["pending"] and not census["node_schedulable"]:
        if census.get("disk_pressure") or _disk_pressure_issues(ctx):
            cleared = _platform.rem_resolve_disk_pressure(ctx)
            actions.append(cleared.detail)
            if cleared.changed:
                census = _jupyterhub_workload_census(ctx)
            elif _disk_pressure_issues(ctx):
                detail = cleared.detail
                if actions:
                    detail += f"  [unwound: {'; '.join(actions)}]"
                return Fix(False, detail)
        if not census["node_schedulable"] and not _node_schedulability_census(ctx)["schedulable"]:
            detail = (
                f"hub/proxy Pending while node not schedulable — "
                f"{census['summary']}. Refusing zarf redeploy."
            )
            if actions:
                detail += f"  [unwound: {'; '.join(actions)}]"
            return Fix(bool(actions), _manual.join_detail(
                detail, _manual.hint_for("T5.jupyterhub")))
        # Pressure cleared — fall through to recycle path

    # --- 4 Pending + schedulable → recycle only ----------------------------
    if census["pending"] and census["node_schedulable"] and (
            census["deploys"] or census["hub_total"] or census["proxy_total"]):
        recycled = _recycle_jupyterhub_pending(ctx)
        actions.extend(recycled)
        # brief re-census
        again = _jupyterhub_workload_census(ctx)
        if again["hub_ready"] >= 1 and (
                again["proxy_ready"] >= 1 or again["proxy_total"] == 0):
            return Fix(True, f"recycled hub/proxy after schedule restored — "
                             f"{again['summary']}"
                             + (f"  [unwound: {'; '.join(actions)}]" if actions else ""))
        # If still pending but schedulable, one more recycle of all hub/proxy
        # (covers controllers that recreate with stuck state)
        if recycled:
            for sel in _JH_SELECTORS:
                ctx.k(["delete", "pod", "-n", _JH_NS, "-l", sel,
                       "--force", "--grace-period=0", "--wait=false",
                       "--ignore-not-found"])
            actions.append("force-recycled all hub/proxy pods for reschedule")
            again = _jupyterhub_workload_census(ctx)
            if again["hub_ready"] >= 1:
                return Fix(True, f"rescheduled jupyterhub — {again['summary']}  "
                                 f"[unwound: {'; '.join(actions)}]")
        # Deploy exists — wait next reconcile pass rather than immediate zarf redeploy
        if census["deploys"] or again.get("deploys"):
            detail = (
                f"recycled pods; hub still not Ready — census: {again['summary']} "
                f"(wait next pass / check resources)"
            )
            if actions:
                detail += f"  [unwound: {'; '.join(actions)}]"
            return Fix(bool(actions), detail)

    # --- 5 Missing ns / deploys / pods → package deploy --------------------
    if not ns_obj or (census["hub_total"] == 0 and not census["deploys"]):
        if not ns_obj:
            actions.append("jupyterhub namespace absent — deploy will create it")
        fix = _zarf_deploy_components(ctx, "jupyterhub,sample-notebooks")
        if actions:
            return Fix(fix.changed or bool(actions),
                       f"{fix.detail}  [unwound: {'; '.join(actions)}]")
        return fix

    # Deploy exists but never Ready and not a pure Pending-sched case → redeploy
    # (ImagePull, CrashLoop, missing replica). Still refuse under disk pressure.
    if _disk_pressure_issues(ctx):
        return Fix(bool(actions), _manual.join_detail(
            f"MANUAL: jupyterhub deploys present but not Ready under disk pressure — "
            f"{census['summary']}",
            _manual.hint_for("T5.jupyterhub"),
        ))

    # Worker oversubscription often coexists — try capacity rem first (cheap)
    hints = " ".join(census.get("sched_hints") or []).lower()
    if any(x in hints for x in ("insufficient", "memory", "cpu", "too many pods")):
        cap = _rem_workers_capacity(ctx)
        if cap.changed:
            actions.append(cap.detail)
            actions.extend(_recycle_jupyterhub_pending(ctx))
            again = _jupyterhub_workload_census(ctx)
            if again["hub_ready"] >= 1:
                return Fix(True, f"worker cap + recycle → hub Ready — {again['summary']}  "
                                 f"[unwound: {'; '.join(actions)}]")

    fix = _zarf_deploy_components(ctx, "jupyterhub,sample-notebooks")
    if actions:
        return Fix(fix.changed or bool(actions),
                   f"{fix.detail}  [unwound: {'; '.join(actions)}]")
    return fix


# Must match zarf/scripts/embed-notebooks.py INCLUDE_NOTEBOOKS + sidecars.
# Presence-only detect masked stale CMs (pre-HDF5) so T5.sample-notebooks was
# green while JupyterLab only showed OTEL/Dask notebooks (converge-27).
# JupyterHub singleuser seeds /root/*.ipynb + generate_hdf5.py + cluster_env.py
# from this ConfigMap at server start (jupyterhub-values.yaml cmd).
_SAMPLE_NOTEBOOK_KEYS = (
    "OTEL_Data_Generator.ipynb",
    "Dask_S3_Validation.ipynb",
    "Dask_S3_Workers_OneCell.ipynb",
    "HDF5_CPHY_Acquisition_Generator.ipynb",
    "HDF5_Iceberg_Metadata_Provider.ipynb",
)
# HDF5 notebooks need these on the CM mount (copied to /root at singleuser start).
_SAMPLE_NOTEBOOK_SIDECARS = (
    "generate_hdf5.py",
    "cluster_env.py",
)


def _sample_notebooks_keys(ctx: Ctx) -> List[str]:
    cm = ctx.get("configmap", "sample-notebooks", ns="jupyterhub")
    if not cm:
        return []
    data = cm.get("data") or {}
    return sorted(data.keys())


def _det_sample_notebooks(ctx: Ctx) -> Probe:
    if not ctx.exists("configmap", "sample-notebooks", ns="jupyterhub"):
        return Probe(False, "sample-notebooks ConfigMap absent")
    keys = set(_sample_notebooks_keys(ctx))
    nb_keys = {k for k in keys if k.endswith(".ipynb")}
    missing_nb = [k for k in _SAMPLE_NOTEBOOK_KEYS if k not in nb_keys]
    missing_side = [k for k in _SAMPLE_NOTEBOOK_SIDECARS if k not in keys]
    if missing_nb or missing_side:
        parts = []
        if missing_nb:
            parts.append(f"notebooks {missing_nb}")
        if missing_side:
            parts.append(f"sidecars {missing_side} (HDF5 needs generate_hdf5.py + cluster_env.py)")
        return Probe(
            False,
            f"sample-notebooks ConfigMap missing {' and '.join(parts)} "
            f"(have {sorted(keys)}) — re-embed + package create, then "
            f"`zarf package deploy --components=sample-notebooks` (or full converge); "
            f"stop/start Jupyter singleuser so /root seeds refresh",
        )
    hdf5 = [k for k in _SAMPLE_NOTEBOOK_KEYS if k.startswith("HDF5_")]
    return Probe(
        True,
        f"sample-notebooks ConfigMap OK ({len(nb_keys)} notebooks incl. "
        f"{len(hdf5)} HDF5 + sidecars) — singleuser start copies to /root",
    )


def _rem_sample_notebooks(ctx: Ctx) -> Fix:
    """Redeploy CM; recycle hub user pods so mounts/startup re-seed home copies."""
    fix = _zarf_deploy_components(ctx, "sample-notebooks")
    actions = [fix.detail]
    # Singleuser pods mount the CM; without restart they keep the old volume
    # snapshot and /root/*.ipynb from the previous startup copy.
    r = ctx.k([
        "delete", "pod", "-n", "jupyterhub",
        "-l", "component=singleuser-server",
        "--ignore-not-found", "--wait=false",
    ])
    if r.returncode == 0:
        actions.append(
            "deleted jupyterhub singleuser pods "
            "(re-login / Start My Server to re-seed /root incl. HDF5 + generate_hdf5.py)"
        )
    # Re-detect content
    if _det_sample_notebooks(ctx).ok:
        return Fix(True, " | ".join(actions))
    return Fix(fix.changed, " | ".join(actions) + f"; still: {_det_sample_notebooks(ctx).detail}")


def _det_ingress(ctx: Ctx) -> Probe:
    ings = ctx.items("ingress")
    names = [i["metadata"]["name"] for i in ings]
    want = {"dask-dashboard", "panel-viz"}
    if not want.issubset(set(names)):
        return Probe(False, f"ingress missing objects: have {names}, want {want}")
    # Class alignment (traefik-on-RKE2 silent unbound)
    cls_probe = _platform.det_ingress_class_aligned(ctx)
    if not cls_probe.ok:
        return cls_probe
    # Optional: /ws path on panel-viz for terminal
    for ing in ings:
        if (ing.get("metadata") or {}).get("name") != "panel-viz":
            continue
        paths = []
        for rule in (ing.get("spec") or {}).get("rules") or []:
            for p in ((rule.get("http") or {}).get("paths") or []):
                paths.append(p.get("path"))
        if "/ws" not in paths:
            return Probe(False,
                         f"panel-viz ingress missing /ws path (have {paths}) — "
                         "terminal WebSocket will not work through ingress")
    return Probe(True, f"ingress: {names}; {cls_probe.detail}")


def _rem_ingress(ctx: Ctx) -> Fix:
    _platform.ensure_ingress_class_in_ctx(ctx)
    return _zarf_deploy_components(ctx, "ingress")

# APP_NAMESPACES / DASK_CRD_KINDS: imported from discovery (managed-scope SSOT).
# Teardown and remediations mutate only those Layer-B targets; registry hostPath
# data and Layer-A node images are never deleted.


# --------------------------------------------------------------------------- #
# The catalog
# --------------------------------------------------------------------------- #

def build_catalog(dynamic_provisioning: bool = False,
                  registry_pvc_enabled: bool = True) -> List[Invariant]:
    """The invariant catalog for the active storage modality.

    DEFAULT — RESILIENT AIR-GAP (the only modality public releases target): the single
    disk-limited node is the baseline. The Zarf registry binds a claimRef hostPath PV
    (``T0.5.registry-pv``), so NO default StorageClass, NO local-path-provisioner and
    NO bootstrap images (local-path-provisioner/busybox) are needed — the SC↔registry
    chicken-egg simply doesn't exist. Worker count is sized to capacity, but behavior
    never forks on node count.

    ``dynamic_provisioning=True`` — OPT-IN, for a resourced multi-node cluster running
    workloads that need dynamic PVCs: restores the default-StorageClass tier (the
    node-preloaded local-path-provisioner + its Layer-A bootstrap-image CLOSURE check)
    and routes the registry PVC through it.

    The T1→T6 tier (init → registry → images → components) is identical across
    modalities; only the T0.5 storage tier and ``T1.registry-running``'s dependency
    differ.
    """
    # manual_hint SSOT: zarf/converge/manual.py HINTS (DISCOVER + FIX, copy-paste in situ).
    H = _manual.hint_for

    inv: List[Invariant] = [
        Invariant("T0.api", "T0", "Kubernetes API reachable", Layer.B, _det_api,
                  manual_hint=H("T0.api")),
        Invariant("T0.node-ready", "T0", "Nodes Ready and schedulable", Layer.B,
                  _det_node_ready, _rem_node_ready, depends_on=("T0.api",),
                  manual_hint=H("T0.node-ready")),
        Invariant("T0.system-plane", "T0",
                  "RKE2/system plane healthy (API, DNS, ingress controller observed)",
                  Layer.B, _platform.det_system_plane, _platform.rem_system_plane,
                  depends_on=("T0.api",), manual_hint=H("T0.system-plane")),
        Invariant("T0.kubelet-gc", "T0",
                  "Kubelet image-GC policy raised (protects Layer-A images on disk pressure)",
                  Layer.B, _platform.det_kubelet_gc_policy, _platform.rem_kubelet_gc_policy,
                  depends_on=("T0.api",), manual_hint=H("T0.kubelet-gc")),
        Invariant("T0.no-disk-pressure", "T0",
                  "No DiskPressure (df vs configured hard GiB; clear taint + wait when free OK)",
                  Layer.B, _det_no_disk_pressure, _rem_no_disk_pressure,
                  depends_on=("T0.api", "T0.kubelet-gc"),
                  manual_hint=H("T0.no-disk-pressure")),
        # Layer-A tools: binary + init package must be on the node (rc=127 / air-gap init).
        Invariant("T0.layer-a-zarf-tools", "T0",
                  "Zarf binary + init package present (Layer A — CLOSURE)",
                  Layer.A, _det_layer_a_zarf_tools, depends_on=("T0.api",),
                  manual_hint=H("T0.layer-a-zarf-tools")),
        Invariant("T0.package-uniqueness", "T0",
                  "At most one cybersec-dask deploy package staged (mtime-safe)",
                  Layer.A, _platform.det_package_uniqueness, depends_on=("T0.api",),
                  manual_hint=H("T0.package-uniqueness")),
    ]

    # T0.5 — storage. Resilient (default): the registry binds a claimRef hostPath PV,
    # no StorageClass. Dynamic (opt-in): the node-preloaded local-path-provisioner
    # supplies a default StorageClass and the registry PVC binds through it.
    # Disk pressure gates long zarf deploys (T1+). T0.kubelet-gc stays independent so
    # it can still raise GC thresholds while the operator frees space.
    _disk = ("T0.no-disk-pressure",)

    if dynamic_provisioning:
        inv += [
            Invariant("T0.layer-a-images", "T0", "Bootstrap images present (CLOSURE)", Layer.A,
                      _det_layer_a_images, depends_on=("T0.api",),
                      manual_hint=H("T0.layer-a-images")),
            Invariant("T0.5.sc-default", "T0.5", "Default StorageClass exists", Layer.B,
                      _det_sc_default, _rem_sc_default,
                      depends_on=("T0.node-ready", "T0.layer-a-images") + _disk,
                      manual_hint=H("T0.5.sc-default")),
            Invariant("T0.5.provisioner", "T0.5", "local-path-provisioner Running", Layer.B,
                      _det_provisioner, _rem_provisioner, depends_on=("T0.5.sc-default",),
                      manual_hint=H("T0.5.provisioner")),
        ]
        registry_dep = ("T0.5.sc-default",) + _disk
    elif registry_pvc_enabled:
        inv += [
            Invariant("T0.5.registry-pv", "T0.5",
                      "Registry storage prebound (claimRef hostPath PV — no default SC needed)",
                      Layer.B, _det_registry_pv, _rem_registry_pv,
                      depends_on=("T0.node-ready",) + _disk,
                      manual_hint=H("T0.5.registry-pv")),
        ]
        registry_dep = ("T0.5.registry-pv",) + _disk
    else:
        # --no-registry-pvc: the registry runs on emptyDir — nothing to prebind.
        registry_dep = ("T0.node-ready",) + _disk

    inv += [
        Invariant("T1.registry-running", "T1", "Zarf internal registry initialized + Running",
                  Layer.B, _det_registry_running, _rem_registry_running, cost=Cost.EXPENSIVE,
                  depends_on=registry_dep, manual_hint=H("T1.registry-running")),

        Invariant("T2.images-pushed", "T2", "App images pushed to internal registry",
                  Layer.B, _det_images_pushed, _rem_images_pushed, cost=Cost.EXPENSIVE,
                  depends_on=("T1.registry-running",) + _disk,
                  manual_hint=H("T2.images-pushed")),

        Invariant("T3.dask-operator", "T3", "Dask operator + CRDs", Layer.B,
                  _det_operator, _rem_operator, cost=Cost.EXPENSIVE,
                  depends_on=("T2.images-pushed",) + _disk,
                  manual_hint=H("T3.dask-operator")),

        Invariant("T4.scheduler", "T4", "Dask scheduler Ready", Layer.B,
                  _det_scheduler, _rem_scheduler, cost=Cost.EXPENSIVE,
                  depends_on=("T3.dask-operator",) + _disk,
                  manual_hint=H("T4.scheduler")),
        Invariant("T4.workers-capacity", "T4",
                  "Workers sized (replicas/nthreads/cpu/memory) and fit capacity",
                  Layer.B, _det_workers_capacity, _rem_workers_capacity,
                  depends_on=("T4.scheduler",), manual_hint=H("T4.workers-capacity")),

        Invariant("T5.otel-navigator", "T5", "otel-navigator Ready (2/2, fits memory)", Layer.B,
                  _det_otel_navigator, _rem_otel_navigator,
                  depends_on=("T4.scheduler", "T4.workers-capacity") + _disk,
                  manual_hint=H("T5.otel-navigator")),
        Invariant("T5.navigator-engine", "T5", "navigator-engine Ready", Layer.B,
                  _det_engine, _rem_engine, depends_on=("T4.scheduler",) + _disk,
                  manual_hint=H("T5.navigator-engine")),
        Invariant("T5.jupyterhub", "T5", "JupyterHub hub Ready", Layer.B,
                  _det_jupyterhub, _rem_jupyterhub, cost=Cost.EXPENSIVE,
                  depends_on=("T2.images-pushed",) + _disk,
                  manual_hint=H("T5.jupyterhub")),
        Invariant("T5.sample-notebooks", "T5", "Sample-notebooks ConfigMap present", Layer.B,
                  _det_sample_notebooks, _rem_sample_notebooks, cost=Cost.EXPENSIVE,
                  depends_on=("T2.images-pushed",) + _disk,
                  manual_hint=H("T5.sample-notebooks")),
        # Data path: Ready pods ≠ app can load spans. Layer-B detect-only — seed data
        # / fix creds is operator action (script: zarf/scripts/verify-s3-datapath.sh).
        Invariant("T5.s3-datapath", "T5",
                  "Configured S3 reachable; active-dataset marker + span parquet readable",
                  Layer.B, _det_s3_datapath, depends_on=("T5.otel-navigator", "T4.scheduler"),
                  manual_hint=H("T5.s3-datapath")),

        Invariant("T6.ingress", "T6", "Ingress resources present", Layer.B,
                  _det_ingress, _rem_ingress, depends_on=("T5.otel-navigator",),
                  manual_hint=H("T6.ingress")),
    ]
    return inv


# The RESILIENT air-gap modality is the DEFAULT (and the only one public releases
# target). This module-level catalog is what importers (by_id/layer_a_ids/tests) see;
# __main__ rebuilds per-run with the active flags (dynamic_provisioning / pvc).
CATALOG: List[Invariant] = build_catalog()


def by_id() -> dict:
    return {inv.id: inv for inv in CATALOG}


def layer_a_ids() -> list:
    return [inv.id for inv in CATALOG if inv.layer is Layer.A]
