"""Cluster-wide discovery and Layer-B vestige / partial-rollout sweep.

Design intent (CADS — comprehensive anticipatory):
  * Every ``apply`` pass **always** re-walks entry points on the RKE2 instance:
    nodes → namespaces → controllers → pods → PVC/PV/SC → helm bookkeeping →
    webhooks/poison labels → CR finalizers.
  * Relationships are followed to root cause (mount → PVC → PV; ownerRef chain;
    finalizers blocking deletion; helm pending blocking upgrade).
  * Vestigial **Layer-B** husks are eliminated when they block convergence.
  * **Partial rollout artifacts** (FSM- or zarf-started intermediate state) are
    detected and unwound **when the functional surface is not fully Ready** —
    so converge remains valid from every intermediate state. Unwinds stay
    Layer-B only and idempotent (no-op on a healthy surface).
  * **Layer-A** is never destroyed: no image prune, no hostPath registry data wipe,
    no deletion of zarf-init package / zarf binary, no crictl rmi.

The sweep is idempotent and safe to run on a healthy converged cluster (no-ops).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from .kube import Ctx

# Pending pods older than this become wedges (eviction fallout that never resumed).
# Age is computed from pod metadata.creationTimestamp (kubectl/API SoR — not a journal).
_PENDING_GRACE_S = 300
# Workloads that never became Available within this age are partial-rollout wedges.
_STALL_GRACE_S = 180

# --------------------------------------------------------------------------- #
# Scope: what we own vs what we must not touch
# --------------------------------------------------------------------------- #

# Workload namespaces the package deploys (disposable Layer-B).
APP_NAMESPACES: Tuple[str, ...] = ("dask", "dask-operator", "jupyterhub", "panel-viz")

# Platform namespace from ``zarf init`` — special-cased (registry hostPath conserved).
PLATFORM_NS = "zarf"

# All namespaces we may mutate contents of (never kube-system / rke2 / cattle-*).
MANAGED_NAMESPACES: Tuple[str, ...] = APP_NAMESPACES + (PLATFORM_NS,)

# Cluster-scoped kinds we may clean when they reference managed namespaces only.
DASK_CRD_KINDS: Tuple[str, ...] = (
    "daskclusters", "daskworkergroups", "daskautoscalers", "daskjobs",
)

# Exhaustive namespaced kinds to drain when emptying a managed ns.
DRAIN_KINDS: Tuple[str, ...] = (
    "deployments", "replicasets", "statefulsets", "daemonsets",
    "jobs", "cronjobs", "horizontalpodautoscalers", "poddisruptionbudgets",
    "services", "endpoints", "endpointslices",
    "ingresses", "networkpolicies",
    "configmaps", "secrets", "serviceaccounts",
    "roles", "rolebindings",
    "persistentvolumeclaims",
    "pods",
)

# Layer-A / foundational — NEVER deleted by sweep (document for auditors).
LAYER_A_PROTECTED = (
    "node containerd images (no crictl rmi/prune)",
    "hostPath /var/lib/zarf-registry data (Retain PV object ok to recreate)",
    "zarf binary + zarf-init-*.tar.zst + deploy package on disk",
    "RKE2 system namespaces (kube-system, kube-public, …)",
)

_IMAGE_WAIT_BAD = ("ImagePullBackOff", "ErrImagePull", "ErrImageNeverPull")
_HELM_PENDING = ("pending-install", "pending-upgrade", "pending-rollback")
_POD_JUNK_PHASES = ("Failed", "Evicted", "Unknown")

# Functional surface: package workloads that must read Ready for "converged".
# Each entry: (namespace, alternative label selectors, min_ready).
# Absent namespace is "not ready" only after zarf init / first apply has created it
# — surface assessment treats missing ns as a gap only when *other* surface parts
# exist (partial rollout), not on a clean slate.
_FUNCTIONAL_SURFACE: Tuple[Tuple[str, Tuple[str, ...], int], ...] = (
    (PLATFORM_NS, (
        "app=docker-registry",
        "app.kubernetes.io/name=docker-registry",
        "app.kubernetes.io/name=zarf-docker-registry",
    ), 1),
    ("dask-operator", ("app.kubernetes.io/name=dask-kubernetes-operator",), 1),
    ("dask", ("dask.org/component=scheduler",), 1),
    ("panel-viz", ("app=otel-navigator",), 1),
    ("panel-viz", ("app=navigator-engine",), 1),  # terminal stack (converge-11/12)
    ("jupyterhub", ("component=hub",), 1),
)


@dataclass
class Finding:
    """One discovery observation — optional relationship chain to root cause."""

    severity: str          # info | warn | wedge
    entry: str             # entry point (ns, pvc, pod, …)
    summary: str
    root: str = ""         # root condition if walked
    chain: List[str] = field(default_factory=list)


@dataclass
class DiscoveryReport:
    findings: List[Finding] = field(default_factory=list)
    managed_ns: Dict[str, str] = field(default_factory=dict)  # name → phase
    actions_preview: List[str] = field(default_factory=list)

    def add(self, severity: str, entry: str, summary: str,
            root: str = "", chain: Optional[List[str]] = None) -> None:
        self.findings.append(Finding(severity, entry, summary, root, chain or []))

    @property
    def wedges(self) -> List[Finding]:
        return [f for f in self.findings if f.severity == "wedge"]

    def format(self) -> str:
        lines = ["  ── DISCOVERY (entry points → relationships → root) ──"]
        if self.managed_ns:
            ns_s = ", ".join(f"{n}={p}" for n, p in sorted(self.managed_ns.items()))
            lines.append(f"  managed namespaces: {ns_s or '(none present)'}")
        if not self.findings:
            lines.append("  (no wedges or notable drift)")
        for f in self.findings:
            mark = {"info": "·", "warn": "!", "wedge": "✖"}.get(f.severity, "·")
            lines.append(f"  {mark} [{f.entry}] {f.summary}")
            if f.root:
                lines.append(f"      root: {f.root}")
            for step in f.chain:
                lines.append(f"      → {step}")
        if self.actions_preview:
            lines.append("  would remediate:")
            for a in self.actions_preview[:20]:
                lines.append(f"    • {a}")
            if len(self.actions_preview) > 20:
                lines.append(f"    … +{len(self.actions_preview) - 20} more")
        lines.append("  ── end discovery ──")
        return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Relationship walkers
# --------------------------------------------------------------------------- #

def _pvc_chain(ctx: Ctx, ns: str, pvc_name: str) -> Tuple[str, List[str]]:
    """Walk PVC → PV → SC → pods mounting → root cause string."""
    chain: List[str] = []
    pvc = ctx.get("pvc", pvc_name, ns=ns)
    if not pvc:
        return "PVC absent", chain
    phase = pvc.get("status", {}).get("phase", "?")
    sc = (pvc.get("spec") or {}).get("storageClassName")
    sc = "" if sc is None else sc
    vol = (pvc.get("spec") or {}).get("volumeName") or ""
    del_ts = (pvc.get("metadata") or {}).get("deletionTimestamp")
    finals = (pvc.get("metadata") or {}).get("finalizers") or []
    chain.append(f"PVC {ns}/{pvc_name} phase={phase} sc={sc!r} vol={vol!r} "
                 f"deleting={bool(del_ts)} finals={finals}")
    if vol:
        pv = ctx.get("pv", vol)
        if pv:
            pvp = pv.get("status", {}).get("phase", "?")
            pvsc = (pv.get("spec") or {}).get("storageClassName") or ""
            chain.append(f"PV {vol} phase={pvp} sc={pvsc!r}")
        else:
            chain.append(f"PV {vol} MISSING (claim points at gone volume)")
    if sc:
        sc_obj = ctx.get("storageclass", sc)
        if not sc_obj:
            chain.append(f"StorageClass {sc!r} ABSENT — dynamic provision impossible air-gap")
            return "missing StorageClass for dynamic PVC", chain
        prov = (sc_obj.get("provisioner") or "")
        chain.append(f"StorageClass {sc} provisioner={prov}")
    holders = []
    for p in ctx.items("pods", ns=ns):
        for v in p.get("spec", {}).get("volumes") or []:
            if (v.get("persistentVolumeClaim") or {}).get("claimName") == pvc_name:
                holders.append(p["metadata"]["name"])
    if holders:
        chain.append(f"mounted by pods: {holders}")
    if del_ts or phase == "Terminating":
        root = "PVC Terminating — release mounts then strip finalizers"
    elif phase == "Pending" and sc not in ("", None):
        root = f"PVC Pending on sc={sc!r} — provisioner dead or SC capture"
    elif phase == "Pending":
        root = "PVC Pending on empty class — need static PV claimRef bind"
    elif phase == "Bound":
        root = "PVC Bound (storage OK at this layer)"
    else:
        root = f"PVC phase={phase}"
    return root, chain


def _pod_chain(ctx: Ctx, ns: str, pod: dict) -> Tuple[str, List[str]]:
    """Walk pod → ownerRef → images/waiting reasons → volumes."""
    chain: List[str] = []
    name = pod.get("metadata", {}).get("name", "?")
    phase = pod.get("status", {}).get("phase", "?")
    chain.append(f"Pod {ns}/{name} phase={phase}")
    owners = (pod.get("metadata") or {}).get("ownerReferences") or []
    for o in owners:
        chain.append(f"owner {o.get('kind')}/{o.get('name')} controller={o.get('controller')}")
    for cs in (pod.get("status", {}).get("containerStatuses") or []):
        waiting = (cs.get("state") or {}).get("waiting") or {}
        if waiting:
            chain.append(f"container {cs.get('name')}: waiting {waiting.get('reason')} "
                         f"{(waiting.get('message') or '')[:80]}")
            if waiting.get("reason") in _IMAGE_WAIT_BAD:
                img = ""
                for c in pod.get("spec", {}).get("containers") or []:
                    if c.get("name") == cs.get("name"):
                        img = c.get("image", "")
                if img.startswith("127.0.0.1:"):
                    return "ImagePull on internal registry — content missing (T2)", chain
                return "ImagePull on upstream ref — agent rewrite/poison (T1)", chain
    for v in pod.get("spec", {}).get("volumes") or []:
        claim = (v.get("persistentVolumeClaim") or {}).get("claimName")
        if claim:
            root, sub = _pvc_chain(ctx, ns, claim)
            chain.extend(sub)
            if "Pending" in root or "Terminating" in root or "missing" in root:
                return root, chain
    if phase == "Pending":
        age = _pod_age_s(pod)
        if age is not None and age > _PENDING_GRACE_S:
            return (
                f"Pod Pending beyond grace ({int(age)}s > {_PENDING_GRACE_S}s) — "
                "eviction fallout or stalled rollout; recycle or fix schedule",
                chain,
            )
        return "Pod Pending — scheduling/mount/capacity", chain
    if phase in _POD_JUNK_PHASES:
        return f"Pod junk phase={phase} — delete Layer-B", chain
    return f"Pod phase={phase}", chain


def _pod_age_s(pod: dict) -> Optional[float]:
    ts = (pod.get("metadata") or {}).get("creationTimestamp")
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            created = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            created = datetime.fromisoformat(ts)
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - created).total_seconds()
    except (TypeError, ValueError):
        return None


def _pods_matching_deploy(ctx: Ctx, ns: str, deploy: dict) -> List[dict]:
    """Pods belonging to a Deployment via matchLabels — not namespace-wide."""
    sel = ((deploy.get("spec") or {}).get("selector") or {}).get("matchLabels") or {}
    if not sel:
        return []
    label = ",".join(f"{k}={v}" for k, v in sorted(sel.items()))
    return ctx.items("pods", ns=ns, selector=label)


def _ready_count(pods: List[dict]) -> int:
    n = 0
    for p in pods:
        conds = {c["type"]: c["status"] for c in p.get("status", {}).get("conditions", [])}
        if conds.get("Ready") == "True":
            n += 1
    return n


# --------------------------------------------------------------------------- #
# Discovery (read-only)
# --------------------------------------------------------------------------- #

# Design: discovery is *memoryless* by intent. The K8s API (and on-node Layer-A
# paths like the zarf registry hostPath / package tarballs when present) is the
# system of record. Other operators may have mutated the cluster since the last
# converge run; a local journal cannot outrank live API state and must not gate
# remediations. When we miss a failure class, we extend inspection of *current*
# objects (Deployments, Pods, helm secrets, zarf package secrets, node conditions)
# and the catalog logic table — not out-of-band procedural memory.

def discover(ctx: Ctx) -> DiscoveryReport:
    """Full entry-point walk of managed scope + related cluster objects."""
    rep = DiscoveryReport()

    # --- nodes ---
    for n in ctx.items("nodes"):
        name = n.get("metadata", {}).get("name", "?")
        conds = {c["type"]: c["status"] for c in n.get("status", {}).get("conditions", [])}
        ready = conds.get("Ready") == "True"
        taints = n.get("spec", {}).get("taints") or []
        pressure = [t.get("key") for t in taints
                    if "pressure" in (t.get("key") or "") or t.get("key") == "node.kubernetes.io/disk-pressure"]
        if not ready:
            rep.add("wedge", f"node/{name}", "NotReady", root="node not Ready")
        elif pressure:
            rep.add("wedge", f"node/{name}", f"pressure taints={pressure}",
                    root="disk/memory pressure — free disk SAFELY (no image prune)")
        else:
            rep.add("info", f"node/{name}", "Ready")

    # --- managed namespaces ---
    for ns in MANAGED_NAMESPACES:
        obj = ctx.get("namespace", ns)
        if not obj:
            rep.managed_ns[ns] = "Absent"
            continue
        phase = obj.get("status", {}).get("phase", "?")
        rep.managed_ns[ns] = phase
        labels = (obj.get("metadata") or {}).get("labels") or {}
        if labels.get("zarf.dev/agent") in ("ignore", "skip") and ns in APP_NAMESPACES:
            rep.add("wedge", f"ns/{ns}", "labeled zarf.dev/agent=ignore",
                    root="re-init poison — image rewrite disabled for this ns")
        if phase == "Terminating":
            rep.add("wedge", f"ns/{ns}", "Terminating",
                    root="drain contents then force-finalize when empty")

    # --- husk / controller signatures per managed ns ---
    for ns in MANAGED_NAMESPACES:
        if rep.managed_ns.get(ns) in (None, "Absent"):
            continue
        if rep.managed_ns.get(ns) == "Terminating":
            continue
        deps = ctx.items("deployments", ns=ns)
        sts = ctx.items("statefulsets", ns=ns)
        pods = ctx.items("pods", ns=ns)
        svcs = ctx.items("services", ns=ns)
        ready_pods = _ready_count(pods)
        # Deployments — readiness is **per-Deployment** (selector-scoped pods).
        # Namespace-wide ready_pods hides a wedged scheduler beside healthy workers
        # (converge-09 precondition).
        for d in deps:
            des = d.get("spec", {}).get("replicas")
            if des is None:
                des = 1
            name = d.get("metadata", {}).get("name", "?")
            avail = d.get("status", {}).get("availableReplicas") or 0
            dpods = _pods_matching_deploy(ctx, ns, d)
            dready = _ready_count(dpods)
            if des and des > 0 and not dpods:
                rep.add("wedge", f"deploy/{ns}/{name}",
                        f"desired={des} available={avail} but ZERO pods for this Deployment",
                        root="husk/stalled Deployment — recycle pods or recreate controller")
            elif des and des > 0 and avail == 0 and dready == 0:
                # Promote to wedge: desired replicas, zero ready *for this deploy*
                rep.add("wedge", f"deploy/{ns}/{name}",
                        f"desired={des} available=0 deploy_ready_pods={dready} "
                        f"(ns_ready_pods={ready_pods} — not used for this gate)",
                        root="controller not producing Ready pods — Pending/ImagePull/"
                             "CrashLoop; recycle or fix schedule (not ns-wide healthy)")
            # CR image/spec may have advanced while children stay on old image
            if ns == "dask" and name and "scheduler" in name.lower() and dpods:
                imgs = sorted({
                    c.get("image", "")
                    for p in dpods
                    for c in (p.get("spec") or {}).get("containers") or []
                    if c.get("image")
                })
                if imgs:
                    rep.add("info", f"deploy/{ns}/{name}",
                            f"live images={imgs}",
                            root="compare to package target tag for drift (T4.scheduler)")
        for s in sts:
            des = s.get("spec", {}).get("replicas") or 1
            name = s.get("metadata", {}).get("name", "?")
            ready = s.get("status", {}).get("readyReplicas") or 0
            if des and not pods:
                rep.add("wedge", f"sts/{ns}/{name}",
                        f"desired={des} ready={ready} ZERO pods",
                        root="husk StatefulSet — drain/recreate")
        # Service-only leftovers (classic zarf-injector husk)
        if svcs and not deps and not sts and ready_pods == 0:
            names = [x.get("metadata", {}).get("name") for x in svcs]
            rep.add("wedge", f"ns/{ns}",
                    f"Service-only husk (no controllers, no Ready pods): {names}",
                    root="etcd leftovers after partial destroy — drain ns")

    # --- PVCs in managed ns ---
    for ns in MANAGED_NAMESPACES:
        if rep.managed_ns.get(ns) == "Absent":
            continue
        for pvc in ctx.items("pvc", ns=ns):
            name = pvc.get("metadata", {}).get("name", "?")
            phase = pvc.get("status", {}).get("phase", "?")
            del_ts = (pvc.get("metadata") or {}).get("deletionTimestamp")
            if del_ts or phase in ("Pending", "Terminating", "Lost"):
                root, chain = _pvc_chain(ctx, ns, name)
                rep.add("wedge", f"pvc/{ns}/{name}", f"phase={phase}", root=root, chain=chain)

    # --- orphan / Released PVs claiming managed namespaces ---
    for pv in ctx.items("pv"):
        pname = (pv.get("metadata") or {}).get("name", "?")
        phase = pv.get("status", {}).get("phase", "?")
        claim = (pv.get("spec") or {}).get("claimRef") or {}
        cns = claim.get("namespace") or ""
        # Protect registry hostPath PV object reset is handled in catalog; here only flag.
        if cns in MANAGED_NAMESPACES or "hub-db" in pname or "zarf-registry" in pname:
            if phase in ("Released", "Failed"):
                rep.add("wedge", f"pv/{pname}", f"phase={phase} claim={cns}/{claim.get('name')}",
                        root="Released/Failed PV — reset claimRef or delete object "
                             "(hostPath data conserved if Retain)")
            elif phase == "Available" and cns in APP_NAMESPACES:
                rep.add("warn", f"pv/{pname}", f"Available with stale claimRef to {cns}",
                        root="may block rebinding — clear or re-apply claimRef")

    # --- helm pending + DEAD releases ---
    obj = ctx.kjson(["get", "secrets", "-A", "-l", "owner=helm"]) or {}
    latest: dict = {}
    history: dict = {}   # (ns, release) -> set of all revision statuses
    for s in obj.get("items", []):
        md = s.get("metadata", {}) or {}
        lab = md.get("labels", {}) or {}
        try:
            ver = int(lab.get("version", 0))
        except (TypeError, ValueError):
            continue
        key = (md.get("namespace"), lab.get("name"))
        history.setdefault(key, set()).add(lab.get("status"))
        if key not in latest or ver > latest[key][0]:
            latest[key] = (ver, md.get("name"), lab.get("status"), md.get("namespace"))
    for (ns, rel), (ver, name, status, _) in latest.items():
        hist = history.get((ns, rel), set())
        if status in _HELM_PENDING:
            rep.add("wedge", f"helm/{ns}/{rel}",
                    f"latest rev {ver} status={status}",
                    root="delete pending secret so next deploy can proceed")
        elif status == "failed" and not (hist & {"deployed", "superseded"}):
            # DEAD release (field-proven): first install failed, no revision ever
            # deployed → every `helm upgrade` refuses with "has no deployed releases".
            rep.add("wedge", f"helm/{ns}/{rel}",
                    f"DEAD release: rev {ver} failed, no deployed revision in history",
                    root="helm upgrade will always fail 'has no deployed releases' — "
                         "remove the component's failed chart and redeploy (engine "
                         "auto-recovers on the next apply)")
        elif status == "failed" and (hist & {"deployed", "superseded"}):
            # Interrupted upgrade (converge-09 class): wait-killed or mid-flight fail
            # leaves latest=failed while an older revision still "deployed"/superseded.
            rep.add("wedge", f"helm/{ns}/{rel}",
                    f"INTERRUPTED upgrade: latest rev {ver} failed; history has "
                    f"prior deployed/superseded {sorted(hist)}",
                    root="do not re-run full package deploy blindly — unwedge helm, "
                         "verify target Deployment Ready, then targeted redeploy")

    # --- Zarf package ledger (which components last deployed) ---
    # Prefer zarf ns + managed app ns; match name/label conventions (avoid cluster-wide
    # secret dump).
    ledger_ns = (PLATFORM_NS,) + APP_NAMESPACES
    for lns in ledger_ns:
        if rep.managed_ns.get(lns) == "Absent" and lns != PLATFORM_NS:
            continue
        zobj = ctx.kjson(["get", "secrets", "-n", lns]) or {}
        for s in zobj.get("items", []) or []:
            md = s.get("metadata") or {}
            name = md.get("name") or ""
            labs = md.get("labels") or {}
            if not (name.startswith("zarf-package") or "zarf-package" in name
                    or labs.get("zarf.dev/package-name")
                    or labs.get("package-deployed") == "true"):
                continue
            data_keys = list((s.get("data") or {}).keys())[:12]
            zlabs = {k: labs[k] for k in labs
                     if "zarf" in k or k in ("package-deployed", "name", "version")}
            rep.add("info", f"zarf-ledger/{lns}/{name}",
                    f"labels={zlabs} data_keys={data_keys}",
                    root="cluster-resident record of package deploy (API SoR; not a journal)")

    # --- junk / ImagePull / Pending-beyond-grace pods ---
    for ns in MANAGED_NAMESPACES:
        if rep.managed_ns.get(ns) == "Absent":
            continue
        for p in ctx.items("pods", ns=ns):
            phase = p.get("status", {}).get("phase", "")
            name = p.get("metadata", {}).get("name", "?")
            stuck_pull = any(
                ((cs.get("state") or {}).get("waiting") or {}).get("reason") in _IMAGE_WAIT_BAD
                for cs in (p.get("status", {}).get("containerStatuses") or []))
            age = _pod_age_s(p)
            pending_stale = (
                phase == "Pending"
                and age is not None
                and age > _PENDING_GRACE_S
            )
            if phase in _POD_JUNK_PHASES or stuck_pull or phase == "Pending":
                root, chain = _pod_chain(ctx, ns, p)
                # Pending-beyond-grace is a wedge (eviction fallout that never resumed)
                sev = (
                    "wedge"
                    if stuck_pull or phase in _POD_JUNK_PHASES or pending_stale
                    else "warn"
                )
                age_s = f" age={int(age)}s" if age is not None else ""
                rep.add(sev, f"pod/{ns}/{name}", f"phase={phase}{age_s}",
                        root=root, chain=chain)

    # --- VolumeAttachments (node-level mounts left after pod death) ---
    for va in ctx.items("volumeattachments"):
        md = va.get("metadata") or {}
        if md.get("deletionTimestamp"):
            rep.add("wedge", f"volumeattachment/{md.get('name')}",
                    "Terminating",
                    root="stuck VolumeAttachment — may hold PVC protection finalizer")

    # --- Dask CRs with finalizers when operator may be dead ---
    for kind in DASK_CRD_KINDS:
        for it in ctx.items(kind):
            md = it.get("metadata") or {}
            if md.get("deletionTimestamp") or md.get("finalizers"):
                if md.get("deletionTimestamp"):
                    rep.add("wedge", f"{kind}/{md.get('namespace')}/{md.get('name')}",
                            f"finalizers={md.get('finalizers')}",
                            root="CR stuck deleting — strip finalizers (operator may be gone)")

    return rep


# --------------------------------------------------------------------------- #
# Sweep (Layer-B only, idempotent)
# --------------------------------------------------------------------------- #

def _strip_finalizers_obj(ctx: Ctx, kind: str, name: str, ns: Optional[str] = None) -> bool:
    args = ["patch", kind, name, "--type=merge", "-p", '{"metadata":{"finalizers":null}}']
    if ns:
        args[1:1] = []  # no-op keep structure
        args = ["patch", kind, name, "-n", ns, "--type=merge",
                "-p", '{"metadata":{"finalizers":null}}']
    return ctx.k(args).returncode == 0


def _force_delete_pvc_local(ctx: Ctx, ns: str, name: str) -> bool:
    """Mount-aware PVC delete (mirrors catalog; kept local to avoid import cycles)."""
    # release mounts
    for kind in ("deploy", "sts", "ds", "job"):
        ctx.k(["delete", kind, "--all", "-n", ns, "--wait=false", "--ignore-not-found"])
    for p in ctx.items("pods", ns=ns):
        for v in p.get("spec", {}).get("volumes") or []:
            if (v.get("persistentVolumeClaim") or {}).get("claimName") == name:
                ctx.k(["delete", "pod", p["metadata"]["name"], "-n", ns,
                       "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
    ctx.k(["delete", "pvc", name, "-n", ns, "--ignore-not-found", "--wait=false"])
    if ctx.get("pvc", name, ns=ns):
        ctx.k(["patch", "pvc", name, "-n", ns, "--type=merge",
               "-p", '{"metadata":{"finalizers":null}}'])
    if ctx.get("pvc", name, ns=ns):
        ctx.k(["patch", "pvc", name, "-n", ns, "--type=json",
               "-p", '[{"op":"remove","path":"/metadata/finalizers"}]'])
    return ctx.get("pvc", name, ns=ns) is None


def _force_finalize_ns(ctx: Ctx, ns: str) -> bool:
    obj = ctx.get("namespace", ns)
    if not obj:
        return False
    obj["spec"] = {"finalizers": []}
    r = ctx.run(ctx.kubectl + ["replace", "--raw",
                f"/api/v1/namespaces/{ns}/finalize", "-f", "-"],
                input_=json.dumps(obj))
    return r.returncode == 0


def _drain_ns(ctx: Ctx, ns: str) -> List[str]:
    """Empty managed namespace contents (not the ns object). Layer-B only."""
    if not ctx.exists("namespace", ns):
        return []
    actions: List[str] = []
    # Controllers first
    for kind in ("deployments", "replicasets", "statefulsets", "daemonsets",
                 "jobs", "cronjobs", "horizontalpodautoscalers"):
        ctx.k(["delete", kind, "--all", "-n", ns, "--wait=false", "--ignore-not-found"])
    ctx.k(["delete", "pods", "--all", "-n", ns,
           "--force", "--grace-period=0", "--wait=false", "--ignore-not-found"])
    for kind in ("services", "endpoints", "endpointslices", "ingresses", "networkpolicies",
                 "configmaps", "secrets", "roles", "rolebindings", "serviceaccounts",
                 "poddisruptionbudgets"):
        ctx.k(["delete", kind, "--all", "-n", ns, "--wait=false", "--ignore-not-found"])
    for pvc in list(ctx.items("pvc", ns=ns)):
        name = pvc.get("metadata", {}).get("name")
        if name and _force_delete_pvc_local(ctx, ns, name):
            actions.append(f"drained PVC {ns}/{name}")
    # finalizer strip on stragglers
    for kind in DRAIN_KINDS:
        for it in ctx.items(kind, ns=ns):
            if (it.get("metadata") or {}).get("finalizers"):
                n = it["metadata"]["name"]
                if _strip_finalizers_obj(ctx, kind, n, ns=ns):
                    actions.append(f"stripped finalizers {kind}/{ns}/{n}")
    actions.append(f"drained ns {ns}")
    return actions


def _registry_ready(ctx: Ctx) -> bool:
    for sel in ("app=docker-registry", "app.kubernetes.io/name=zarf-docker-registry"):
        ready, _ = ctx.pods_ready(PLATFORM_NS, sel)
        if ready >= 1:
            return True
    return False


def _is_app_husk(ctx: Ctx, ns: str) -> Optional[str]:
    """Return reason if Active app ns should be torn down as a husk."""
    deps = ctx.items("deployments", ns=ns)
    sts = ctx.items("statefulsets", ns=ns)
    pods = ctx.items("pods", ns=ns)
    svcs = ctx.items("services", ns=ns)
    if (deps or sts) and not pods:
        return "controllers present, zero pods (resurrection husk)"
    if svcs and not deps and not sts and not pods:
        return "Service-only husk (no controllers/pods)"
    # Deployments desired>0, zero available, all pods non-Ready for extended junk
    for d in deps:
        des = d.get("spec", {}).get("replicas")
        if des is None:
            des = 1
        avail = d.get("status", {}).get("availableReplicas") or 0
        if des > 0 and avail == 0 and pods:
            # only husk if every pod is Failed/Evicted or ImagePull with upstream
            all_junk = True
            for p in pods:
                phase = p.get("status", {}).get("phase")
                stuck = any(
                    ((cs.get("state") or {}).get("waiting") or {}).get("reason") in _IMAGE_WAIT_BAD
                    for cs in (p.get("status", {}).get("containerStatuses") or []))
                imgs = [c.get("image", "") for c in p.get("spec", {}).get("containers") or []]
                upstream = any(i and not i.startswith("127.0.0.1:") for i in imgs)
                if phase not in _POD_JUNK_PHASES and not (stuck and upstream):
                    all_junk = False
                    break
            if all_junk and pods:
                return "all pods junk/unmutated ImagePull — recycle ns"
    return None


def _is_zarf_husk(ctx: Ctx) -> Optional[str]:
    if not ctx.exists("namespace", PLATFORM_NS):
        return None
    ns = ctx.get("namespace", PLATFORM_NS)
    if ns and ns.get("status", {}).get("phase") == "Terminating":
        return "zarf ns Terminating"
    if _registry_ready(ctx):
        return None
    pvc = ctx.get("pvc", "zarf-docker-registry", ns=PLATFORM_NS)
    if pvc:
        phase = pvc.get("status", {}).get("phase")
        sc = (pvc.get("spec") or {}).get("storageClassName")
        sc = "" if sc is None else sc
        del_ts = (pvc.get("metadata") or {}).get("deletionTimestamp")
        if del_ts or phase == "Terminating":
            return "registry PVC Terminating"
        if phase == "Bound" and sc == "":
            return None  # in-progress healthy bind
        if phase == "Pending" or sc != "":
            return f"registry PVC phase={phase} sc={sc!r}"
    svcs = ctx.items("services", ns=PLATFORM_NS)
    deps = ctx.items("deployments", ns=PLATFORM_NS)
    pods = ctx.items("pods", ns=PLATFORM_NS)
    if svcs or deps or pods or pvc:
        return "zarf partial/husk without Ready registry"
    return None


# --------------------------------------------------------------------------- #
# Functional surface + partial-rollout classification
# --------------------------------------------------------------------------- #

def _pods_ready_any(ctx: Ctx, ns: str, selectors: Sequence[str]) -> Tuple[int, int]:
    """(ready, total) for the first selector that matches any pods."""
    best = (0, 0)
    for sel in selectors:
        r, t = ctx.pods_ready(ns, sel)
        if t > best[1]:
            best = (r, t)
        if r >= 1:
            return r, t
    return best


def functional_surface(ctx: Ctx) -> dict:
    """Whether the package functional surface is Ready across the board.

    Used to decide **deep** partial-rollout unwind: when the surface is not
    fully Ready, intermediate FSM/zarf artifacts (failed helm revs, stalled
    Deployments, Failed Jobs, …) are wedges that must be cleared before the
    next remediate pass is valid. When every surface check is Ready, deep
    unwind no-ops (only always-safe vestiges run).

    Returns dict: ready (bool), issues (list[str]), checks (list[dict]).
    """
    issues: List[str] = []
    checks: List[dict] = []
    any_ns = False
    for ns, selectors, need in _FUNCTIONAL_SURFACE:
        ns_obj = ctx.get("namespace", ns)
        if not ns_obj:
            checks.append({"ns": ns, "ready": 0, "total": 0, "absent": True})
            continue
        any_ns = True
        ready, total = _pods_ready_any(ctx, ns, selectors)
        checks.append({
            "ns": ns, "ready": ready, "total": total, "absent": False,
            "ok": ready >= need,
        })
        if ready < need:
            issues.append(f"{ns}: ready={ready}/{total} (need ≥{need})")
    # Clean slate (no managed ns at all) is not a "partial rollout" — no deep
    # unwind yet; first apply creates state. Surface not ready when we have
    # some stack but not full Ready.
    if not any_ns:
        return {"ready": False, "issues": ["no managed namespaces yet (pre-deploy)"],
                "checks": checks, "partial": False}
    # Content-tag drift (converge-10/11/12): Ready pods on stale cybersec-dask
    # must not count as a Ready surface — arm partial-rollout unwind.
    target = ""
    manifest = getattr(ctx, "manifest", None) or {}
    for img in (manifest.get("package_images") or {}).get("images", []) or []:
        ref = img.get("ref") or ""
        if "cybersec-dask" in ref and ":" in ref.rsplit("/", 1)[-1]:
            tag = ref.rsplit(":", 1)[-1].split("-zarf-", 1)[0]
            target = tag
            break
    if target:
        for ns, sels in (
            ("dask", ("dask.org/component=scheduler",)),
            ("panel-viz", ("app=otel-navigator", "app=navigator-engine")),
        ):
            if not ctx.get("namespace", ns):
                continue
            for sel in sels:
                for p in ctx.items("pods", ns=ns, selector=sel):
                    for c in (p.get("spec") or {}).get("containers") or []:
                        img = c.get("image") or ""
                        if "cybersec-dask" not in img:
                            continue
                        last = img.split("@", 1)[0].rsplit("/", 1)[-1]
                        run = (last.rsplit(":", 1)[-1].split("-zarf-", 1)[0]
                               if ":" in last else "")
                        if run and run != target:
                            issues.append(
                                f"{ns}/{sel}: image drift running={run} target={target}")
    partial = bool(issues)
    return {
        "ready": not issues,
        "issues": issues,
        "checks": checks,
        "partial": partial,
    }


def _helm_release_index(ctx: Ctx) -> List[dict]:
    """Index helm release secrets → one row per (ns, release name).

    Each row:
      ns, name, latest_ver, latest_status, latest_secret,
      history: set of statuses, secrets: [(ver, secret_name, status), ...]
    """
    obj = ctx.kjson(["get", "secrets", "-A", "-l", "owner=helm"]) or {}
    by: Dict[Tuple[str, str], dict] = {}
    for s in obj.get("items", []) or []:
        md = s.get("metadata", {}) or {}
        lab = md.get("labels", {}) or {}
        ns = md.get("namespace") or ""
        rel = lab.get("name") or ""
        if not ns or not rel:
            continue
        try:
            ver = int(lab.get("version", 0))
        except (TypeError, ValueError):
            continue
        status = lab.get("status") or ""
        secret = md.get("name") or ""
        key = (ns, rel)
        row = by.setdefault(key, {
            "ns": ns, "name": rel,
            "latest_ver": -1, "latest_status": "", "latest_secret": "",
            "history": set(), "secrets": [],
        })
        row["history"].add(status)
        row["secrets"].append((ver, secret, status))
        if ver > row["latest_ver"]:
            row["latest_ver"] = ver
            row["latest_status"] = status
            row["latest_secret"] = secret
    return list(by.values())


def classify_helm_release(row: dict) -> str:
    """Return pending | dead | interrupted | ok | other for a helm release index row."""
    status = row.get("latest_status") or ""
    hist = row.get("history") or set()
    if status in _HELM_PENDING:
        return "pending"
    if status == "failed" and not (hist & {"deployed", "superseded"}):
        return "dead"
    if status == "failed" and (hist & {"deployed", "superseded"}):
        return "interrupted"
    if status in ("deployed", "superseded", "uninstalled"):
        return "ok"
    return "other"


def _age_s_from_meta(obj: dict) -> Optional[float]:
    ts = (obj.get("metadata") or {}).get("creationTimestamp")
    if not ts:
        return None
    try:
        # RFC3339
        if ts.endswith("Z"):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except (TypeError, ValueError):
        return None


def _deployment_progress_failed(dep: dict) -> bool:
    for c in (dep.get("status") or {}).get("conditions") or []:
        if c.get("type") == "Progressing" and c.get("status") == "False":
            if c.get("reason") == "ProgressDeadlineExceeded":
                return True
    return False


def _deployment_stalled_unavailable(dep: dict) -> bool:
    """Desired replicas > 0, none available, older than stall grace."""
    spec = dep.get("spec") or {}
    status = dep.get("status") or {}
    desired = spec.get("replicas")
    if desired is None:
        desired = 1
    try:
        desired = int(desired)
    except (TypeError, ValueError):
        desired = 1
    if desired <= 0:
        return False
    avail = status.get("availableReplicas") or 0
    try:
        avail = int(avail)
    except (TypeError, ValueError):
        avail = 0
    if avail > 0:
        return False
    age = _age_s_from_meta(dep)
    return age is not None and age > _STALL_GRACE_S


def _unwind_helm_partial(ctx: Ctx, *, dry_run: bool) -> List[str]:
    """Clear helm intermediate states that block the next deploy.

    * pending-* latest → delete that secret (already always-safe)
    * DEAD (failed only) → delete **all** release secrets (install can restart)
    * INTERRUPTED (failed latest + prior deployed) → delete **latest failed** only
      so upgrade can retry from last good revision
    """
    actions: List[str] = []
    for row in _helm_release_index(ctx):
        kind = classify_helm_release(row)
        ns, rel = row["ns"], row["name"]
        if kind == "pending":
            name = row["latest_secret"]
            if dry_run:
                actions.append(
                    f"(dry-run) delete pending helm {ns}/{name} ({rel} v{row['latest_ver']})")
            elif name and ctx.k(["delete", "secret", name, "-n", ns,
                                "--ignore-not-found"]).returncode == 0:
                actions.append(
                    f"unwedged pending Helm {ns}/{rel} v{row['latest_ver']} "
                    f"({row['latest_status']})")
        elif kind == "dead":
            # All revisions failed — wipe release bookkeeping so next zarf install
            # is not blocked by 'has no deployed releases'.
            secrets = sorted(row["secrets"], key=lambda x: x[0], reverse=True)
            deleted = 0
            for ver, secret, status in secrets:
                if dry_run:
                    actions.append(
                        f"(dry-run) delete DEAD helm secret {ns}/{secret} "
                        f"({rel} v{ver} {status})")
                    deleted += 1
                    continue
                if secret and ctx.k(["delete", "secret", secret, "-n", ns,
                                    "--ignore-not-found"]).returncode == 0:
                    deleted += 1
            if deleted:
                actions.append(
                    f"unwound DEAD helm release {ns}/{rel} "
                    f"({deleted} failed-only revision secret(s))")
        elif kind == "interrupted":
            name = row["latest_secret"]
            if dry_run:
                actions.append(
                    f"(dry-run) delete INTERRUPTED helm latest {ns}/{name} "
                    f"({rel} v{row['latest_ver']} failed; prior deployed kept)")
            elif name and ctx.k(["delete", "secret", name, "-n", ns,
                                "--ignore-not-found"]).returncode == 0:
                actions.append(
                    f"unwound INTERRUPTED helm {ns}/{rel} "
                    f"(dropped failed v{row['latest_ver']}; prior deployed retained)")
    return actions


def _unwind_stalled_workloads(ctx: Ctx, *, dry_run: bool) -> List[str]:
    """Recycle partial Deployments / Failed Jobs when surface is not Ready.

    Targets FSM- and zarf-created intermediate controllers that will never
    become Available without a pod recycle or re-create.
    """
    actions: List[str] = []
    for ns in APP_NAMESPACES:
        if not ctx.exists("namespace", ns):
            continue
        for dep in list(ctx.items("deployments", ns=ns)):
            name = (dep.get("metadata") or {}).get("name")
            if not name:
                continue
            stalled = (_deployment_progress_failed(dep)
                       or _deployment_stalled_unavailable(dep))
            if not stalled:
                continue
            reason = ("ProgressDeadlineExceeded" if _deployment_progress_failed(dep)
                      else f"unavailable>{_STALL_GRACE_S}s")
            if dry_run:
                actions.append(
                    f"(dry-run) recycle stalled Deployment {ns}/{name} ({reason})")
                continue
            # Prefer label from deployment selector so the RS recreates pods
            match = ((dep.get("spec") or {}).get("selector") or {}).get("matchLabels") or {}
            if match:
                sel = ",".join(f"{k}={v}" for k, v in match.items())
                if ctx.k(["delete", "pod", "-n", ns, "-l", sel,
                          "--force", "--grace-period=0", "--wait=false",
                          "--ignore-not-found"]).returncode == 0:
                    actions.append(
                        f"recycled pods for stalled Deployment {ns}/{name} ({reason})")
                else:
                    actions.append(
                        f"attempted recycle stalled Deployment {ns}/{name} ({reason})")
            else:
                actions.append(
                    f"stalled Deployment {ns}/{name} ({reason}) — no selector to recycle")

        # Failed Jobs (helm hooks / one-shot) block some chart re-applies
        for job in list(ctx.items("jobs", ns=ns)):
            name = (job.get("metadata") or {}).get("name")
            status = job.get("status") or {}
            failed = int(status.get("failed") or 0)
            succeeded = int(status.get("succeeded") or 0)
            if failed > 0 and succeeded == 0 and name:
                if dry_run:
                    actions.append(f"(dry-run) delete Failed Job {ns}/{name}")
                elif ctx.k(["delete", "job", name, "-n", ns,
                            "--wait=false", "--ignore-not-found"]).returncode == 0:
                    actions.append(f"deleted Failed Job {ns}/{name} (partial rollout)")

        # Zero-replica ReplicaSets left after a failed scale/roll — GC layer-B only
        for rs in list(ctx.items("replicasets", ns=ns)):
            name = (rs.get("metadata") or {}).get("name")
            desired = (rs.get("spec") or {}).get("replicas")
            try:
                desired = int(desired if desired is not None else 1)
            except (TypeError, ValueError):
                desired = 1
            status = rs.get("status") or {}
            replicas = int(status.get("replicas") or 0)
            age = _age_s_from_meta(rs)
            if desired == 0 and replicas == 0 and age is not None and age > _STALL_GRACE_S:
                if dry_run:
                    actions.append(f"(dry-run) delete orphan ReplicaSet {ns}/{name}")
                elif name and ctx.k(["delete", "replicaset", name, "-n", ns,
                                    "--wait=false", "--ignore-not-found"]).returncode == 0:
                    actions.append(f"deleted orphan ReplicaSet {ns}/{name}")
    return actions


def _unwind_partial_dask_children(ctx: Ctx, *, dry_run: bool) -> List[str]:
    """If DaskCluster CR exists but scheduler Deployment is missing — partial CR.

    Operator is creation-only; a live CR with no scheduler child is an intermediate
    that will never heal without CR recycle (handled fully in catalog rem; here we
    only annotate via action log when dry-run, or strip stuck finalizers).
    """
    actions: List[str] = []
    if not ctx.exists("namespace", "dask"):
        return actions
    crs = ctx.items("daskcluster", ns="dask") or ctx.items("daskclusters", ns="dask")
    if not crs:
        return actions
    scheds = ctx.items("deployments", ns="dask", selector="dask.org/component=scheduler")
    if scheds:
        return actions
    for cr in crs:
        name = (cr.get("metadata") or {}).get("name") or "cybersec-dask"
        if dry_run:
            actions.append(
                f"(dry-run) DaskCluster {name} has no scheduler Deployment "
                f"(partial — catalog rem will recycle CR)")
            continue
        # Do not delete CR here (needs package re-apply path); strip stuck delete
        if (cr.get("metadata") or {}).get("deletionTimestamp"):
            if ctx.k(["patch", "daskcluster", name, "-n", "dask", "--type=merge",
                      "-p", '{"metadata":{"finalizers":null}}']).returncode == 0:
                actions.append(f"stripped finalizers stuck-deleting DaskCluster/{name}")
        else:
            actions.append(
                f"partial DaskCluster/{name}: no scheduler Deployment "
                f"(deferred to T4.scheduler rem)")
    return actions


def sweep_vestiges(ctx: Ctx, *, dry_run: bool = False) -> List[str]:
    """Eliminate Layer-B vestiges + partial-rollout artifacts that block convergence.

    Never touches Layer-A. Safe on a healthy cluster: if the functional surface is
    fully Ready, deep partial-rollout unwind no-ops and only always-safe vestiges
    (pending helm, junk pods, Terminating) may act — usually empty.

    When the surface is **not** Ready across the board (partial=True), also unwind:
      * DEAD / INTERRUPTED helm releases
      * stalled Deployments (ProgressDeadlineExceeded / long unavailable)
      * Failed Jobs, orphan zero ReplicaSets
      * stuck-deleting Dask CR finalizers
    """
    actions: List[str] = []
    surface = functional_surface(ctx)
    deep = bool(surface.get("partial"))  # some stack present but not fully Ready

    def act(msg: str, fn=None) -> None:
        if dry_run:
            actions.append(f"(dry-run) {msg}")
            return
        if fn:
            fn()
        actions.append(msg)

    # 0. Helm intermediate states — always (pending/dead/interrupted block deploy)
    actions.extend(_unwind_helm_partial(ctx, dry_run=dry_run))

    # 0b. Deep partial-rollout unwind only when functional surface incomplete.
    # (Idempotent: fully Ready → skip; partial stack → clear intermediate artifacts.)
    if deep:
        more = _unwind_stalled_workloads(ctx, dry_run=dry_run)
        more += _unwind_partial_dask_children(ctx, dry_run=dry_run)
        if more:
            actions.append(
                "partial-rollout unwind (surface not ready: "
                + "; ".join(surface["issues"][:4]) + ")")
            actions.extend(more)

    # 2. Agent poison on app namespaces
    for ns in APP_NAMESPACES:
        obj = ctx.get("namespace", ns)
        if not obj:
            continue
        labels = (obj.get("metadata") or {}).get("labels") or {}
        if labels.get("zarf.dev/agent") in ("ignore", "skip"):
            if dry_run:
                actions.append(f"(dry-run) strip zarf.dev/agent=ignore from {ns}")
            elif ctx.k(["label", "namespace", ns, "zarf.dev/agent-", "--overwrite"]).returncode == 0:
                actions.append(f"stripped zarf.dev/agent=ignore from {ns}")

    # 3. Per managed namespace
    for ns in MANAGED_NAMESPACES:
        nobj = ctx.get("namespace", ns)
        if not nobj:
            continue
        phase = nobj.get("status", {}).get("phase")

        if phase == "Terminating":
            if dry_run:
                actions.append(f"(dry-run) drain+finalize Terminating ns {ns}")
                continue
            actions.extend(_drain_ns(ctx, ns))
            leftovers = any(ctx.items(k, ns=ns) for k in ("deployments", "pods", "pvc"))
            if not leftovers and _force_finalize_ns(ctx, ns):
                actions.append(f"force-finalized Terminating ns {ns}")
            continue

        # Terminating PVCs always
        for pvc in list(ctx.items("pvc", ns=ns)):
            name = pvc.get("metadata", {}).get("name")
            pphase = pvc.get("status", {}).get("phase")
            del_ts = (pvc.get("metadata") or {}).get("deletionTimestamp")
            sc = (pvc.get("spec") or {}).get("storageClassName")
            sc = "" if sc is None else sc
            # Do not delete healthy Bound registry PVC while registry Ready
            if (ns == PLATFORM_NS and name == "zarf-docker-registry"
                    and pphase == "Bound" and sc == "" and _registry_ready(ctx)):
                continue
            bad = bool(del_ts) or pphase in ("Terminating", "Lost")
            # Pending dynamic class in app ns — orphan path
            if ns in APP_NAMESPACES and pphase == "Pending" and sc not in ("",):
                bad = True
            if ns == PLATFORM_NS and (pphase == "Pending" or (sc != "" and pphase != "Bound")):
                bad = True
            if bad and name:
                if dry_run:
                    actions.append(f"(dry-run) force-delete PVC {ns}/{name} phase={pphase}")
                elif _force_delete_pvc_local(ctx, ns, name):
                    actions.append(f"force-deleted PVC {ns}/{name} (phase={pphase} sc={sc!r})")

        # Junk pods
        for p in list(ctx.items("pods", ns=ns)):
            name = p.get("metadata", {}).get("name")
            pphase = p.get("status", {}).get("phase")
            stuck = any(
                ((cs.get("state") or {}).get("waiting") or {}).get("reason") in _IMAGE_WAIT_BAD
                for cs in (p.get("status", {}).get("containerStatuses") or []))
            imgs = [c.get("image", "") for c in p.get("spec", {}).get("containers") or []]
            upstream_stuck = stuck and any(i and not i.startswith("127.0.0.1:") for i in imgs)
            if pphase in _POD_JUNK_PHASES or upstream_stuck:
                if dry_run:
                    actions.append(f"(dry-run) delete junk pod {ns}/{name} phase={pphase}")
                elif name and ctx.k(["delete", "pod", name, "-n", ns,
                                     "--force", "--grace-period=0",
                                     "--wait=false"]).returncode == 0:
                    actions.append(f"deleted junk pod {ns}/{name} phase={pphase}")

        # Husk namespace recycle (app)
        if ns in APP_NAMESPACES:
            reason = _is_app_husk(ctx, ns)
            if reason:
                if dry_run:
                    actions.append(f"(dry-run) recycle husk ns {ns}: {reason}")
                else:
                    actions.extend(_drain_ns(ctx, ns))
                    ctx.k(["delete", "namespace", ns, "--wait=false"])
                    actions.append(f"deleted husk ns {ns} ({reason})")

        # Zarf husk (platform) — drain but do NOT delete hostPath data
        if ns == PLATFORM_NS:
            reason = _is_zarf_husk(ctx)
            if reason and "Bound" not in (reason or ""):
                if dry_run:
                    actions.append(f"(dry-run) drain zarf husk: {reason}")
                else:
                    actions.extend(_drain_ns(ctx, ns))
                    actions.append(f"drained zarf husk ({reason})")

    # 4. Orphan PVs for app claims (never wipe zarf-registry hostPath *data*;
    #    deleting the PV *object* with Retain is OK and recreated by catalog)
    for pv in list(ctx.items("pv")):
        pname = (pv.get("metadata") or {}).get("name", "")
        phase = pv.get("status", {}).get("phase", "")
        claim = (pv.get("spec") or {}).get("claimRef") or {}
        cns = claim.get("namespace") or ""
        if phase not in ("Released", "Failed"):
            continue
        if cns in APP_NAMESPACES or "hub-db" in pname:
            if dry_run:
                actions.append(f"(dry-run) delete orphan PV {pname} phase={phase}")
            elif ctx.k(["delete", "pv", pname, "--ignore-not-found"]).returncode == 0:
                actions.append(f"deleted orphan PV {pname} (phase={phase})")
        # zarf-registry-pv Released → leave for catalog reset (re-apply claimRef)

    # 5. Stuck VolumeAttachments
    for va in list(ctx.items("volumeattachments")):
        md = va.get("metadata") or {}
        if not md.get("deletionTimestamp"):
            continue
        name = md.get("name")
        if dry_run:
            actions.append(f"(dry-run) strip finalizers volumeattachment/{name}")
        elif name and _strip_finalizers_obj(ctx, "volumeattachment", name):
            actions.append(f"stripped finalizers volumeattachment/{name}")

    # 6. Dask CRs stuck deleting
    for kind in DASK_CRD_KINDS:
        for it in list(ctx.items(kind)):
            md = it.get("metadata") or {}
            if not md.get("deletionTimestamp"):
                continue
            name, ns = md.get("name"), md.get("namespace")
            if not name:
                continue
            if dry_run:
                actions.append(f"(dry-run) strip finalizers {kind}/{ns}/{name}")
                continue
            if ns:
                rc = ctx.k(["patch", kind, name, "-n", ns, "--type=merge",
                            "-p", '{"metadata":{"finalizers":null}}']).returncode
            else:
                rc = ctx.k(["patch", kind, name, "--type=merge",
                            "-p", '{"metadata":{"finalizers":null}}']).returncode
            if rc == 0:
                actions.append(f"stripped finalizers {kind}/{ns}/{name}")

    return actions


def print_discovery(ctx: Ctx, *, preview_sweep: bool = False) -> DiscoveryReport:
    """Run discovery, optionally attach dry-run sweep preview, print, return report."""
    rep = discover(ctx)
    if preview_sweep:
        rep.actions_preview = sweep_vestiges(ctx, dry_run=True)
    print(rep.format())
    # System / RKE2 plane + package inventory (anticipatory edges)
    try:
        from . import platform as _platform
        sys_lines = _platform.discover_system_plane(ctx)
        if sys_lines:
            print("  ── SYSTEM / RKE2 PLANE ──")
            for line in sys_lines:
                print(f"  · {line}")
        pkgs = _platform.find_deploy_packages()
        if pkgs:
            print(f"  ── LAYER-A PACKAGES ({len(pkgs)}) ──")
            for p in pkgs[:8]:
                print(f"  · {p}")
            if len(pkgs) > 8:
                print(f"  · … +{len(pkgs) - 8} more")
        gc_ok = _platform.kubelet_gc_policy_ok()
        print(f"  ── KUBELET GC POLICY: "
              f"{'raised (safe)' if gc_ok else 'DEFAULT/RISKY — apply will raise thresholds'} ──")
    except Exception as e:  # noqa: BLE001 — discovery must never abort reconcile
        print(f"  ── platform discovery partial: {e} ──")
    return rep