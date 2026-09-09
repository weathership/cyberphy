"""Node/platform anticipatory edges for air-gap RKE2 convergence.

Covers residual edges that live *outside* pure K8s object state but determine
whether a closed-world deploy can complete or stay complete:

  * kubelet image-GC policy (default 85% silently deletes Layer-A images)
  * RKE2 / system-plane health (read-only discovery + limited safe remediations)
  * IngressClass selection (nginx on RKE2 vs traefik on K3s)
  * Layer-A package uniqueness (multiple deploy tarballs → wrong mtime pick)
  * HostPath writability for registry (shared with catalog hostPath prep)

CONSERVATION: never prunes container images; never deletes package tarballs
except when the operator explicitly stages a single version. Kubelet policy
remediation only *raises* GC thresholds (more conservative), never lowers them.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .kube import Ctx
from . import manual as _manual
from .model import Fix, Probe

# --------------------------------------------------------------------------- #
# Paths / constants
# --------------------------------------------------------------------------- #

RKE2_CONFIG = Path("/etc/rancher/rke2/config.yaml")
REGISTRY_HOSTPATH = Path("/var/lib/zarf-registry")
# Field: package often staged beside the unpacked engine, not only /var/tmp.
DEFAULT_PKG_DIRS = (
    Path("/var/tmp"),
    Path("/opt/zarf"),
    Path.cwd(),
    Path(__file__).resolve().parent.parent,  # …/zarf when in-repo
    Path(__file__).resolve().parent,         # engine unpack root (converge/)
)

# Kubelet args for air-gap / large-disk workstations:
#  - Absolute free-space thresholds (Gi) so a 900G disk at ~90% full still schedules
#    (percent-based soft/hard eviction would fire with tens of GiB free).
#  - Image GC raised so kubelet does not prune transported Layer-A images.
_KUBELET_GC_ARGS = (
    'eviction-hard=nodefs.available<5Gi,imagefs.available<5Gi,nodefs.inodesFree<1%,memory.available<100Mi',
    'eviction-soft=nodefs.available<10Gi,imagefs.available<10Gi,memory.available<200Mi',
    'eviction-soft-grace-period=nodefs.available=5m,imagefs.available=5m,memory.available=2m',
    'eviction-minimum-reclaim=nodefs.available=1Gi,imagefs.available=1Gi',
    'image-gc-high-threshold=100',
    'image-gc-low-threshold=99',
)

# Canonical absolute free-space floors (GiB) — match _KUBELET_GC_ARGS hard/soft.
# Hard = DiskPressure / NoSchedule; soft = grace before hard.
DEFAULT_EVICTION_HARD_GIB = 5.0
DEFAULT_EVICTION_SOFT_GIB = 10.0
# Soft grace default matches _KUBELET_GC_ARGS (5m). Wait budget is ALWAYS
# configured soft-grace (from rke2/kubelet config) + this lag — never invent
# multi-minute padding (converge-19: 420s was wrong).
DEFAULT_EVICTION_SOFT_GRACE_S = 300
DISK_PRESSURE_CONDITION_LAG_S = 10
DISK_PRESSURE_POLL_S = 15

# System namespaces we observe but never mutate (except never).
SYSTEM_NS_OBSERVE = (
    "kube-system",
    "kube-public",
    "kube-node-lease",
    "ingress-nginx",
    "cattle-system",
)


# --------------------------------------------------------------------------- #
# Kubelet image-GC policy
# --------------------------------------------------------------------------- #

def _read_rke2_config() -> str:
    try:
        return RKE2_CONFIG.read_text() if RKE2_CONFIG.is_file() else ""
    except OSError:
        return ""


def _canonical_kubelet_block() -> str:
    lines = [
        "# Dev / air-gap workstation: absolute free-space thresholds (not %).",
        "# Percent-based eviction on large disks triggers DiskPressure with tens",
        "# of GiB still free. Image GC raised so Layer-A images are not pruned.",
        "kubelet-arg:",
    ]
    for a in _KUBELET_GC_ARGS:
        lines.append(f'  - "{a}"')
    return "\n".join(lines) + "\n"


def kubelet_gc_policy_ok(text: Optional[str] = None) -> bool:
    """True if absolute eviction thresholds + raised image-GC are present."""
    raw = text if text is not None else _read_rke2_config()
    if not raw:
        return False
    m = re.search(r"image-gc-high-threshold[=:](\d+)", raw)
    if not m:
        return False
    try:
        if int(m.group(1)) < 99:
            return False
    except ValueError:
        return False
    # Prefer absolute Gi thresholds (dev large-disk posture).
    if "nodefs.available<5Gi" in raw:
        return True
    # Percent-based hard eviction: force upgrade to absolute Gi form.
    if "eviction-hard=" in raw and "Gi" not in raw:
        return False
    return False


def det_kubelet_gc_policy(_ctx: Ctx) -> Probe:
    """Layer-B node policy: default % eviction + 85% image-GC break air-gap stacks."""
    if not RKE2_CONFIG.parent.is_dir() and not Path("/etc/rancher").is_dir():
        return Probe(True, "not an RKE2 host path layout — kubelet GC N/A")
    raw = _read_rke2_config()
    if kubelet_gc_policy_ok(raw):
        return Probe(True, f"absolute eviction + image-GC raised in {RKE2_CONFIG}")
    if not raw:
        return Probe(False,
                     f"{RKE2_CONFIG} missing/empty — default kubelet eviction/GC will "
                     "DiskPressure / prune images under ordinary disk use")
    return Probe(False,
                 f"{RKE2_CONFIG} needs absolute free-space eviction (<5Gi hard) and "
                 "image-gc-high-threshold=100 — percent thresholds fire too early on large disks")


# --------------------------------------------------------------------------- #
# Disk free vs configured eviction thresholds + DiskPressure resolution
# --------------------------------------------------------------------------- #

def _parse_gib_threshold(text: str, key: str) -> Optional[float]:
    """Parse e.g. nodefs.available<5Gi from eviction-hard/soft string → 5.0."""
    m = re.search(rf"{re.escape(key)}<(\d+(?:\.\d+)?)\s*Gi", text, re.I)
    if m:
        return float(m.group(1))
    m = re.search(rf"{re.escape(key)}<(\d+(?:\.\d+)?)\s*G\b", text, re.I)
    if m:
        return float(m.group(1))
    return None


def configured_eviction_thresholds_gib(text: Optional[str] = None) -> Dict[str, float]:
    """Knowable min free space from on-disk kubelet config (or canonical defaults).

    Returns hard_gib / soft_gib — the floors under which DiskPressure is expected
    (hard) vs soft eviction grace (soft). Soft grace period seconds also returned.
    """
    raw = text if text is not None else _read_rke2_config()
    hard = DEFAULT_EVICTION_HARD_GIB
    soft = DEFAULT_EVICTION_SOFT_GIB
    grace_s = DEFAULT_EVICTION_SOFT_GRACE_S
    if raw:
        for label, store in (("eviction-hard", "hard"), ("eviction-soft", "soft")):
            m = re.search(rf"{label}=([^\s\"']+)", raw)
            if not m:
                # YAML list form: - "eviction-hard=..."
                m = re.search(rf'{label}=([^"\']+)', raw)
            if m:
                blob = m.group(1)
                vals = []
                for key in ("nodefs.available", "imagefs.available"):
                    g = _parse_gib_threshold(blob, key)
                    if g is not None:
                        vals.append(g)
                if vals:
                    if store == "hard":
                        hard = min(vals)
                    else:
                        soft = min(vals)
        # Prefer nodefs grace; fall back to imagefs or any available=Nm in the line
        gm = re.search(
            r"eviction-soft-grace-period=[^\n]*nodefs\.available=(\d+)m", raw)
        if not gm:
            gm = re.search(
                r"eviction-soft-grace-period=[^\n]*imagefs\.available=(\d+)m", raw)
        if not gm:
            gm = re.search(
                r"eviction-soft-grace-period=[^\n]*available=(\d+)m", raw)
        if gm:
            grace_s = int(gm.group(1)) * 60
        # seconds form: nodefs.available=300s
        gs = re.search(
            r"eviction-soft-grace-period=[^\n]*nodefs\.available=(\d+)s", raw)
        if gs:
            grace_s = int(gs.group(1))
    return {
        "hard_gib": hard,
        "soft_gib": soft,
        "soft_grace_s": grace_s,
        # Configured soft-grace only + fixed lag — no invented multi-minute pad
        "clear_wait_s": grace_s + DISK_PRESSURE_CONDITION_LAG_S,
    }


def df_available_gib(path: str) -> Optional[float]:
    """Free space on the filesystem containing path (GiB), or None if unreadable."""
    try:
        st = os.statvfs(path)
        return (st.f_bavail * st.f_frsize) / (1024 ** 3)
    except OSError:
        return None


def disk_free_census() -> Dict[str, object]:
    """df-equivalent free space on paths that drive nodefs/imagefs pressure."""
    paths = ["/", "/var/lib/rancher", "/var/tmp"]
    if REGISTRY_HOSTPATH.exists() or REGISTRY_HOSTPATH.parent.exists():
        paths.append(str(REGISTRY_HOSTPATH))
    per: Dict[str, float] = {}
    for p in paths:
        if Path(p).exists() or p in ("/", "/var/tmp"):
            g = df_available_gib(p if Path(p).exists() else "/")
            if g is not None:
                # dedupe same device
                per[p] = round(g, 2)
    # Minimum free across measured mounts (worst imagefs/nodefs proxy)
    min_free = min(per.values()) if per else None
    thr = configured_eviction_thresholds_gib()
    below_hard = (
        min_free is not None and min_free < thr["hard_gib"]
    )
    return {
        "mounts_gib": per,
        "min_free_gib": min_free,
        "hard_gib": thr["hard_gib"],
        "soft_gib": thr["soft_gib"],
        "clear_wait_s": thr["clear_wait_s"],
        "below_hard": below_hard,
        "summary": (
            f"df_min={min_free}Gi hard={thr['hard_gib']}Gi soft={thr['soft_gib']}Gi "
            f"mounts={per}"
        ),
    }


def _node_disk_pressure_true(ctx: Ctx) -> List[str]:
    """Nodes with DiskPressure condition True and/or disk-pressure taint."""
    out: List[str] = []
    for n in ctx.items("nodes"):
        name = (n.get("metadata") or {}).get("name") or "?"
        conds = {c.get("type"): c for c in (n.get("status") or {}).get("conditions") or []}
        dp = conds.get("DiskPressure") or {}
        if str(dp.get("status", "")).lower() == "true":
            out.append(name)
        for t in n.get("spec", {}).get("taints", []) or []:
            if t.get("key") == "node.kubernetes.io/disk-pressure":
                if name not in out:
                    out.append(name)
    return out


def rem_resolve_disk_pressure(ctx: Ctx) -> Fix:
    """FSM resolution for DiskPressure when free space is knowable.

    Policy (single-node air-gap, operator-owned):
      * df min free < hard GiB → MANUAL free disk (early return; never prune images)
      * df min free >= hard GiB → clear NoSchedule taint + uncordon
      * Wait budget is **only** configured soft-grace (from rke2/kubelet config
        eviction-soft-grace-period) **+ 10s** — never invent 420s or other padding
      * df min free >= soft GiB → wait at most the +10s lag (grace already satisfied
        by free space); then continue even if condition bit lags
      * hard <= free < soft → wait up to soft_grace_s + 10s
      * free drops below hard mid-wait → MANUAL

    Never prunes images. Does not delete Layer-A packages.
    """
    free = disk_free_census()
    thr = configured_eviction_thresholds_gib()
    pressured = _node_disk_pressure_true(ctx)
    if not pressured and not free.get("below_hard"):
        return Fix(False, f"no DiskPressure to resolve ({free['summary']})")

    if free.get("below_hard"):
        return Fix(False, _manual.join_detail(
            f"MANUAL: free disk below configured hard eviction "
            f"(min_free={free.get('min_free_gib')}Gi < hard={free.get('hard_gib')}Gi) "
            f"— {free['summary']}. Free space safely (journal vacuum, Failed pods); "
            f"NEVER crictl rmi --prune.",
            _manual.hint_for("T0.no-disk-pressure"),
        ))

    min_free = free.get("min_free_gib")
    soft = float(free.get("soft_gib") or thr["soft_gib"])
    grace_s = int(thr.get("soft_grace_s") or DEFAULT_EVICTION_SOFT_GRACE_S)
    lag_s = DISK_PRESSURE_CONDITION_LAG_S
    # Configured grace + fixed lag only (no multi-minute invention)
    clear_wait_s = int(thr.get("clear_wait_s") or (grace_s + lag_s))
    above_soft = min_free is not None and float(min_free) >= soft

    actions: List[str] = []
    if os.geteuid() != 0:
        return Fix(False, _manual.join_detail(
            f"MANUAL: free space OK ({free['summary']}) but need root to clear "
            f"disk-pressure taint on {pressured}",
            _manual.hint_for("T0.no-disk-pressure"),
        ))

    node_names = list(pressured) or [
        n for n in (
            (n.get("metadata") or {}).get("name")
            for n in ctx.items("nodes")
        ) if n
    ]
    for name in node_names:
        r = ctx.k([
            "taint", "nodes", name,
            "node.kubernetes.io/disk-pressure:NoSchedule-",
        ])
        if r.returncode == 0:
            actions.append(f"cleared disk-pressure taint on {name}")
        r2 = ctx.k(["uncordon", name])
        if r2.returncode == 0:
            actions.append(f"uncordoned {name}")

    # Wait: soft_grace (from config) + 10s. If df already >= soft, grace is
    # satisfied by free space — only the +10s lag applies.
    wait_s = lag_s if above_soft else clear_wait_s
    print(
        f"    DiskPressure wait budget={wait_s}s "
        f"(configured soft-grace={grace_s}s + lag={lag_s}s"
        f"{'; df>=soft → grace skipped' if above_soft else ''}; "
        f"df min={min_free}Gi soft={soft}Gi)",
        flush=True,
    )
    deadline = time.time() + wait_s
    while time.time() < deadline:
        still = _node_disk_pressure_true(ctx)
        if not still:
            for name in node_names:
                ctx.k([
                    "taint", "nodes", name,
                    "node.kubernetes.io/disk-pressure:NoSchedule-",
                ])
            return Fix(
                True,
                f"DiskPressure cleared after wait "
                f"({free['summary']}; wait={wait_s}s grace={grace_s}s+{lag_s}s; "
                f"actions={actions or ['none']})",
            )
        time.sleep(min(DISK_PRESSURE_POLL_S, max(1, wait_s // 3)))
        free = disk_free_census()
        if free.get("below_hard"):
            return Fix(False, _manual.join_detail(
                f"MANUAL: free space fell below hard during wait "
                f"({free['summary']})",
                _manual.hint_for("T0.no-disk-pressure"),
            ))
        mf = free.get("min_free_gib")
        # Crossed soft mid-wait → only remaining lag applies
        if (not above_soft and mf is not None
                and float(mf) >= float(free.get("soft_gib") or soft)):
            above_soft = True
            wait_s = lag_s
            deadline = time.time() + lag_s
            print(
                f"    df now >= soft ({free['summary']}) — remaining wait {lag_s}s only",
                flush=True,
            )

    still = _node_disk_pressure_true(ctx)
    free = disk_free_census()
    # Taint cleared + free still >= hard: continue; condition may lag
    if not free.get("below_hard") and actions:
        return Fix(
            True,
            f"waited {wait_s}s (grace={grace_s}s+{lag_s}s); df still >= hard "
            f"({free['summary']}); taint clear attempted; condition lag still={still} "
            f"— continuing  [actions={actions}]",
        )
    return Fix(False, _manual.join_detail(
        f"MANUAL: waited {wait_s}s (configured soft-grace={grace_s}s + {lag_s}s) "
        f"for DiskPressure clear; df={free['summary']}; still pressured={still}. "
        f"Free more disk (above soft={soft}Gi) or check kubelet if sticky.",
        _manual.hint_for("T0.no-disk-pressure"),
    ))


def rem_kubelet_gc_policy(_ctx: Ctx) -> Fix:
    """Write canonical lenient kubelet-arg block. Restarts rke2-server when root
    (does not disrupt running pods — containerd keeps them). MANUAL if not root."""
    if os.geteuid() != 0:
        return Fix(False, _manual.join_detail(
            f"MANUAL: as root, install absolute-threshold kubelet-arg block in "
            f"{RKE2_CONFIG} and `systemctl restart rke2-server`",
            _manual.hint_for("T0.kubelet-gc")))
    try:
        RKE2_CONFIG.parent.mkdir(parents=True, exist_ok=True)
        raw = _read_rke2_config()
        if kubelet_gc_policy_ok(raw):
            return Fix(False, "kubelet absolute-eviction + GC policy already applied")
        block = _canonical_kubelet_block()
        # Replace entire kubelet-arg: list(s) with the canonical block so we do not
        # leave duplicate keys (YAML last-wins) or stale percent thresholds.
        if re.search(r"(?m)^kubelet-arg:\s*$", raw):
            # Drop all kubelet-arg sections and any immediately following list items.
            cleaned = re.sub(
                r"(?ms)^kubelet-arg:\s*\n(?:[ \t]+-.*\n)*",
                "",
                raw,
            )
            new = cleaned.rstrip() + "\n\n" + block if cleaned.strip() else block
        else:
            new = (raw.rstrip() + "\n\n" + block) if raw.strip() else block
        RKE2_CONFIG.write_text(new)
        detail = f"wrote absolute-eviction kubelet policy to {RKE2_CONFIG}"
        r = subprocess.run(
            ["systemctl", "restart", "rke2-server"],
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode != 0:
            return Fix(True, f"{detail}; rke2-server restart rc={r.returncode} "
                             f"(policy on disk — restart manually if needed)")
        return Fix(True, f"{detail}; restarted rke2-server")
    except OSError as e:
        return Fix(False, _manual.join_detail(
            f"MANUAL: cannot write {RKE2_CONFIG}: {e}",
            _manual.hint_for("T0.kubelet-gc")))
    except subprocess.TimeoutExpired:
        return Fix(True, f"wrote GC policy; rke2-server restart still running")

# --------------------------------------------------------------------------- #
# IngressClass selection
# --------------------------------------------------------------------------- #

def detect_ingress_class(ctx: Ctx) -> str:
    """Prefer nginx (RKE2), then any non-empty IngressClass name, else nginx default."""
    classes = []
    for ic in ctx.items("ingressclass"):
        name = (ic.get("metadata") or {}).get("name")
        if name:
            classes.append(name)
    if "nginx" in classes:
        return "nginx"
    if "rke2-ingress-nginx" in classes:
        return "rke2-ingress-nginx"
    # Some installs use controller without IngressClass object — probe controller pods
    for ns, sel in (
        ("kube-system", "app.kubernetes.io/name=rke2-ingress-nginx"),
        ("kube-system", "app=rke2-ingress-nginx"),
        ("ingress-nginx", "app.kubernetes.io/name=ingress-nginx"),
        ("kube-system", "app.kubernetes.io/name=traefik"),
    ):
        ready, total = ctx.pods_ready(ns, sel)
        if total > 0:
            if "traefik" in sel:
                return "traefik"
            return "nginx"
    if classes:
        return classes[0]
    return "nginx"  # RKE2 resilient default even if class object not listed yet


def det_ingress_class_aligned(ctx: Ctx) -> Probe:
    """Managed Ingress objects should use a class the cluster actually serves."""
    want = detect_ingress_class(ctx)
    ings = ctx.items("ingress")
    if not ings:
        return Probe(True, f"no Ingress objects yet; preferred class={want}")
    bad = []
    for ing in ings:
        ns = (ing.get("metadata") or {}).get("namespace", "")
        name = (ing.get("metadata") or {}).get("name", "")
        if ns not in ("dask", "jupyterhub", "panel-viz"):
            continue
        cls = (ing.get("spec") or {}).get("ingressClassName") or ""
        if cls and cls != want and want == "nginx" and cls == "traefik":
            bad.append(f"{ns}/{name} class={cls!r}")
        elif cls and cls not in (want, "") and want != cls:
            # only flag traefik-on-rke2 classic misconfig strongly
            if cls == "traefik" and want == "nginx":
                bad.append(f"{ns}/{name} class={cls!r}")
    if bad:
        return Probe(False,
                     f"Ingress bound to wrong class (want {want}): {bad} — "
                     "RKE2 ships nginx; traefik default silently unbound all routes")
    return Probe(True, f"ingress class aligned (preferred={want})")


def rem_ingress_class_aligned(ctx: Ctx) -> Fix:
    """Redeploy ingress component with auto-detected INGRESS_CLASS."""
    cls = detect_ingress_class(ctx)
    ctx.s3["INGRESS_CLASS"] = cls
    from .catalog import _zarf_deploy_components
    fix = _zarf_deploy_components(ctx, "ingress")
    return Fix(fix.changed, f"INGRESS_CLASS={cls}; {fix.detail}")


# --------------------------------------------------------------------------- #
# Layer-A package uniqueness
# --------------------------------------------------------------------------- #

def find_deploy_packages() -> List[Path]:
    found: List[Path] = []
    for d in DEFAULT_PKG_DIRS:
        try:
            found.extend(sorted(d.glob("zarf-package-cybersec-dask-amd64-*.tar.zst")))
        except OSError:
            continue
    # de-dupe by resolved path
    seen = set()
    out = []
    for p in found:
        try:
            rp = p.resolve()
        except OSError:
            continue
        if rp not in seen:
            seen.add(rp)
            out.append(p)
    return out


def det_package_uniqueness(ctx: Ctx) -> Probe:
    """Multiple deploy packages → mtime discovery may pick the wrong version.

    When ``--package`` / converge-node argv2 is explicit and readable, siblings on
    disk are **not** a CLOSURE failure (converge-15): the operator pinned the kit;
    Layer-A conservation forbids us deleting the extras. Report OK with a warn
    detail so CONVERGED is possible while still naming the archive copies.
    """
    pkgs = find_deploy_packages()
    # Include the explicit package even if outside DEFAULT_PKG_DIRS
    # (operator-chosen path — any absolute/relative location they pass).
    if ctx.package_path:
        try:
            exp = Path(ctx.package_path).resolve()
        except OSError:
            exp = Path(ctx.package_path)
        if exp.is_file():
            pkgs = [p for p in pkgs if p.resolve() != exp] + [exp]
            # de-dupe preserving explicit last
            seen = set()
            uniq = []
            for p in pkgs:
                try:
                    rp = p.resolve()
                except OSError:
                    rp = p
                if rp not in seen:
                    seen.add(rp)
                    uniq.append(p)
            pkgs = uniq
        others = [p for p in pkgs if p.resolve() != exp]
        if others:
            # Explicit pin wins — do not fail converge (archive copies are OK)
            return Probe(
                True,
                f"explicit package {exp} (OK); {len(others)} other copy(ies) on disk "
                f"ignored for deploy: {[str(p) for p in others[:4]]} — optional: "
                f"mv extras to /var/tmp/pkg-archive/ to silence this note",
            )
        return Probe(True, f"explicit package {exp.name}")
    if len(pkgs) > 1:
        return Probe(False,
                     f"{len(pkgs)} deploy packages in search path "
                     f"({[p.name for p in pkgs[:6]]}) — keep only ONE version "
                     f"or pass an explicit package path as argv2 / --package")
    if len(pkgs) == 1:
        return Probe(True, f"single package {pkgs[0].name}")
    return Probe(True, "no package tarballs in default paths (engine may use --package)")


def rem_package_uniqueness(ctx: Ctx) -> Fix:
    """Cannot auto-delete packages (Layer-A). Report MANUAL only when ambiguous."""
    if ctx.package_path and Path(ctx.package_path).is_file():
        return Fix(False, "explicit --package set — uniqueness OK (no Layer-A delete)")
    pkgs = find_deploy_packages()
    names = ", ".join(str(p) for p in pkgs)
    return Fix(False, _manual.join_detail(
        f"MANUAL: leave only the intended package tarball on disk "
        f"(found: {names}), or re-run with explicit path: "
        f"converge-node.sh apply /var/tmp/zarf-package-….tar.zst. "
        f"Engine will not delete Layer-A artifacts.",
        _manual.hint_for("T0.package-uniqueness")))


# --------------------------------------------------------------------------- #
# System / RKE2 health (mostly discover + limited remediations)
# --------------------------------------------------------------------------- #

def discover_system_plane(ctx: Ctx) -> List[str]:
    """Read-only lines for discovery report about system plane."""
    lines: List[str] = []
    # API healthz
    r = ctx.k(["get", "--raw", "/healthz"])
    if r.returncode == 0 and (r.stdout or "").strip() == "ok":
        lines.append("API /healthz=ok")
    else:
        lines.append(f"API /healthz FAILED rc={r.returncode}")
    # Critical kube-system pods
    for sel, label in (
        ("component=kube-apiserver", "apiserver"),
        ("k8s-app=kube-dns", "coredns"),
        ("app.kubernetes.io/name=rke2-ingress-nginx", "ingress-nginx"),
        ("app.kubernetes.io/name=rke2-canal", "canal"),
        ("k8s-app=canal", "canal"),
    ):
        ready, total = ctx.pods_ready("kube-system", sel)
        if total == 0:
            continue
        lines.append(f"kube-system {label} ready {ready}/{total}")
        if ready < total:
            lines.append(f"WEDGE: {label} not fully Ready")
    # IngressClass inventory
    ics = [(ic.get("metadata") or {}).get("name") for ic in ctx.items("ingressclass")]
    ics = [n for n in ics if n]
    lines.append(f"IngressClass: {ics or '(none)'}; preferred={detect_ingress_class(ctx)}")
    # rke2-server unit (node-local)
    if Path("/etc/rancher/rke2").is_dir():
        try:
            r = subprocess.run(
                ["systemctl", "is-active", "rke2-server"],
                capture_output=True, text=True, timeout=10,
            )
            lines.append(f"rke2-server: {(r.stdout or r.stderr or '').strip() or r.returncode}")
        except (OSError, subprocess.TimeoutExpired):
            lines.append("rke2-server: status unavailable")
    return lines


def det_system_plane(ctx: Ctx) -> Probe:
    """Soft gate: API healthy; warn on degraded coredns/ingress but only FAIL API."""
    r = ctx.k(["get", "--raw", "/healthz"])
    if r.returncode != 0 or (r.stdout or "").strip() not in ("ok", "ok\n"):
        # some clusters return more than "ok"
        if r.returncode != 0:
            return Probe(False, f"API /healthz unreachable rc={r.returncode}")
    bits = discover_system_plane(ctx)
    wedges = [b for b in bits if b.startswith("WEDGE")]
    if wedges:
        # Degraded data plane / DNS — surface as not-ok so remediate can try uncordon etc.
        return Probe(False, "; ".join(bits))
    return Probe(True, "; ".join(bits[:4]))


def rem_system_plane(ctx: Ctx) -> Fix:
    """Safe remediations only: uncordon nodes; never restart etcd from here unless
    API is up. CoreDNS/ingress image issues are CLOSURE (Layer-A) MANUAL."""
    actions: List[str] = []
    # Uncordon any SchedulingDisabled nodes
    for n in ctx.items("nodes"):
        name = (n.get("metadata") or {}).get("name")
        spec = n.get("spec") or {}
        if spec.get("unschedulable") and name:
            if ctx.k(["uncordon", name]).returncode == 0:
                actions.append(f"uncordoned {name}")
    # If coredns ImagePullBackOff — cannot pull air-gap
    miss = ctx.pod_image_missing("kube-system", "k8s-app=kube-dns")
    if miss is True:
        head = (
            "MANUAL: coredns ImagePullBackOff — re-import RKE2 bundled images "
            "(Layer-A); engine will not pull"
            + (f"  [also: {'; '.join(actions)}]" if actions else "")
        )
        return Fix(False, _manual.join_detail(head, _manual.hint_for("T0.system-plane")))
    if actions:
        return Fix(True, "; ".join(actions))
    return Fix(False, _manual.join_detail(
        "system plane degraded — inspect kube-system pods; "
        "engine does not restart etcd/control-plane automatically"
        + (f"  [{'; '.join(discover_system_plane(ctx)[:3])}]"),
        _manual.hint_for("T0.system-plane")))


# --------------------------------------------------------------------------- #
# HostPath registry (shared prep)
# --------------------------------------------------------------------------- #

def ensure_registry_hostpath() -> List[str]:
    actions: List[str] = []
    try:
        REGISTRY_HOSTPATH.mkdir(parents=True, exist_ok=True)
        if os.geteuid() == 0:
            os.chmod(REGISTRY_HOSTPATH, 0o777)
            actions.append(f"chmod 0777 {REGISTRY_HOSTPATH}")
        probe = REGISTRY_HOSTPATH / ".converge-write-probe"
        probe.write_text("ok")
        try:
            probe.unlink(missing_ok=True)  # type: ignore[call-arg]
        except TypeError:
            probe.unlink()
        if not actions:
            actions.append(f"hostPath {REGISTRY_HOSTPATH} ready (writable)")
    except PermissionError:
        actions.append(
            f"MANUAL: mkdir -p {REGISTRY_HOSTPATH} && chmod 0777 {REGISTRY_HOSTPATH} (need root)")
    except OSError as e:
        actions.append(f"hostPath prepare: {e}")
    return actions

def ensure_ingress_class_in_ctx(ctx: Ctx) -> str:
    """Stamp detected class into ctx.s3 so all zarf deploys pick it up."""
    if ctx.s3.get("INGRESS_CLASS"):
        return ctx.s3["INGRESS_CLASS"]
    cls = detect_ingress_class(ctx)
    ctx.s3["INGRESS_CLASS"] = cls
    return cls
