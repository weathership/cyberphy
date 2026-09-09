"""Precise in-situ discovery + intervention recipes for converge MANUAL/FAILED.

Every recipe is copy-pasteable on an RKE2 control-plane node (typically root).
Format:

  DISCOVER:  read-only commands to confirm the failure mode
  FIX:       the exact intervention (matches what the engine runs when it can)

Layer-B app charts always FIX via ``zarf package deploy --components=…`` — the
same path that unblocks installs in situ. Surgical kubectl only where the engine
does surgical kubectl (registry PV, uncordon, SC annotation, worker cap).

Consumed by:
  * ``Invariant.manual_hint`` (catalog) — printed on MANUAL outcomes
  * ``_zarf_deploy_components`` / other rems — appended when auto-rem degrades
  * ``engine.report`` — multi-line IN SITU section for MANUAL + FAILED
"""
from __future__ import annotations

from typing import Iterable, List, Optional, Sequence

# --------------------------------------------------------------------------- #
# Formatting
# --------------------------------------------------------------------------- #

# Session preamble operators should have once; referenced by name, not re-printed
# on every invariant (keeps MANUAL lines scannable). Full block in SETUP_PREAMBLE.
SETUP_PREAMBLE = r"""# Session setup (once per shell) — RKE2 control plane
export KUBECONFIG=/etc/rancher/rke2/rke2.yaml
export PATH="$PATH:/var/lib/rancher/rke2/bin"   # zarf Helm actions need bare `kubectl`
kc()  { /var/lib/rancher/rke2/bin/kubectl --kubeconfig "$KUBECONFIG" "$@" 2>/dev/null \
        || zarf tools kubectl --kubeconfig "$KUBECONFIG" "$@"; }
cri() { /var/lib/rancher/rke2/bin/crictl --runtime-endpoint unix:///run/k3s/containerd/containerd.sock "$@"; }
# Package: beside script/CWD, /var/tmp, /opt/zarf (same search as converge)
PKG=$(ls -t zarf-package-cybersec-dask-amd64-*.tar.zst \
          /var/tmp/zarf-package-cybersec-dask-amd64-*.tar.zst \
          /opt/zarf/zarf-package-cybersec-dask-amd64-*.tar.zst 2>/dev/null | head -1)
echo "PKG=$PKG"; command -v zarf; command -v kubectl; kc get --raw /healthz
"""

# S3 env for any zarf package deploy that templates bucket/creds (panel/engine/dask).
S3_ENV_PREAMBLE = r"""# S3 for zarf deploy (secrets → ZARF_CONFIG tmpfs; non-secrets → --set-variables)
# set -a; . /dev/shm/s3-creds; set +a   # if you stage creds that way
umask 077; cat > /dev/shm/zarf-secrets.toml <<EOF
[package.deploy.set]
S3_ACCESS_KEY = "${S3_ACCESS_KEY}"
S3_SECRET_KEY = "${S3_SECRET_KEY}"
EOF
export ZARF_CONFIG=/dev/shm/zarf-secrets.toml
SETV=(--set-variables=S3_BUCKET="${S3_BUCKET}" --set-variables=S3_ENDPOINT="${S3_ENDPOINT}" --set-variables=S3_REGION="${S3_REGION:-us-east-1}")
# after deploys: shred -u /dev/shm/zarf-secrets.toml; unset ZARF_CONFIG
"""


def _lines(items: Sequence[str]) -> str:
    return "\n".join(f"  {x}" for x in items if x)


def block(
    discover: Sequence[str],
    fix: Sequence[str],
    *,
    note: str = "",
) -> str:
    """Multi-line DISCOVER/FIX recipe (indented for report nesting)."""
    parts: List[str] = []
    if note:
        parts.append(note.rstrip())
    if discover:
        parts.append("DISCOVER:")
        parts.append(_lines(discover))
    if fix:
        parts.append("FIX:")
        parts.append(_lines(fix))
    return "\n".join(parts)


def join_detail(detail: str, recipe: str) -> str:
    """Append recipe to a probe/fix detail if not already present.

    Skips when ``detail`` already embeds a DISCOVER/FIX block (e.g. zarf deploy
    failures already attach ``zarf_deploy_recipe``) so report does not double-print.
    """
    detail = (detail or "").rstrip()
    recipe = (recipe or "").rstrip()
    if not recipe:
        return detail
    if "DISCOVER:" in detail and "FIX:" in detail:
        return detail
    if recipe in detail:
        return detail
    first = recipe.splitlines()[0] if recipe else ""
    if first and first in detail and "DISCOVER:" in recipe:
        # same note already glued once
        return detail
    if not detail:
        return recipe
    return f"{detail}\n{recipe}"


# --------------------------------------------------------------------------- #
# Component map — SSOT for zarf package deploy --components=
# --------------------------------------------------------------------------- #

# component list → primary namespace(s) + discovery kubectl lines
_COMPONENT_DISCOVER = {
    "cybersec-images": [
        "zarf tools registry catalog 2>/dev/null | head -40",
        "zarf tools registry catalog 2>/dev/null | grep -E 'cybersec-dask|panel' || true",
        "kc get pods -A | grep -E 'ImagePull|ErrImage' || true",
    ],
    "dask-operator": [
        "kc get ns dask-operator 2>/dev/null; kc -n dask-operator get pods,deploy -o wide",
        "kc get crd | grep -i dask || true",
        "kc get secrets -A -l owner=helm | grep -i dask || true",
    ],
    "dask-cluster": [
        "kc get ns dask 2>/dev/null; kc -n dask get pods,deploy,daskcluster -o wide 2>/dev/null",
        "kc -n dask describe daskcluster cybersec-dask 2>/dev/null | tail -40",
        "kc get secrets -A -l owner=helm | grep -E 'dask|cluster' || true",
    ],
    "panel-viz": [
        "kc get ns panel-viz 2>/dev/null; kc -n panel-viz get pods,deploy,svc,ep -o wide",
        "kc -n panel-viz get pods -l app=otel-navigator -o jsonpath='{range .items[*]}{.metadata.name} phase={.status.phase} ready={.status.containerStatuses[*].ready} restarts={.status.containerStatuses[*].restartCount}{\"\\n\"}{end}'",
        "kc -n panel-viz describe pod -l app=otel-navigator 2>/dev/null | sed -n '/Events:/,$p' | tail -20",
        "kc -n panel-viz logs deploy/otel-navigator -c otel-navigator --tail=40 2>/dev/null",
        "kc -n panel-viz logs deploy/otel-navigator -c pty-proxy --tail=20 2>/dev/null",
        # blank S3 bricks UI even when TCP Ready
        "kc -n panel-viz get cm otel-navigator-config -o jsonpath='S3_BUCKET={.data.S3_BUCKET}{\"\\n\"}OTEL_DATA_PATH={.data.OTEL_DATA_PATH}{\"\\n\"}'",
        "kc -n panel-viz get secret otel-navigator-credentials "
        "-o jsonpath='cred keys present: {range .data.*}{\"·\"}{end}{\"\\n\"}' 2>/dev/null",
        "kc -n panel-viz get deploy vpc-flow-generator -o wide 2>/dev/null || true",
    ],
    "navigator-engine": [
        "kc -n panel-viz get pods,deploy,ep -l app=navigator-engine -o wide 2>/dev/null",
        "kc -n panel-viz describe pod -l app=navigator-engine 2>/dev/null | sed -n '/Events:/,$p' | tail -15",
        "kc -n panel-viz logs deploy/navigator-engine --tail=40 2>/dev/null",
        "kc -n panel-viz get cm otel-navigator-config -o jsonpath='S3_BUCKET={.data.S3_BUCKET} path={.data.OTEL_DATA_PATH}{\"\\n\"}'",
    ],
    "jupyterhub": [
        "kc get ns jupyterhub -o yaml 2>/dev/null | head -30",
        "kc -n jupyterhub get pods,deploy,svc,pvc -o wide 2>/dev/null",
        "kc -n jupyterhub get pods -l component=hub -o yaml 2>/dev/null | head -80",
        "kc get secrets -A -l owner=helm | grep -i jupyter || true",
    ],
    "sample-notebooks": [
        "kc -n jupyterhub get cm sample-notebooks -o yaml 2>/dev/null | head -40",
        "kc -n jupyterhub get cm 2>/dev/null",
    ],
    "ingress": [
        "kc get ingress -A; kc get ingressclass",
        "kc -n panel-viz get ingress panel-viz -o yaml 2>/dev/null | head -60",
        "kc -n dask get ingress 2>/dev/null",
    ],
}


def _discover_for_components(components: str) -> List[str]:
    seen = set()
    out: List[str] = [
        'echo "PKG=$PKG"; ls -la "$PKG" 2>/dev/null || ls -lt zarf-package-cybersec-dask-amd64-*.tar.zst /var/tmp/zarf-package-cybersec-dask-amd64-*.tar.zst 2>/dev/null | head -5',
        "command -v zarf; zarf version 2>&1 | head -3",
        "command -v kubectl; kc get --raw /healthz",
    ]
    for c in components.split(","):
        c = c.strip()
        for line in _COMPONENT_DISCOVER.get(c, []):
            if line not in seen:
                seen.add(line)
                out.append(line)
    # Dead / pending helm (field wedges)
    out.append(
        "kc get secrets -A -l owner=helm "
        "-o custom-columns='NS:.metadata.namespace,NAME:.metadata.name,"
        "REL:.metadata.labels.name,VER:.metadata.labels.version,"
        "ST:.metadata.labels.status' 2>/dev/null | head -40"
    )
    return out


def zarf_deploy_recipe(
    components: str,
    *,
    pkg: str = "$PKG",
    needs_s3: bool = False,
    note: str = "",
) -> str:
    """Full DISCOVER + FIX for ``zarf package deploy --components=…``."""
    needs_s3 = needs_s3 or any(
        x in components for x in ("panel-viz", "navigator-engine", "dask-cluster")
    )
    discover = _discover_for_components(components)
    fix: List[str] = [
        'export KUBECONFIG="${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}"',
        'export PATH="$PATH:/var/lib/rancher/rke2/bin"',
        f'PKG="${{PKG:-{pkg}}}"; test -f "$PKG" || {{ echo "package missing"; exit 1; }}',
    ]
    if needs_s3:
        fix.append(
            "# ensure S3_BUCKET/ENDPOINT/REGION + ZARF_CONFIG secrets (see S3_ENV_PREAMBLE)"
        )
        fix.append(
            'test -n "${S3_BUCKET:-}" || { echo "S3_BUCKET required for this component set"; exit 1; }'
        )
        fix.append(
            f'zarf package deploy "$PKG" --confirm --components={components} '
            '--retries 10 "${SETV[@]}"'
        )
    else:
        fix.append(
            f'zarf package deploy "$PKG" --confirm --components={components} --retries 10'
        )
    fix.append(
        "# if error contains 'has no deployed releases':"
    )
    fix.append(
        "#   zarf package remove \"$PKG\" --confirm --components=<dead-from-error>"
    )
    fix.append("#   then re-run the deploy line above")
    return block(
        discover,
        fix,
        note=note
        or f"Manual mirror of engine rem: zarf package deploy --components={components}",
    )


def zarf_deploy_manual_line(components: str, *, reason: str = "") -> str:
    """One-liner MANUAL prefix + multi-line recipe (for Fix.detail)."""
    head = f"MANUAL: zarf package deploy --components={components}"
    if reason:
        head = f"{head} — {reason}"
    return join_detail(head, zarf_deploy_recipe(components))


# --------------------------------------------------------------------------- #
# Per-invariant recipes (catalog manual_hint SSOT)
# --------------------------------------------------------------------------- #

HINTS = {
    "T0.api": block(
        [
            "systemctl is-active rke2-server; systemctl status rke2-server --no-pager | head -25",
            "journalctl -u rke2-server -n 40 --no-pager",
            "ls -la /etc/rancher/rke2/rke2.yaml; ss -lntp | grep -E '6443|9345' || true",
            "export KUBECONFIG=/etc/rancher/rke2/rke2.yaml; "
            "/var/lib/rancher/rke2/bin/kubectl get --raw /healthz",
        ],
        [
            "# if unit failed: journalctl -xe -u rke2-server",
            "systemctl restart rke2-server   # last resort; pods keep running under containerd",
            "# wait: until kc get --raw /healthz | grep -q ok; do sleep 5; done",
        ],
        note="Kubernetes API unreachable — control plane / kubeconfig",
    ),
    "T0.node-ready": block(
        [
            "kc get nodes -o wide",
            "kc describe node | grep -E 'Taints:|Unschedulable|Ready|Memory|Disk' | head -40",
        ],
        [
            "kc get nodes -o jsonpath='{range .items[*]}{.metadata.name}{\" \"}"
            "{.spec.unschedulable}{\"\\n\"}{end}'",
            "kc uncordon <node-name>   # engine: _rem_node_ready",
        ],
        note="Node NotReady or cordoned",
    ),
    "T0.system-plane": block(
        [
            "kc get --raw /healthz; kc get nodes; kc -n kube-system get pods -o wide",
            "kc -n kube-system get pods -l k8s-app=kube-dns",
            "kc get ingressclass; systemctl is-active rke2-server",
        ],
        [
            "kc get nodes -o jsonpath='{range .items[?(@.spec.unschedulable)]}{.metadata.name}{\"\\n\"}{end}' "
            "| xargs -r -n1 kc uncordon",
            "# ImagePullBackOff on coredns: re-import RKE2 bundled images (Layer A) — never pull",
            "ls /var/lib/rancher/rke2/agent/images/; cri images | grep -i coredns || true",
        ],
        note="System plane degraded (API/DNS/ingress observed)",
    ),
    "T0.kubelet-gc": block(
        [
            "grep -nE 'kubelet-arg|image-gc|eviction' /etc/rancher/rke2/config.yaml || true",
            "df -h /var/lib/rancher /var/tmp",
        ],
        [
            "# as root — absolute free-space thresholds + raised image-GC (air-gap)",
            "cat >> /etc/rancher/rke2/config.yaml <<'EOF'",
            "kubelet-arg:",
            '  - "eviction-hard=nodefs.available<5Gi,imagefs.available<5Gi,nodefs.inodesFree<1%,memory.available<100Mi"',
            '  - "image-gc-high-threshold=100"',
            '  - "image-gc-low-threshold=99"',
            "EOF",
            "# merge if kubelet-arg: already exists (YAML last-wins)",
            "systemctl restart rke2-server",
        ],
        note="Kubelet % eviction / 85% image-GC prunes Layer-A images",
    ),
    "T0.no-disk-pressure": block(
        [
            "kc get nodes -o custom-columns=NAME:.metadata.name,TAINTS:.spec.taints",
            "kc get nodes -o jsonpath='{range .items[*]}{.metadata.name}{\" DiskPressure=\"}"
            "{range .status.conditions[?(@.type==\"DiskPressure\")]}{.status}{\" \"}{.message}{end}{\"\\n\"}{end}'",
            "df -h / /var/lib/rancher /var/tmp; du -sh /var/log /var/tmp/* 2>/dev/null | sort -h | tail",
        ],
        [
            "# NEVER: crictl rmi --prune / ctr images rm (destroys Layer-A, cannot re-fetch)",
            "journalctl --vacuum-size=200M",
            "kc get pods -A --field-selector=status.phase=Failed -o name | xargs -r -n1 kc delete",
            "# optional: remove a *redundant* package tarball copy only",
            "# Wait until condition DiskPressure=False (not only taint removed); then:",
            "#   NODE=$(kc get nodes -o jsonpath='{.items[0].metadata.name}')",
            "#   kc taint nodes $NODE node.kubernetes.io/disk-pressure:NoSchedule- 2>/dev/null || true",
            "#   kc uncordon $NODE 2>/dev/null || true",
            "# Also ensure T0.kubelet-gc absolute free-space thresholds on large disks",
        ],
        note="DiskPressure: MANUAL only if df < configured hard GiB (default 5Gi from "
             "kubelet eviction-hard). If df >= hard, FSM clears taint and waits "
             "configured soft-grace + 10s only "
             "for condition False, then continues.",
    ),
    "T0.layer-a-zarf-tools": block(
        [
            "command -v zarf; ls -la $(command -v zarf) 2>/dev/null; zarf version 2>&1 | head -5",
            "ls -lt zarf-init-*.tar.zst /var/tmp/zarf-init-*.tar.zst 2>/dev/null | head",
            "ls -lt zarf-package-cybersec-dask-amd64-*.tar.zst /var/tmp/zarf-package-*.tar.zst 2>/dev/null | head",
        ],
        [
            "# Transport from release media (never download air-gapped):",
            "install -m 0755 ./zarf /usr/local/bin/zarf   # v0.70.x matching package",
            "cp zarf-init-amd64-v0.70*.tar.zst /var/tmp/  # beside deploy package",
            "cp zarf-package-cybersec-dask-amd64-*.tar.zst /var/tmp/",
        ],
        note="Layer-A CLOSURE: zarf binary + zarf-init package must be on the node",
    ),
    "T0.package-uniqueness": block(
        [
            "ls -lt /var/tmp/zarf-package-cybersec-dask-amd64-*.tar.zst "
            "./zarf-package-cybersec-dask-amd64-*.tar.zst /opt/zarf/zarf-package-*.tar.zst 2>/dev/null",
        ],
        [
            "# Keep ONLY the intended version (engine never deletes Layer-A tarballs)",
            "mkdir -p /var/tmp/pkg-archive && mv /var/tmp/zarf-package-cybersec-dask-amd64-<old>.tar.zst /var/tmp/pkg-archive/",
        ],
        note="Multiple deploy packages → mtime discovery may pick the wrong one",
    ),
    "T0.layer-a-images": block(
        [
            "kc -n local-path-storage get pods -o wide 2>/dev/null",
            "kc -n local-path-storage describe pod -l app=local-path-provisioner 2>/dev/null | tail -30",
            "cri images | grep -Ei 'local-path|busybox' || true",
        ],
        [
            "# RE-IMPORT from media / RKE2 bundled images — NEVER pull",
            "ls /var/lib/rancher/rke2/agent/images/",
            "# cp <image>.tar /var/lib/rancher/rke2/agent/images/ && systemctl restart rke2-server",
            "# or: ctr -a /run/k3s/containerd/containerd.sock -n k8s.io images import <image>.tar",
        ],
        note="Layer-A: bootstrap image absent (ImagePullBackOff)",
    ),
    "T0.5.sc-default": block(
        [
            "kc get sc; kc get sc -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\\.kubernetes\\.io/is-default-class==\"true\")]}{.metadata.name}{\"\\n\"}{end}'",
            "kc -n local-path-storage get pods 2>/dev/null",
        ],
        [
            "kc get sc local-path && kc patch sc local-path -p "
            '\'{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}\'',
            "# else apply bundled manifest: kc apply -f manifests/local-path-provisioner.yaml",
        ],
        note="No default StorageClass (dynamic-provisioning modality only)",
    ),
    "T0.5.provisioner": block(
        [
            "kc -n local-path-storage get pods,deploy -o wide",
            "kc -n local-path-storage describe pod -l app=local-path-provisioner | tail -40",
        ],
        [
            "kc apply -f manifests/local-path-provisioner.yaml   # node-preloaded image",
            "kc -n local-path-storage rollout status deploy/local-path-provisioner",
        ],
    ),
    "T0.5.registry-pv": block(
        [
            "ls -lad /var/lib/zarf-registry; stat -c '%a %U:%G' /var/lib/zarf-registry 2>/dev/null",
            "kc get pv zarf-registry-pv -o yaml 2>/dev/null | head -40",
            "kc -n zarf get pvc 2>/dev/null",
        ],
        [
            "mkdir -p /var/lib/zarf-registry && chmod 0777 /var/lib/zarf-registry",
            "# apply claimRef hostPath PV storageClassName:\"\" (see AIRGAP-REMEDIATION-COMMANDS T0.5)",
            "kc apply -f - <<'YAML'",
            "apiVersion: v1",
            "kind: PersistentVolume",
            "metadata: {name: zarf-registry-pv}",
            "spec:",
            "  capacity: {storage: 5Gi}",
            "  accessModes: [ReadWriteOnce]",
            "  persistentVolumeReclaimPolicy: Retain",
            "  storageClassName: \"\"",
            "  hostPath: {path: /var/lib/zarf-registry, type: DirectoryOrCreate}",
            "  claimRef: {namespace: zarf, name: zarf-docker-registry}",
            "YAML",
        ],
        note="Resilient path: static claimRef PV (no default SC)",
    ),
    "T1.registry-running": block(
        [
            "kc get ns zarf -o yaml 2>/dev/null | head -25",
            "kc -n zarf get pods,deploy,svc,pvc -o wide 2>/dev/null",
            "kc get pv zarf-registry-pv; ls -lad /var/lib/zarf-registry; touch /var/lib/zarf-registry/.w && rm -f /var/lib/zarf-registry/.w",
            "kc get secrets -A -l owner=helm | grep -i zarf || true",
            "kc get ns -L zarf.dev/agent | grep -E 'dask|panel|jupyter' || true",
        ],
        [
            "mkdir -p /var/lib/zarf-registry && chmod 0777 /var/lib/zarf-registry",
            "# prefer: re-run converge --apply (engine _pre_init_cleanup + zarf init)",
            "cd \"$(dirname \"$PKG\")\" && zarf init --confirm --storage-class=- "
            "--set=REGISTRY_PVC_SIZE=5Gi",
            "# seed-registry deadline / partial: delete pending helm secrets in zarf, re-init",
            "# agent poison: kc label ns <app> zarf.dev/agent- --overwrite",
        ],
        note="zarf init / internal registry + agent-hook rewriting",
    ),
    "T2.images-pushed": zarf_deploy_recipe(
        "cybersec-images",
        note="App images missing from internal registry (or ImagePullBackOff)",
    ),
    "T3.dask-operator": zarf_deploy_recipe(
        "dask-operator",
        note="Dask operator / CRDs absent or not Ready",
    ),
    "T4.scheduler": block(
        [
            "kc -n dask get pods,daskcluster -o wide",
            "kc -n dask describe pod -l dask.org/component=scheduler 2>/dev/null | sed -n '/Events:/,$p' | tail -30",
            "kc get nodes -o jsonpath='{range .items[*]}{.metadata.name}{\" DiskPressure=\"}"
            "{range .status.conditions[?(@.type==\"DiskPressure\")]}{.status}{end}{\"\\n\"}{end}'",
            "kc -n zarf get pods -o wide; zarf tools registry catalog 2>/dev/null | head -20",
            "kc -n dask-operator get pods -o wide",
        ],
        [
            "# ── A) DiskPressure / taint — NO redeploy (see T0.no-disk-pressure) ──",
            "# Wait DiskPressure=False; clear taint; uncordon",
            "# ── B) Node schedulable + scheduler Pending/CrashLoop — recycle ──",
            "kc -n dask delete pod -l dask.org/component=scheduler "
            "--force --grace-period=0 --wait=false 2>/dev/null || true",
            "# ── C) ImagePull — push app image then recycle ──",
            'zarf package deploy "$PKG" --confirm --components=cybersec-images --retries 5',
            "kc -n dask delete pod -l dask.org/component=scheduler --force --grace-period=0 2>/dev/null || true",
            "# ── D) CR / cluster missing — package path (after-action wait may be 900s) ──",
            'zarf package deploy "$PKG" --confirm --components=dask-cluster --retries 5 "${SETV[@]}"',
            "# If only after-action wait timed out but CR exists: check pods; recycle; skip full redeploy",
        ],
        note="scheduler: census node+registry+pod first. Recycle when schedulable; "
             "zarf dask-cluster only if CR/pods absent. Wait timeout ≠ missing objects.",
    ),
    "T0.package-uniqueness": block(
        [
            "ls -lt ./zarf-package-cybersec-dask-amd64-*.tar.zst "
            "/var/tmp/zarf-package-cybersec-dask-amd64-*.tar.zst 2>/dev/null",
            "# package path is always operator-chosen — never assumed by the script",
        ],
        [
            "# Pin the kit explicitly (path you control):",
            'sudo ./converge-node.sh apply /path/you/chose/zarf-package-cybersec-dask-amd64-1.6.6.tar.zst',
            "# Or place the package next to converge-node.sh / in CWD and omit argv2",
            "# Optional: archive extras (engine never deletes Layer-A packages)",
        ],
        note="Multiple packages only matter without --package (discovery). "
             "Explicit argv2/--package pins the kit; siblings are archive OK.",
    ),
    "T4.workers-capacity": block(
        [
            "kc get nodes -o custom-columns=NAME:.metadata.name,CPU:.status.allocatable.cpu,"
            "MEM:.status.allocatable.memory,SCHED:.spec.unschedulable",
            "kc -n dask get daskcluster cybersec-dask -o jsonpath='"
            "{.spec.worker.replicas}{\" replicas\\n\"}"
            "{.spec.worker.spec.containers[0].args}{\"\\n\"}"
            "{.spec.worker.spec.containers[0].resources}{\"\\n\"}'",
            "kc -n dask get deploy,pods -l dask.org/component=worker -o wide",
            "kc -n dask get pods -l dask.org/component=worker "
            "--field-selector=status.phase=Pending -o wide",
        ],
        [
            "# Engine 0.5.0: surgical CR patch + worker bounce (no zarf re-push).",
            "# Canonical: DASK_WORKER_REPLICAS / NTHREADS / CPU / MEMORY",
            "# Aliases:   DASK_WORKER_MEM_LIMIT→MEMORY, MEM_REQUEST→requests.memory",
            "export DASK_WORKER_REPLICAS=${DASK_WORKER_REPLICAS:-4}",
            "export DASK_WORKER_NTHREADS=${DASK_WORKER_NTHREADS:-2}",
            "export DASK_WORKER_CPU=${DASK_WORKER_CPU:-2}",
            "export DASK_WORKER_MEMORY=${DASK_WORKER_MEMORY:-6Gi}",
            "# Cap by RAM: floor((total_alloc_Gi − 8) / worker_Gi); Pending shrinks further",
            "kc -n dask patch daskcluster cybersec-dask --type merge -p \"{\\\"spec\\\":{"
            "\\\"worker\\\":{\\\"replicas\\\":${DASK_WORKER_REPLICAS}}}}\"",
            "# Full sizing (replicas + args + limits) — prefer converge apply:",
            "#   DASK_WORKER_*=… python3 -m converge --apply …",
            "# Manual template edit: get CR, set --nthreads / --memory-limit / limits, apply;",
            "# then bounce (operator often skips pod roll on template-only changes):",
            "kc -n dask delete pod -l dask.org/component=worker "
            "--force --grace-period=0 --wait=false",
            "# Reap excess worker Deployments (Pending / least-ready first):",
            "kc -n dask get deploy -l dask.org/component=worker "
            "--sort-by=.status.readyReplicas -o name | "
            "head -n -${DASK_WORKER_REPLICAS} | xargs -r -n1 kc -n dask delete --wait=false",
        ],
        note="Workers Pending or sizing drift (replicas/nthreads/cpu/memory) — "
             "surgical CR patch; strands panel if oversubscribed",
    ),
    "T5.otel-navigator": block(
        _discover_for_components("cybersec-images,panel-viz") + [
            "kc get pods -A --field-selector=status.phase=Pending -o wide | head -20",
            "kc -n dask get pods -l dask.org/component=worker --field-selector=status.phase=Pending 2>/dev/null | head",
            "curl -sS -o /dev/null -w 'nodeport30506:%{http_code}\\n' --connect-timeout 3 "
            "http://127.0.0.1:30506/otel-navigator || true",
            "bash zarf/scripts/verify-s3-datapath.sh --allow-empty 2>/dev/null || true",
        ],
        [
            'export KUBECONFIG="${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}"',
            'export PATH="$PATH:/var/lib/rancher/rke2/bin"',
            'test -n "${S3_BUCKET:-}" || { echo "S3_BUCKET required"; exit 1; }',
            "# ── A) CONFIG-ONLY (Deployments already exist; blank/wrong S3) ──",
            "# Preferred when verify LIVE STATE says 'recommended: config-only rem'.",
            "# Engine does this on --apply when S3_* given + deploys present.",
            "kc -n panel-viz patch cm otel-navigator-config --type merge -p \"{\\\"data\\\":{"
            "\\\"S3_BUCKET\\\":\\\"${S3_BUCKET}\\\","
            "\\\"OTEL_DATA_PATH\\\":\\\"s3://${S3_BUCKET}/\\\","
            "\\\"AWS_REGION\\\":\\\"${S3_REGION:-us-east-1}\\\"}}\"",
            "kc -n panel-viz patch secret otel-navigator-credentials --type merge -p \"{\\\"stringData\\\":{"
            "\\\"AWS_ACCESS_KEY_ID\\\":\\\"${S3_ACCESS_KEY}\\\","
            "\\\"AWS_SECRET_ACCESS_KEY\\\":\\\"${S3_SECRET_KEY}\\\","
            "\\\"S3_ENDPOINT\\\":\\\"${S3_ENDPOINT:-}\\\"}}\"",
            "kc -n panel-viz rollout restart deploy/otel-navigator deploy/navigator-engine 2>/dev/null || true",
            "kc -n panel-viz rollout status deploy/otel-navigator --timeout=180s",
            "# ── B) PACKAGE path (missing ns/deploy, ImagePull, tag drift) ──",
            'PKG="${PKG:-$PKG}"; test -f "$PKG" && zarf package deploy "$PKG" --confirm '
            '--components=cybersec-images,panel-viz --retries 10 "${SETV[@]}"',
            "# ── C) capacity if Pending ──",
            "TARGET=$(( $(kc get nodes --no-headers 2>/dev/null | wc -l) - 1 )); "
            "[ \"${TARGET:-1}\" -lt 1 ] && TARGET=1",
            "kc -n dask patch daskcluster cybersec-dask --type merge "
            "-p \"{\\\"spec\\\":{\\\"worker\\\":{\\\"replicas\\\":$TARGET}}}\" 2>/dev/null || true",
            "kc -n panel-viz delete pod -l app=otel-navigator --force --grace-period=0 --wait=false",
            "# ── D) datapath (spans already in bucket) ──",
            "bash zarf/scripts/verify-s3-datapath.sh",
        ],
        note="otel-navigator: prefer CONFIG-ONLY (patch CM/Secret + rollout) when deploys "
             "exist; zarf package deploy when missing/ImagePull/drift. verify prints LIVE STATE.",
    ),
    "T5.navigator-engine": block(
        _discover_for_components("cybersec-images,navigator-engine"),
        [
            'export KUBECONFIG="${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}"',
            'export PATH="$PATH:/var/lib/rancher/rke2/bin"',
            'test -n "${S3_BUCKET:-}" || { echo "S3_BUCKET required (shared panel CM)"; exit 1; }',
            "# If otel-navigator-config has empty S3_BUCKET, redeploy panel-viz FIRST "
            "(engine envFrom that ConfigMap/Secret)",
            'zarf package deploy "$PKG" --confirm --components=cybersec-images,panel-viz '
            '--retries 10 "${SETV[@]}"',
            'zarf package deploy "$PKG" --confirm --components=cybersec-images,navigator-engine '
            '--retries 10 "${SETV[@]}"',
            "kc -n panel-viz delete pod -l app=navigator-engine --force --grace-period=0 --wait=false",
            "kc -n panel-viz get pods,ep -l app=navigator-engine -o wide",
        ],
        note="navigator-engine not Ready — redeploy; shared S3 config via panel-viz",
    ),
    "T5.s3-datapath": block(
        [
            "kc -n panel-viz get cm otel-navigator-config "
            "-o jsonpath='S3_BUCKET={.data.S3_BUCKET}{\"\\n\"}OTEL_DATA_PATH={.data.OTEL_DATA_PATH}{\"\\n\"}'",
            "kc -n panel-viz get secret otel-navigator-credentials >/dev/null && echo creds:present",
            "# Full probe (reads ConfigMap + exec into app/scheduler — no secrets on argv):",
            "bash zarf/scripts/verify-s3-datapath.sh",
            "bash zarf/scripts/verify-s3-datapath.sh --json   # machine-readable",
            "# Auth only (bucket empty OK): bash zarf/scripts/verify-s3-datapath.sh --allow-empty",
        ],
        [
            "# If blank S3 in ConfigMap → redeploy panel-viz with SETV/ZARF_CONFIG (T5.otel-navigator)",
            "# If auth fails (403/InvalidAccessKey) → fix S3_* and redeploy panel-viz + dask-cluster",
            "# If marker/parquet missing — data is operator-provided; seed in-cluster:",
            "#   JupyterHub → OTEL_Data_Generator.ipynb (sample-notebooks) run-all",
            "#   or: zarf/scripts/generate-otel-data.py from a pod with S3 env",
            "# Re-check:",
            "bash zarf/scripts/verify-s3-datapath.sh && echo datapath OK",
        ],
        note="S3 datapath: ConfigMap bucket must be reachable from the app and "
             "_active_dataset.json + span parquet must be readable (spans already in place).",
    ),
    "T5.jupyterhub": block(
        [
            "kc get ns jupyterhub 2>/dev/null; kc -n jupyterhub get pods,deploy,svc,pvc -o wide",
            "kc -n jupyterhub get pods -l 'component in (hub,proxy)' "
            "--field-selector=status.phase=Pending -o wide 2>/dev/null",
            "kc -n jupyterhub describe pod -l component=hub 2>/dev/null | sed -n '/Events:/,$p' | tail -25",
            "kc get nodes -o jsonpath='{range .items[*]}{.metadata.name}{\" Ready=\"}"
            "{range .status.conditions[?(@.type==\"Ready\")]}{.status}{end}"
            "{\" DiskPressure=\"}{range .status.conditions[?(@.type==\"DiskPressure\")]}{.status}{end}"
            "{\"\\n\"}{end}'",
            "kc -n dask get pods -l dask.org/component=worker -o wide 2>/dev/null | head",
            "kc -n jupyterhub get cm sample-notebooks 2>/dev/null | head",
        ],
        [
            "# ── A) Node not schedulable (DiskPressure / taint / cordon) — NO zarf redeploy ──",
            "# Wait until DiskPressure=False; then clear taint + uncordon (see T0.no-disk-pressure)",
            "# ── B) Node schedulable + hub/proxy Pending — recycle only (engine does this) ──",
            "kc -n jupyterhub delete pod -l component=hub --field-selector=status.phase=Pending "
            "--force --grace-period=0 --wait=false 2>/dev/null || true",
            "kc -n jupyterhub delete pod -l component=proxy --field-selector=status.phase=Pending "
            "--force --grace-period=0 --wait=false 2>/dev/null || true",
            "kc -n jupyterhub get pods -o wide",
            "# ── C) Insufficient CPU/memory — cap Dask workers, then recycle hub/proxy ──",
            "TARGET=$(( $(kc get nodes --no-headers 2>/dev/null | wc -l) - 1 )); "
            "[ \"${TARGET:-1}\" -lt 1 ] && TARGET=1",
            "kc -n dask patch daskcluster cybersec-dask --type merge "
            "-p \"{\\\"spec\\\":{\\\"worker\\\":{\\\"replicas\\\":$TARGET}}}\" 2>/dev/null || true",
            "# ── D) Namespace/deploy missing or ImagePull — package path ──",
            'PKG="${PKG:-$PKG}"; test -f "$PKG" || { echo "package missing"; exit 1; }',
            'zarf package deploy "$PKG" --confirm --components=jupyterhub,sample-notebooks --retries 10',
        ],
        note="jupyterhub: census first (Pending vs DiskPressure vs missing deploy). "
             "Recycle pods when node is schedulable; zarf deploy only if chart/ns absent.",
    ),
    "T5.sample-notebooks": zarf_deploy_recipe(
        "sample-notebooks",
        note="sample-notebooks CM missing required keys (HDF5_*.ipynb + "
             "generate_hdf5.py + cluster_env.py) or absent — re-embed, package, "
             "deploy sample-notebooks; Stop/Start Jupyter so /root seeds refresh",
    ),
    "T6.ingress": zarf_deploy_recipe(
        "ingress",
        note="Ingress objects missing or wrong class / missing /ws on panel-viz",
    ),
}


def hint_for(inv_id: str) -> str:
    return HINTS.get(inv_id, "")


def enrich(detail: str, inv_id: str) -> str:
    """Attach the catalog recipe for ``inv_id`` onto a detail string."""
    return join_detail(detail, hint_for(inv_id))


def all_hint_ids() -> List[str]:
    return sorted(HINTS.keys())


def format_report_recipe(inv_id: str, detail: str) -> List[str]:
    """Lines for engine.report — detail first, then recipe if not embedded."""
    lines: List[str] = []
    for line in (detail or "").splitlines() or [""]:
        lines.append(line)
    recipe = hint_for(inv_id)
    if recipe and recipe not in (detail or "") and "DISCOVER:" not in (detail or ""):
        lines.append(f"── in situ ({inv_id}) ──")
        lines.extend(recipe.splitlines())
    return lines
