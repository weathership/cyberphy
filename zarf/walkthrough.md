# Air-Gap RKE2 Zarf Deployment — Step-by-Step Walkthrough

> **Audience**: Operators deploying Cyberphy Dask to an air-gapped RKE2 node.
> Copy-paste every command — variables are set once in Phase 0.
> Each step links to a Discussion section at the bottom for rationale.

**Package version**: `v1.4.0` &nbsp;|&nbsp; **Zarf**: `v0.70.1` &nbsp;|&nbsp; **RKE2**: `v1.34.x`

---

## Table of Contents

- [Phase 0: Setup](#phase-0-setup)
- [Phase 1: Discovery](#phase-1-discovery)
- [Phase 2: Teardown — Packages](#phase-2-teardown--packages)
- [Phase 3: Teardown — Init](#phase-3-teardown--init)
- [Phase 4: Teardown — Storage](#phase-4-teardown--storage)
- [Phase 5: Preflight](#phase-5-preflight)
- [Phase 6: Storage & Init](#phase-6-storage--init)
- [Phase 7: Deploy & Verify](#phase-7-deploy--verify)
- [Discussion A: PVC Immutability](#discussion-a-pvc-immutability)
- [Discussion B: claimRef Pre-Binding](#discussion-b-claimref-pre-binding)
- [Discussion C: Zombie CRD Resources](#discussion-c-zombie-crd-resources)
- [Discussion D: Registry Storage](#discussion-d-registry-storage)
- [Discussion E: Containerd Image Cache](#discussion-e-containerd-image-cache)
- [Discussion F: Disk Pressure & GC Thresholds](#discussion-f-disk-pressure--gc-thresholds)
- [Discussion G: DaskCluster CRD Timing](#discussion-g-daskcluster-crd-timing)
- [Discussion H: Kubeconfig Staleness](#discussion-h-kubeconfig-staleness)
- [Discussion I: Multi-IP Nodes](#discussion-i-multi-ip-nodes)

---

## Phase 0: Setup

Set these three variables for your environment. Everything else derives from them.

```bash
# ── User-editable variables ──────────────────────────────────────────────
export KUBECONFIG=~/.kube/rke2.yaml          # path to your kubeconfig
export NODE_IP=10.0.0.5                       # node IPv4 address
export REGISTRY_PV_PATH=/var/lib/zarf-registry  # local disk only, NOT NFS [-> Discussion D]
# ─────────────────────────────────────────────────────────────────────────

# Derived defaults (rarely need changing)
export REGISTRY_PVC_SIZE=5Gi
export DASK_WORKER_REPLICAS=4
export DASK_SPILL_DIR=/tmp/dask-spill        # or /mnt/nfs/dask-spill if NFS available
export PATH=$PATH:/var/lib/rancher/rke2/bin

# Alias to cut repetition (all phases use this)
alias k="kubectl --kubeconfig $KUBECONFIG"
```

**Kubeconfig**: If you only have the system copy at `/etc/rancher/rke2/rke2.yaml`, make a
user-readable copy first. [-> Discussion H](#discussion-h-kubeconfig-staleness)

```bash
# requires sudo
sudo cp /etc/rancher/rke2/rke2.yaml ~/.kube/rke2.yaml
sudo chown $(id -u):$(id -g) ~/.kube/rke2.yaml
```

**Node IP**: If you don't know the node's IPv4 address, detect it from the cluster.
The JSONPath returns both IPv4 and IPv6 — we take only the first.
[-> Discussion I](#discussion-i-multi-ip-nodes)

```bash
NODE_IP=$(k get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' | awk '{print $1}')
echo "NODE_IP=$NODE_IP"
```

**Verify sudo**: Several steps require sudo for host-path operations and crictl.
Confirm you have it before proceeding.

```bash
sudo echo "sudo OK"
```

---

## Phase 1: Discovery

Read-only queries. Understand what's already on the cluster before touching anything.

```bash
# Namespaces — look for zarf, dask-operator, dask, jupyterhub, panel-viz
k get ns

# Persistent storage — look for zarf-registry-pv, zarf-docker-registry PVC
k get pv,pvc -A

# Helm releases — Zarf uses Helm internally
k get secrets -A -l owner=helm --no-headers | awk '{print $1, $2}'

# Zarf packages — what's currently deployed
zarf package list 2>/dev/null || echo "No packages or zarf not in PATH"

# Pod status across all namespaces
k get pods -A

# DaskCluster CRD instances
k get daskcluster -A 2>/dev/null || echo "No DaskCluster CRD installed"
```

**Decision point**: Based on discovery output, determine which phases to run:

| Cluster state | Start from |
|---------------|------------|
| Fresh / clean — no zarf namespace | [Phase 5](#phase-5-preflight) (skip teardown) |
| Previous deploy exists and is healthy | [Phase 2](#phase-2-teardown--packages) (full cycle) |
| Failed `zarf init` — PVC in Lost/Pending | [Phase 3](#phase-3-teardown--init) (skip package teardown) |
| Only stale PV/PVC remain (no zarf namespace) | [Phase 4](#phase-4-teardown--storage) |

---

## Phase 2: Teardown — Packages

Remove the cybersec-dask application package. **CRD instances must be deleted before
their namespaces** to prevent zombie resources. [-> Discussion C](#discussion-c-zombie-crd-resources)

```bash
# 2a. Delete CRD instances first (prevents zombies with kopf finalizers)
k delete daskcluster cybersec-dask -n dask --ignore-not-found
```

Wait for the DaskCluster to fully delete (the operator cascade-deletes worker pods):

```bash
k wait --for=delete daskcluster/cybersec-dask -n dask --timeout=60s 2>/dev/null || true
```

```bash
# 2b. Remove the Zarf package (cleans up Helm releases and namespaces)
zarf package remove cybersec-dask --confirm 2>/dev/null || true
```

```bash
# 2c. Verify — these namespaces should be gone or terminating
k get ns dask dask-operator jupyterhub panel-viz 2>&1
```

If any namespace is stuck in `Terminating` for more than 60 seconds, force-finalize it:

```bash
# Only if stuck — replace NAMESPACE with the stuck one
NAMESPACE=dask  # or dask-operator, jupyterhub, panel-viz
k patch namespace $NAMESPACE -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
```

**Zombie CRD check**: If the `dask` namespace is already gone but `kubectl get daskcluster -A`
still shows instances, the DaskCluster is a zombie in etcd — its kopf finalizer was never
cleared because the operator is gone. Fix by temporarily recreating the namespace:

```bash
# 2d. Clear zombie DaskCluster (only if namespace already deleted but CRD persists)
k create namespace dask 2>/dev/null || true
k patch daskcluster cybersec-dask -n dask --type=merge \
  -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
k delete namespace dask 2>/dev/null || true
k get daskcluster -A 2>/dev/null  # should show "No resources found"
```

---

## Phase 3: Teardown — Init

Remove Zarf's init infrastructure (registry, git-server, agent, namespace).

```bash
# 3a. Remove Zarf init package (the package name "zarf-init" is required)
zarf package remove zarf-init --confirm 2>/dev/null || true
```

```bash
# 3b. Check if the zarf namespace is gone
k get ns zarf 2>&1
```

If the namespace is stuck in `Terminating`:

```bash
# 3c. Force-finalize the zarf namespace
k delete namespace zarf --wait=false 2>/dev/null || true
k patch namespace zarf -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true

# Wait up to 30 seconds
for i in $(seq 1 6); do
  k get ns zarf 2>&1 | grep -q "not found" && echo "Clean" && break
  sleep 5
done
```

```bash
# 3d. Clean up orphaned Zarf Helm release secrets (left in default namespace)
# List first — only delete secrets whose names contain "zarf-"
k get secrets -n default -l owner=helm --no-headers \
  | awk '$1 ~ /zarf-/ {print $1}'

# Delete them (safe — only targets zarf-prefixed releases)
k get secrets -n default -l owner=helm --no-headers \
  | awk '$1 ~ /zarf-/ {print $1}' \
  | xargs -r kubectl --kubeconfig $KUBECONFIG delete secret -n default 2>/dev/null || true
```

> **Why**: Zarf stores Helm release secrets in the `default` namespace with hashed names
> like `sh.helm.release.v1.zarf-<hash>.v1`. When namespaces are force-finalized during
> teardown, these secrets survive and can cause ownership conflicts on redeploy.

---

## Phase 4: Teardown — Storage

This is where the [bug report](#discussion-a-pvc-immutability) failure occurred.
PVC `spec` fields (including `volumeName` and `storage` requests) are **immutable** on
bound claims. Helm rollback tried to blank the `volumeName` field and got rejected.
The fix: remove the PVC and PV entirely, then recreate them cleanly in Phase 6.
[-> Discussion A](#discussion-a-pvc-immutability)

```bash
# 4a. Read current state (before any mutations)
echo "=== PVC ==="
k get pvc zarf-docker-registry -n zarf -o yaml 2>/dev/null || echo "PVC not found"
echo ""
echo "=== PV ==="
k get pv zarf-registry-pv -o yaml 2>/dev/null || echo "PV not found"
```

```bash
# 4b. Patch finalizers on PVC (so delete isn't blocked)
k patch pvc zarf-docker-registry -n zarf \
  -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true

# 4c. Force-delete PVC
k delete pvc zarf-docker-registry -n zarf \
  --force --grace-period=0 2>/dev/null || true
```

```bash
# 4d. Patch finalizers on PV
k patch pv zarf-registry-pv \
  -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true

# 4e. Force-delete PV
k delete pv zarf-registry-pv \
  --force --grace-period=0 2>/dev/null || true
```

```bash
# 4f. Verify — both should be gone
k get pvc -n zarf 2>&1
k get pv zarf-registry-pv 2>&1
```

**Optional**: Clean up registry data on disk. Only needed if the data is corrupted
or you need to reclaim space.

```bash
# requires sudo
sudo rm -rf ${REGISTRY_PV_PATH}/*
```

---

## Phase 5: Preflight

Validate that the node is ready for a fresh `zarf init`.

### 5a. Disk space

The deployment needs ~7-9 GB of local disk. [-> Discussion F](#discussion-f-disk-pressure--gc-thresholds)

```bash
df -h / /var/lib/rancher ${REGISTRY_PV_PATH} 2>/dev/null
```

| Path | Minimum free |
|------|-------------|
| `/var/lib/rancher` | 5 GB (RKE2 runtime) |
| `$REGISTRY_PV_PATH` | 3 GB (registry images) |
| `/` (or wherever init package is) | 2 GB (temp during init) |

### 5b. RKE2 health

```bash
# requires sudo
sudo systemctl status rke2-server --no-pager | head -5
k get nodes -o wide
```

Both should show the node as `Ready`. If the node shows `NotReady`, check
`sudo journalctl -u rke2-server --no-pager | tail -50`.

### 5c. DiskPressure taint

```bash
k get nodes -o jsonpath='{range .items[*]}{.metadata.name}: DiskPressure={range .status.conditions[?(@.type=="DiskPressure")]}{.status}{end}{"\n"}{end}'
```

If `DiskPressure=True`, pods won't schedule. Free disk or adjust eviction thresholds.
[-> Discussion F](#discussion-f-disk-pressure--gc-thresholds)

### 5d. Containerd image cache (upgrades only)

On fresh deploys, skip this. On **upgrades** where you've rebuilt images, stale cached
images with old Zarf tag suffixes can cause `ImagePullBackOff`.
[-> Discussion E](#discussion-e-containerd-image-cache)

```bash
# requires sudo — list images in containerd
sudo /var/lib/rancher/rke2/bin/crictl \
  -r unix:///run/k3s/containerd/containerd.sock images \
  | grep -i "cybersec\|dask"
```

If stale images exist from a previous deploy, remove them:

```bash
# requires sudo — replace IMAGE_ID with the actual ID from crictl output
sudo /var/lib/rancher/rke2/bin/crictl \
  -r unix:///run/k3s/containerd/containerd.sock rmi IMAGE_ID
```

### 5e. Kubeconfig freshness

If RKE2 was recently reinstalled, the system kubeconfig at
`/etc/rancher/rke2/rke2.yaml` may be newer than your user copy.
[-> Discussion H](#discussion-h-kubeconfig-staleness)

```bash
# Compare mtimes
ls -la /etc/rancher/rke2/rke2.yaml $KUBECONFIG 2>/dev/null
```

If the system copy is newer, refresh:

```bash
# requires sudo
sudo cp /etc/rancher/rke2/rke2.yaml $KUBECONFIG
sudo chown $(id -u):$(id -g) $KUBECONFIG
```

### 5f. Zarf CLI and init package

```bash
zarf version
ls -lh zarf-init-amd64-*.tar.zst
```

Both the `zarf` binary and the `zarf-init-amd64-v0.70.1.tar.zst` package must be
present in the current directory (or on PATH for the binary).

### 5g. kube-system pods (Zarf injector requirement)

Zarf's injector needs at least 2 running pods in `kube-system` (typically CoreDNS):

```bash
k get pods -n kube-system --no-headers | grep Running
```

---

## Phase 6: Storage & Init

### 6a. Create registry directory on local disk

The registry uses hard links — it **must** be on local disk, not NFS.
[-> Discussion D](#discussion-d-registry-storage)

```bash
# requires sudo
sudo mkdir -p ${REGISTRY_PV_PATH}
sudo chown 1000:2000 ${REGISTRY_PV_PATH}
sudo chmod 777 ${REGISTRY_PV_PATH}
sudo chcon -R -t container_file_t ${REGISTRY_PV_PATH} 2>/dev/null || true
```

### 6b. Create PV with claimRef pre-binding

The `claimRef` guarantees this PV binds to Zarf's PVC even without a StorageClass
provisioner. Without it, the PVC stays `Pending` forever on bare RKE2.
[-> Discussion B](#discussion-b-claimref-pre-binding)

Review the manifest before applying:

```bash
cat manifests/registry-pv.yaml
```

The defaults (`5Gi`, `/var/lib/zarf-registry`) work for most deployments. Edit the file
if you need a different size or path, then apply:

```bash
k apply -f manifests/registry-pv.yaml
```

Verify the PV was created correctly:

```bash
k get pv zarf-registry-pv
# STATUS should be "Available" (not "Bound" yet — that happens during init)
```

### 6c. Run zarf init

The init package must be in the current directory. The `REGISTRY_PVC_SIZE` must match
the PV capacity from 6b.

```bash
cd /path/to/your/artifacts  # wherever zarf-init-amd64-*.tar.zst lives

zarf init --confirm --set REGISTRY_PVC_SIZE=${REGISTRY_PVC_SIZE}
```

This takes 5-10 minutes. Zarf:
1. Injects a bootstrap registry into a kube-system pod
2. Deploys a persistent registry (the `zarf-docker-registry` PVC binds to our PV)
3. Pushes init images into the registry
4. Installs the Zarf agent mutating webhook

### 6d. Verify init

```bash
# Registry pod should be Running
k get pods -n zarf

# PVC should be Bound to zarf-registry-pv
k get pvc -n zarf

# Registry should respond (requires auth — check HTTP 401 means it's alive)
curl -so /dev/null -w "%{http_code}" http://${NODE_IP}:31999/v2/
# 401 = registry is running (auth required). 000 = not reachable.
```

---

## Phase 7: Deploy & Verify

### 7a. Deploy the cybersec-dask package

[-> Discussion G](#discussion-g-daskcluster-crd-timing) explains why the package splits
namespace and CRD creation into separate manifests.

Discover the package file (version may vary):

```bash
cd /path/to/your/artifacts  # wherever the .tar.zst lives

# Find the latest package by modification time
PKG_FILE=$(ls -t zarf-package-cybersec-dask-amd64-*.tar.zst 2>/dev/null | head -1)
echo "Deploying: $PKG_FILE"

zarf package deploy "$PKG_FILE" --confirm \
  --set DASK_WORKER_REPLICAS=${DASK_WORKER_REPLICAS} \
  --set DASK_SPILL_DIR=${DASK_SPILL_DIR}
```

If you have S3/MinIO connectivity, add these flags:

```bash
  --set S3_ENDPOINT=http://minio:9000 \
  --set S3_ACCESS_KEY=minioadmin \
  --set S3_SECRET_KEY=minioadmin
```

### 7a-note. JupyterHub hub database

The JupyterHub Helm chart defaults to `hub.db.type: sqlite-pvc`, which creates a 1Gi
PVC (`hub-db-dir`) with the cluster's default StorageClass. On air-gapped RKE2 without
a working `local-path-provisioner`, that PVC stays Pending forever and blocks the hub.

**Current fix (v1.2.2+):** `jupyterhub-values.yaml` sets `hub.db.type: sqlite-memory`,
eliminating the PVC entirely. Hub state (sessions, tokens) lives in memory only — on
hub pod restart users re-login, but running notebook servers are unaffected.

> **Older packages (pre-v1.2.2):** If the hub is stuck on a Pending `hub-db-dir` PVC,
> you must either (a) rebuild the package with the `sqlite-memory` fix, or (b) manually
> create a hostPath PV with a `claimRef` to `hub-db-dir` in the `jupyterhub` namespace.
> See [Discussion A] for PV pre-binding details.

### 7b. Wait for pods

```bash
# Watch all pods come up (Ctrl-C when all are Running)
k get pods -A -w
```

Expected pods:

| Namespace | Pod | Count |
|-----------|-----|-------|
| `zarf` | `docker-registry-*` | 1 |
| `dask-operator` | `dask-kubernetes-operator-*` | 1 |
| `dask` | `cybersec-dask-scheduler-*` | 1 |
| `dask` | `cybersec-dask-default-worker-*` | N (= `DASK_WORKER_REPLICAS`) |
| `jupyterhub` | `hub-*` | 1 |
| `jupyterhub` | `proxy-*` | 1 |
| `panel-viz` | `panel-viz-*` (label: `app=otel-navigator`) | 1 |
| `panel-viz` | `navigator-engine-*` | 1 |

### 7c. Verify DaskCluster CRD

```bash
k get daskcluster -n dask
```

If the DaskCluster is missing but the `dask` namespace exists, the CRD timing
issue occurred. [-> Discussion G](#discussion-g-daskcluster-crd-timing)

### 7d. Verify HTTP endpoints

```bash
# Dask Dashboard
curl -sf http://${NODE_IP}:30087/health && echo "Dask Dashboard OK"

# JupyterHub
curl -sf -o /dev/null -w "HTTP %{http_code}" http://${NODE_IP}:30080/ && echo ""

# OTEL Navigator
curl -sf -o /dev/null -w "HTTP %{http_code}" http://${NODE_IP}:30506/ && echo ""
```

### 7e. Verify Dask cluster connectivity

```bash
k exec -n dask \
  $(k get pod -n dask -l dask.org/component=scheduler -o jsonpath='{.items[0].metadata.name}') \
  -- python -c "
from dask.distributed import Client
c = Client('tcp://localhost:8786', timeout='10s')
info = c.scheduler_info()
print(f'Workers: {len(info[\"workers\"])}')
c.close()
"
```

### 7f. Final status

```bash
k get pods -A
k get svc -A | grep -E "NodePort|LoadBalancer"
```

**Access services**:

| Service | URL |
|---------|-----|
| Dask Dashboard | `http://<NODE_IP>:30087` |
| Dask Scheduler | `tcp://<NODE_IP>:30086` |
| JupyterHub | `http://<NODE_IP>:30080` (admin / changeme) |
| OTEL Navigator | `http://<NODE_IP>:30506` |

---

## Discussion Sections

### Discussion A: PVC Immutability

**Why Helm rollback fails after a `zarf init` timeout.**

When `zarf init` times out (default 15 minutes), it attempts a Helm rollback of the
`docker-registry` chart. The rollback tries to revert the PVC to its pre-upgrade state,
which means blanking the `volumeName` field:

```
- VolumeName: "zarf-registry-pv"
+ VolumeName: ""
```

Kubernetes rejects this because PVC `spec` fields are **immutable after creation** for
bound claims. The exact error from the [bug report](../zarf-error-2026-03-04.txt):

```
PersistentVolumeClaim "zarf-docker-registry" is invalid:
  spec: Forbidden: spec is immutable after creation except
  resources.requests and volumeAttributesClassName for bound claims
```

**The only recovery path is to delete the PVC and PV entirely** (Phase 4), then
recreate them from scratch (Phase 6). You cannot patch your way out — the Kubernetes
API server enforces immutability at the admission level.

The bug report also showed YAML indentation errors in the manual PV creation:

```yaml
# WRONG — spec nested under metadata
metadata:
  name: zarf-registry-pv
  spec:          # <-- indented under metadata, silently ignored
    capacity: ...

# RIGHT — spec at same level as metadata
metadata:
  name: zarf-registry-pv
spec:            # <-- top-level
  capacity: ...
```

Always validate with `kubectl apply --dry-run=client -f -` before applying.

---

### Discussion B: claimRef Pre-Binding

**How to guarantee PV-to-PVC binding without a StorageClass.**

Bare RKE2 (without Rancher or Longhorn) has no default StorageClass provisioner. When
Zarf creates a PVC, Kubernetes has no way to dynamically provision a PV. The PVC stays
`Pending` indefinitely and `zarf init` times out.

The `claimRef` field on a PV tells Kubernetes "this PV is reserved for this specific PVC."
When the PVC is created during `zarf init`, the scheduler sees the pre-bound PV and
immediately binds them:

```yaml
spec:
  claimRef:
    namespace: zarf
    name: zarf-docker-registry
```

Without `claimRef`, even a manually created PV may bind to the wrong PVC (or none at all)
depending on access modes and capacity matching.

The PV size (`capacity.storage`) and the `--set REGISTRY_PVC_SIZE` passed to `zarf init`
**must match**. If the PVC requests 20Gi but the PV only offers 5Gi, binding fails.

---

### Discussion C: Zombie CRD Resources

**Why CRD instances must be deleted before their namespaces.**

The Dask Kubernetes Operator uses [kopf](https://kopf.readthedocs.io/) finalizers on
`DaskCluster` resources. When you force-finalize a namespace (to get past `Terminating`),
the namespace is deleted but the CRD instance's finalizer is never processed. This leaves
a "zombie" DaskCluster in etcd.

When you recreate the namespace (during the next deploy), the zombie reappears — with
stale Helm annotations that conflict with the new deployment's ownership labels.

**Always delete CRD instances before their namespaces:**

```bash
# RIGHT order
kubectl delete daskcluster cybersec-dask -n dask --ignore-not-found
kubectl delete namespace dask

# WRONG order — creates zombies
kubectl delete namespace dask --force
kubectl patch namespace dask -p '{"metadata":{"finalizers":null}}'
# DaskCluster is now a zombie in etcd
```

**Recovering from existing zombies**: If the namespace is already gone but the CRD
instance persists (visible via `kubectl get daskcluster -A`), you **cannot** patch it
because kubectl routes through the namespace API which returns "not found". The fix:
temporarily recreate the namespace, clear the finalizer, then delete the namespace.
See Phase 2d.

**Additional danger**: Removing the kopf finalizer directly from a DaskCluster can
trigger a cascade that deletes the DaskCluster **CRD itself** (not just the instance),
requiring a full operator re-deploy.

Prefer `zarf package remove` over manual cleanup — it handles this ordering correctly.

---

### Discussion D: Registry Storage

**Why the registry must use local disk, and what REGISTRY_PVC_ENABLED=false actually does.**

The Docker registry uses a `filesystem` storage driver that relies on:
- **Hard links**: Layer deduplication creates hard links between blobs
- **Atomic renames**: Uploads are written to a temp file, then `rename(2)`'d into place

NFS does not support cross-directory hard links and has inconsistent `rename` semantics
across implementations. Using NFS for the registry causes push failures with
`Filesystem` errors.

**`REGISTRY_PVC_ENABLED=false` is a trap.** It doesn't switch to `emptyDir` — it
removes the volume mount entirely. The registry container starts, finds no storage
configuration, and crashes immediately:

```
"no storage configuration provided"
```

The correct approach for disk-constrained nodes is a **small hostPath PV** (5Gi label,
~2 GB actual usage) on local disk. Dask spill and user data can go on NFS.

| Data | Storage type | Why |
|------|-------------|-----|
| Zarf registry | Local hostPath | Hard links + atomic renames |
| Dask spill-to-disk | NFS or emptyDir | Large, ephemeral |
| JupyterHub notebooks | NFS (optional) | Persistence across restarts |

---

### Discussion E: Containerd Image Cache

**Why `ImagePullBackOff` occurs on upgrades but not fresh deploys.**

Zarf rewrites image tags with a CRC32 suffix derived from the image reference. For
example:

```
localhost:5555/cybersec-dask:2025.2.0
  → 127.0.0.1:31999/library/cybersec-dask:2025.2.0-zarf-3958020789
```

Pods use `imagePullPolicy: IfNotPresent`. On a **fresh deploy**, containerd has no
cached image, so it pulls from the Zarf registry. On an **upgrade** where you've
rebuilt the image with the same tag, containerd still has the old image cached and
never re-pulls.

**Fix**: Remove the stale image from containerd's cache, then delete the pod:

```bash
# requires sudo
CRICTL="sudo /var/lib/rancher/rke2/bin/crictl -r unix:///run/k3s/containerd/containerd.sock"

# Find the stale image
$CRICTL images | grep cybersec-dask

# Remove it (use the IMAGE ID from the output above)
$CRICTL rmi <IMAGE_ID>

# Delete the pod so it re-pulls
kubectl delete pod -n dask -l dask.org/component=scheduler
```

---

### Discussion F: Disk Pressure & GC Thresholds

**Why air-gap nodes need `image-gc-high-threshold=99`.**

Default kubelet thresholds:
- `eviction-hard`: `nodefs.available<5%` (evict pods when <5% disk free)
- `image-gc-high-threshold`: `85%` (garbage-collect container images when disk >85% used)

On an air-gap node, **image GC is catastrophic**: containerd deletes cached images that
can never be re-pulled from the internet. The node enters a death spiral — GC frees
space, pods restart, try to pull images, fail, get evicted again.

**Recommended `/etc/rancher/rke2/config.yaml`**:

```yaml
kubelet-arg:
  - eviction-hard=nodefs.available<2%,imagefs.available<2%
  - eviction-soft=nodefs.available<5%,imagefs.available<5%
  - eviction-soft-grace-period=nodefs.available=2m,imagefs.available=2m
  - image-gc-high-threshold=99
```

`image-gc-high-threshold=99` effectively disables image GC (only triggers at 99% disk
usage). The `eviction-hard=2%` gives more runway before pod eviction.

After changing `config.yaml`:

```bash
# requires sudo
sudo systemctl restart rke2-server
```

Note: `evictionPressureTransitionPeriod` defaults to 5 minutes — after crossing back
above the threshold, budget 5 minutes before DiskPressure clears and pods reschedule.

---

### Discussion G: DaskCluster CRD Timing

**Why the Zarf package splits namespace and DaskCluster into separate manifests.**

In `zarf.yaml`, the `dask-cluster` component uses two separate manifest entries:

```yaml
manifests:
  - name: dask-namespaces
    files:
      - manifests/namespace.yaml
  - name: dask-cluster-cr
    files:
      - manifests/dask-cluster.yaml
```

This is intentional. When namespace creation and CRD instance creation were combined
into a single manifest (`dask-namespace-and-cluster`), Zarf's raw manifest wrapper
would silently fail to create the DaskCluster. The namespace was created, but the
DaskCluster never appeared — no error, no warning.

Splitting them into separate entries forces Zarf to process them as two separate
Helm releases, ensuring the namespace exists before the DaskCluster is applied.

**How to tell if this happened**: The `dask` namespace exists and the operator pod
is running, but `kubectl get daskcluster -n dask` returns nothing. Re-apply the
DaskCluster manifest manually:

```bash
kubectl apply -f manifests/dask-cluster.yaml
```

---

### Discussion H: Kubeconfig Staleness

**System vs. user kubeconfig copies and mtime-based detection.**

RKE2 writes its kubeconfig to `/etc/rancher/rke2/rke2.yaml` (root-owned, mode 600).
Operators typically copy it to `~/.kube/rke2.yaml` for non-root access. This creates
a staleness risk:

| Scenario | Result |
|----------|--------|
| RKE2 reinstalled, user copy not refreshed | `kubectl` uses expired certificates → connection refused |
| RKE2 upgraded, API server address changed | `kubectl` connects to wrong endpoint |
| User copy was made before cluster fully initialized | Missing cluster CA data |

**Detection**: Compare modification times:

```bash
stat -c '%Y %n' /etc/rancher/rke2/rke2.yaml ~/.kube/rke2.yaml 2>/dev/null
```

If the system copy is newer, refresh the user copy (see Phase 0).

The kubeconfig content **may not contain** "rke2" or "rancher" strings — don't rely on
content matching for cluster type detection. Check the file path and kubelet version
instead:

```bash
kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.kubeletVersion}'
# RKE2 versions contain "+rke2r1"
```

---

### Discussion I: Multi-IP Nodes

**Why the JSONPath for InternalIP returns more than you expect.**

The JSONPath expression:

```
{.items[0].status.addresses[?(@.type=="InternalIP")].address}
```

returns **all** addresses matching `type==InternalIP`. On dual-stack nodes (IPv4 + IPv6),
this gives you two addresses space-separated:

```
10.0.0.5 fd00::5
```

Using this directly in URLs like `http://${NODE_IP}:30087` breaks because it becomes
`http://10.0.0.5 fd00::5:30087`.

**Fix**: Always pipe through `awk '{print $1}'` to extract only the first (IPv4) address:

```bash
NODE_IP=$(kubectl get nodes \
  -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' \
  | awk '{print $1}')
```

This is implemented in Phase 0 and in `verify-zarf-deployment.sh:175`.
