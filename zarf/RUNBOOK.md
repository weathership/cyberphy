# Cyberphy Dask — Air-Gap Deployment Runbook

**Package**: `cybersec-dask` v1.4.0
**Built with**: Zarf **v0.70.1** (the deploy binary and init package MUST match this version)
**Target**: RKE2 cluster (bare metal or cloud, no internet required at deploy time)

> ⚠️ **Version pinning is critical in air-gap.** Zarf rejects a package whose
> build version differs from the deploying binary ("format-version skew"), and
> you cannot recover offline. Use **zarf v0.70.1** and **zarf-init-amd64-v0.70.1.tar.zst**
> with this v1.4.0 package. The authoritative trio + checksums are listed in
> `BOOTSTRAP_VERSIONS.txt` (shipped as a release asset).

---

## Prerequisites

- RKE2 cluster running with `kubectl` access
- `root` or `sudo` on the control plane node
- Three files transferred to the control plane (see Phase 1)

---

## Phase 1: Acquire Artifacts (Connected Machine)

> **Simplest path:** download all four required artifacts straight from the
> [zarf-v1.4.0 release](https://github.com/rch/cldr-cybersec/releases/tag/zarf-v1.4.0)
> — the release bundles the version-matched `zarf` binary, init package, the
> cybersec-dask package, and `BOOTSTRAP_VERSIONS.txt` (with checksums). Then skip
> to the Transfer Checklist. The manual steps below are the build-it-yourself path.

```bash
# 1. Zarf binary — MUST be v0.70.1 to match the package build version
curl -LO https://github.com/zarf-dev/zarf/releases/download/v0.70.1/zarf_v0.70.1_Linux_amd64
mv zarf_v0.70.1_Linux_amd64 zarf && chmod +x zarf

# 2. Zarf init package — version MUST match the binary (v0.70.1)
./zarf tools download-init
# produces: zarf-init-amd64-v0.70.1.tar.zst (~390 MB)

# 3. Cyberphy Dask package (build from source or download release)
# Option A: Build  (requires the same zarf v0.70.1 in PATH)
git clone https://github.com/cloudera/cybersec && cd cybersec
zarf package create zarf/ --confirm
# produces: zarf/zarf-package-cybersec-dask-amd64-1.4.0.tar.zst (~1.3 GB)

# Option B: Download from GitHub Releases
# https://github.com/rch/cldr-cybersec/releases/tag/zarf-v1.4.0

# 4. Vendored StorageClass manifest + recovery scripts (included in repo)
cp zarf/manifests/local-path-provisioner.yaml .
cp zarf/scripts/zarf-clean-slate.sh zarf/scripts/zarf-init-recovery.sh .
```

> ⚠️ **The zarf binary, the `zarf-init-amd64-vX.Y.Z.tar.zst`, and the package
> must all be the SAME zarf version (v0.70.1).** A mismatch (e.g. a v0.74.0
> binary against this v0.70.1 package) is rejected by Zarf and is unrecoverable
> on an air-gapped node. Verify against `BOOTSTRAP_VERSIONS.txt`.

### Transfer Checklist

| File | Size | Required |
|------|------|----------|
| `zarf` (binary, **v0.70.1** linux/amd64) | ~180 MB | Yes |
| `zarf-init-amd64-v0.70.1.tar.zst` | ~390 MB | Yes |
| `zarf-package-cybersec-dask-amd64-1.4.0.tar.zst` | ~1.3 GB | Yes |
| `local-path-provisioner.yaml` | 5 KB | Yes (if no default StorageClass) |
| `scripts/zarf-clean-slate.sh` | ~20 KB | Recommended (cleans stale state) |
| `scripts/zarf-init-recovery.sh` | ~5 KB | Recommended (registry-PVC recovery) |
| `BOOTSTRAP_VERSIONS.txt` | 1 KB | Recommended (version + checksum manifest) |

Before transfer, verify all three artifacts against the shipped manifest:

```bash
grep 'sha256:' BOOTSTRAP_VERSIONS.txt | awk '{gsub("sha256:","",$3); print $3"  "$2}' | sha256sum -c -
# expect: <each artifact>: OK
```

Transfer all files to the control plane node via USB, SCP, data diode, etc.

---

## Phase 2: Deploy (Air-Gapped Control Plane)

All commands run as `root` (or prefix with `sudo`).

### Step 1 — Environment Setup

```bash
export KUBECONFIG=/etc/rancher/rke2/rke2.yaml
export PATH=$PATH:/var/lib/rancher/rke2/bin

# Install Zarf binary
cp zarf /usr/local/bin/zarf
chmod +x /usr/local/bin/zarf
zarf version
```

### Step 2 — Clean Slate (required if any previous Zarf attempt was made)

If the cluster has ANY leftover state from a previous Zarf init or deploy
(even a failed one), run the cleanup script first. This is safe to run on
a fresh cluster — it will detect nothing to clean and confirm clean state.

```bash
chmod +x scripts/zarf-clean-slate.sh

# Preview what will be removed (no changes)
./scripts/zarf-clean-slate.sh --dry-run

# Run cleanup (keeps local-path-provisioner if you want to preserve it)
./scripts/zarf-clean-slate.sh --keep-provisioner

# Or full cleanup including provisioner
./scripts/zarf-clean-slate.sh

# Should print: CLEAN SLATE — ready for zarf init
```

### Step 3 — Verify Cluster Health

```bash
kubectl get nodes
# All nodes should be Ready

kubectl get storageclass
# Note: if no default StorageClass is listed, Step 4 is required
```

### Step 4 — Install StorageClass (skip if one already exists)

Bare RKE2 has no default StorageClass. Without one, Zarf's registry PVC
stays Pending forever.

```bash
kubectl apply -f local-path-provisioner.yaml

# Wait for provisioner to be ready
kubectl wait --for=condition=ready pod -l app=local-path-provisioner \
  -n local-path-storage --timeout=60s

# Verify it's the default
kubectl get storageclass
# NAME                   PROVISIONER             AGE
# local-path (default)   rancher.io/local-path   10s
```

### Step 5 — Zarf Init

Zarf auto-detects the init package by looking for `zarf-init-amd64-*.tar.zst`
in the current directory. If not found, it attempts to download from GitHub
(which will fail in air-gap).

```bash
cd /path/to/artifacts
ls zarf-init-amd64-*.tar.zst   # confirm init package is here

zarf init --confirm --set REGISTRY_PVC_SIZE=1Gi

# Wait ~2-3 minutes. Verify:
kubectl get pods -n zarf
# NAME                                      READY   STATUS
# zarf-docker-registry-*                    1/1     Running
# agent-hook-*                              1/1     Running
```

### Step 6 — Deploy Cyberphy Dask

```bash
zarf package deploy zarf-package-cybersec-dask-amd64-1.4.0.tar.zst \
  --confirm \
  --set DASK_WORKER_REPLICAS=4 \
  --set DASK_SPILL_DIR=/tmp/dask-spill
```

#### S3 storage — REQUIRED for a local (non-AWS) gateway

There is **no AWS instance role / IMDS in air-gap**, so the Dask workers and the
viz app cannot auto-discover credentials. If your OTEL data lives on a local
S3-compatible gateway (MinIO, etc.) you **must** pass the endpoint **and** the
access/secret keys, or the Dask workers fail S3 auth silently and **Panel-Viz
renders an empty heatmap**:

```bash
zarf package deploy zarf-package-cybersec-dask-amd64-1.4.0.tar.zst \
  --confirm \
  --set DASK_WORKER_REPLICAS=4 \
  --set DASK_SPILL_DIR=/tmp/dask-spill \
  --set S3_ENDPOINT=http://minio.storage.svc:9000 \
  --set S3_BUCKET=cybersec-data \
  --set S3_REGION=us-east-1 \
  --set S3_ACCESS_KEY=<access-key> \
  --set S3_SECRET_KEY=<secret-key>
```

Notes for on-prem gateways:

- **Credentials are mandatory** here (unlike AWS, where they are left empty so
  the instance role is used). Omitting `S3_ACCESS_KEY`/`S3_SECRET_KEY` deploys
  cleanly but leaves the data path unauthenticated → blank viz.
- **Path-style addressing** is applied automatically whenever `S3_ENDPOINT` is
  set (gateways reached by IP/hostname reject AWS virtual-hosted addressing).
- Credentials are injected into the **Dask scheduler + workers**, the **viz**
  pod, and the **navigator-engine** so the whole data path authenticates.
- The package auto-creates the bucket on deploy if it does not exist. Load your
  OTEL parquet under `s3://<S3_BUCKET>/<dataset>/spans/...` (default dataset is
  `otel-minimal`; the app honors an `_active_dataset.json` marker at the bucket
  root if present). Override the path with `--set OTEL_DATA_PATH=...` if needed.

Deployment takes ~5-15 minutes (image push to internal registry is the bottleneck).

---

## Phase 3: Verify

### Quick Check

```bash
# All pods running
kubectl get pods -A | grep -E 'dask|jupyter|panel-viz|local-path|zarf'
```

### Expected State

```bash
# Dask
kubectl get pods -n dask
# cybersec-dask-scheduler-*     1/1   Running
# cybersec-dask-worker-*        1/1   Running   (×DASK_WORKER_REPLICAS)

# JupyterHub
kubectl get pods -n jupyterhub
# hub-*                         1/1   Running
# proxy-*                       1/1   Running

# Panel-Viz (OTEL Navigator)
kubectl get pods -n panel-viz
# otel-navigator-*              2/2   Running   (app + PTY proxy sidecar)
# navigator-engine-*            1/1   Running

# Dask cluster health
kubectl exec -n dask deploy/cybersec-dask-scheduler -- \
  python -c "from distributed import Client; c=Client('localhost:8786'); print(c)"
```

### Service Ports (NodePort)

| Service | NodePort | Description |
|---------|----------|-------------|
| Dask Scheduler | 30086 | Client connections |
| Dask Dashboard | 30087 | Web UI |
| JupyterHub | 30080 | Web UI |
| Panel-Viz | 30506 | OTEL Navigator |

Access from any node IP: `http://<node-ip>:<nodeport>`

---

## Troubleshooting

### Clean up previous Zarf state before re-init

If a previous `zarf init` or `zarf package deploy` was run on this cluster,
you must clean it first. Common error: `Forbidden: field cannot be less than
previous value` (PVC size mismatch from earlier init).

```bash
# Remove previous app package (if any)
zarf package remove cybersec-dask --confirm 2>/dev/null || true

# Remove previous Zarf init
zarf destroy --confirm 2>/dev/null || true

# If zarf destroy fails, clean manually:
kubectl delete namespace zarf --wait=false 2>/dev/null || true
kubectl delete pvc -n zarf zarf-docker-registry 2>/dev/null || true
kubectl delete pv -l app=docker-registry 2>/dev/null || true

# Wait for namespace to terminate (may need finalizer cleanup)
kubectl get ns zarf 2>/dev/null && \
  kubectl patch ns zarf -p '{"metadata":{"finalizers":null}}' --type=merge

# Verify clean
kubectl get ns zarf 2>&1 | grep -q "not found" && echo "Clean"

# Now retry Step 4
```

### Zarf init PVC stuck Pending

```bash
# Check if StorageClass exists
kubectl get storageclass

# If none: install local-path-provisioner (Step 3)
# If stuck from failed previous attempt, use the cleanup steps above
```

### Image push timeout during deploy

```bash
# Retry — layers are cached from first attempt
zarf package deploy zarf-package-cybersec-dask-*.tar.zst --confirm \
  --set DASK_WORKER_REPLICAS=4
```

### Pod in ImagePullBackOff

```bash
# Check if image is in Zarf registry
kubectl get pods -n zarf -l app=docker-registry
zarf tools registry catalog 127.0.0.1:31999

# If image missing, redeploy the component
zarf package deploy zarf-package-cybersec-dask-*.tar.zst --confirm \
  --components=cybersec-images
```

### Panel-Viz loads but the heatmap is blank (no data)

The pod is healthy and the page renders, but the scatter/heatmap is empty. On an
on-prem gateway this is almost always an S3 data-path problem, not a viz bug:

```bash
# 1. Confirm credentials reached the data path (must be non-empty on a gateway)
kubectl exec -n dask deploy/cybersec-dask-scheduler -- \
  sh -c 'echo "AKID len=${#AWS_ACCESS_KEY_ID} ENDPOINT=$S3_ENDPOINT BUCKET=$S3_BUCKET"'
# AKID len must be > 0. If 0, redeploy with --set S3_ACCESS_KEY/S3_SECRET_KEY.

# 2. Confirm the gateway is reachable with PATH-STYLE addressing and that data exists
kubectl exec -n panel-viz deploy/otel-navigator -- python -c "
import os, s3fs
fs = s3fs.S3FileSystem(
    key=os.environ['AWS_ACCESS_KEY_ID'], secret=os.environ['AWS_SECRET_ACCESS_KEY'],
    client_kwargs={'endpoint_url': os.environ['S3_ENDPOINT']},
    config_kwargs={'s3': {'addressing_style': 'path'}})
b = os.environ['S3_BUCKET']
print('parquet files:', len(fs.glob(f'{b}/**/spans/**/*.parquet')))"
# 0 files => no data loaded into the bucket, or wrong OTEL_DATA_PATH/bucket.
```

Common causes and fixes:

| Symptom | Cause | Fix |
|---------|-------|-----|
| `AKID len=0` | credentials not passed | redeploy with `--set S3_ACCESS_KEY=... --set S3_SECRET_KEY=...` |
| `InvalidAccessKeyId` / connection refused | wrong gateway addressing | already auto path-style when `S3_ENDPOINT` set; verify endpoint URL/port |
| `0 parquet files` | no data, or wrong path | load data under `s3://<bucket>/<dataset>/spans/`, or `--set OTEL_DATA_PATH=` |
| renders only with a wide preset | data older than "Last 24 Hours" | none needed — v1.4.0 anchors the window to the dataset's latest date |

> The **sample notebooks** (`Dask_S3_Validation.ipynb`, `OTEL_Data_Generator.ipynb`)
> are AWS-oriented examples ("uses IAM role automatically") — for an on-prem
> gateway, edit their S3 client cells to pass `endpoint_url` + `key`/`secret`
> and `config_kwargs={'s3': {'addressing_style': 'path'}}`.

### Disk-constrained node

Use a small hostPath PV instead of the dynamic provisioner:

```bash
mkdir -p /var/lib/zarf-registry && chmod 777 /var/lib/zarf-registry
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: PersistentVolume
metadata:
  name: zarf-registry-pv
spec:
  capacity:
    storage: 5Gi
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  hostPath:
    path: /var/lib/zarf-registry
  claimRef:
    namespace: zarf
    name: zarf-docker-registry
EOF

zarf init --confirm --set REGISTRY_PVC_SIZE=5Gi
```

---

## Teardown

```bash
# Remove application package
zarf package remove cybersec-dask --confirm

# Remove Zarf itself
zarf destroy --confirm

# Remove StorageClass (if installed)
kubectl delete -f local-path-provisioner.yaml
```

---

## Deploy Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `DASK_WORKER_REPLICAS` | `4` | Number of Dask workers |
| `DASK_SPILL_DIR` | `/tmp/dask-spill` | Host path for spill-to-disk (NFS recommended) |
| `S3_ENDPOINT` | _(empty)_ | S3-compatible endpoint URL |
| `S3_BUCKET` | `cybersec-dask-data` | Bucket name for OTEL data |
| `S3_REGION` | `us-east-1` | AWS region |
| `S3_ACCESS_KEY` | _(empty)_ | S3 access key |
| `S3_SECRET_KEY` | _(empty)_ | S3 secret key |
| `S3_SESSION_TOKEN` | _(empty)_ | AWS STS session token |
| `INGRESS_CLASS` | `traefik` | Ingress controller class |
| `INGRESS_DOMAIN` | `cyberphy.local` | Base domain for ingress |

## Components

All optional components deploy by default. Skip with `--components`:

```bash
# Deploy without JupyterHub
zarf package deploy *.tar.zst --confirm \
  --components=local-path-provisioner,cybersec-images,dask-operator,dask-cluster,panel-viz,navigator-engine
```

| Component | Default | Skip-safe? |
|-----------|---------|------------|
| local-path-provisioner | on | Yes (if StorageClass exists) |
| cybersec-images | required | No |
| dask-operator | required | No |
| dask-cluster | required | No |
| jupyterhub | on | Yes |
| panel-viz | on | Yes |
| navigator-engine | on | Yes |
| sample-notebooks | on | Yes |
| ingress | on | Yes (if no ingress controller) |
