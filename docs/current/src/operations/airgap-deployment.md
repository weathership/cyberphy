# Air-Gap Deployment: OTEL Navigator + Dask

Deploy the Cyberphy OTEL Navigator visualization stack to RKE2 clusters without internet access using [Zarf](https://zarf.dev/).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    RKE2 Cluster (Air-Gap)                   │
│                                                             │
│  ┌─────────────┐     ┌─────────────────────────────────┐   │
│  │   Zarf      │     │         Dask Namespace           │   │
│  │  Registry   │────▶│  ┌───────────┐  ┌────────────┐  │   │
│  └─────────────┘     │  │ Scheduler │  │  Workers   │  │   │
│                      │  │  :8786    │  │  (N pods)  │  │   │
│                      │  └───────────┘  └────────────┘  │   │
│                      └─────────────────────────────────┘   │
│                                 │                           │
│                                 ▼                           │
│  ┌───────────────────────────────────────────────────────┐ │
│  │                Panel-Viz Namespace                     │ │
│  │   ┌───────────────────────────────────────────────┐   │ │
│  │   │            OTEL Navigator                      │   │ │
│  │   │   Panel + HoloViews + Datashader + Dask       │   │ │
│  │   │              :5006 (NodePort: 30506)          │   │ │
│  │   └───────────────────────────────────────────────┘   │ │
│  └───────────────────────────────────────────────────────┘ │
│                                 │                           │
│                                 ▼                           │
│                    ┌───────────────────┐                    │
│                    │   S3 Storage      │                    │
│                    │ (MinIO/Ceph/AWS)  │                    │
│                    │  OTEL Span Data   │                    │
│                    └───────────────────┘                    │
└─────────────────────────────────────────────────────────────┘
```

## Components

| Component | Image | Description |
|-----------|-------|-------------|
| cybersec-dask | Pre-built (~2GB) | Dask + all Python deps + OTEL Navigator app |
| Dask Operator | 50MB | Manages DaskCluster CRDs |
| Dask Scheduler | Uses cybersec-dask | Coordinates distributed computation |
| Dask Workers | Uses cybersec-dask | Execute computation tasks |
| OTEL Navigator | Uses cybersec-dask | Panel visualization dashboard |

## Pre-Built Image Contents

The `cybersec-dask:2024.8.0` image includes:

- **Runtime**: Python 3.11, Dask 2024.8.0, Distributed
- **Visualization**: Panel, HoloViews, Datashader, Bokeh, Colorcet
- **Data**: Pandas, NumPy, PyArrow, FastParquet
- **Storage**: S3fs, FSSpec
- **OTEL**: OpenTelemetry-proto, Protobuf
- **App**: `/app/otel-navigator.py` - Production Panel application
- **Optimization**: Numba JIT cache pre-compiled

**Startup time**: ~10-15 seconds (no runtime pip install)

## Prerequisites

### Build Environment (Internet Connected)

```bash
# Required tools
zarf version        # 0.32+ recommended
podman version      # or docker
kubectl version
helm version

# Project setup
cd cybersec
devenv shell        # Activates environment
```

### Target Environment (Air-Gap RKE2)

- RKE2 cluster with kubectl access
- StorageClass for JupyterHub PVCs (optional)
- S3-compatible storage with OTEL data pre-loaded

## Build Package

### Step 1: Build the Image

```bash
# Build cybersec-dask image with OTEL Navigator
devenv tasks run zarf:image

# Verify
podman images | grep cybersec-dask
# cybersec-dask   2024.8.0   abc123   2.1GB
```

### Step 2: Run Preflight Checks

```bash
# Validate all requirements
devenv tasks run zarf:preflight

# Or via CLI
cybersec "/zarf preflight"
```

Expected output:
```
Zarf Preflight Validation
==================================================

INFO:
  - kubectl installed at /usr/bin/kubectl
  - helm installed at /usr/bin/helm
  - podman available at /usr/bin/podman

Preflight PASSED - Ready for Zarf deployment
```

### Step 3: Create Zarf Package

```bash
# Create package (~2.5GB)
devenv tasks run zarf:package

# Output: zarf/zarf-package-cybersec-dask-amd64-1.0.0.tar.zst
```

### Step 4: Inspect Package (Optional)

```bash
cd zarf
zarf package inspect zarf-package-cybersec-dask-*.tar.zst
```

## Deploy to RKE2

### Step 1: Transfer Package

Transfer `zarf-package-cybersec-dask-*.tar.zst` to the air-gap environment via:
- USB drive
- Internal file server
- S3 bucket (if accessible)
- SCP to bastion host

### Step 2: Initialize Zarf

On the RKE2 cluster (first time only):

```bash
# Initialize Zarf - deploys internal registry
zarf init --confirm
```

### Step 3: Deploy Package

```bash
# Deploy with default settings
zarf package deploy zarf-package-cybersec-dask-*.tar.zst --confirm

# Or with custom settings
zarf package deploy zarf-package-cybersec-dask-*.tar.zst \
  --set DASK_WORKER_REPLICAS=8 \
  --set S3_ENDPOINT=http://minio.storage.svc:9000 \
  --set S3_ACCESS_KEY=minioadmin \
  --set S3_SECRET_KEY=minioadmin \
  --confirm
```

### Step 4: Validate Deployment

```bash
# Basic validation
./zarf/scripts/validate-deployment.sh

# Full validation including Dask/S3 connectivity
./zarf/scripts/validate-deployment.sh --full
```

Expected output:
```
OTEL Navigator + Dask Deployment Validation
==============================================

[OK] Connected to Kubernetes cluster
[OK] Namespace dask exists
[OK] Namespace panel-viz exists
[OK] Dask scheduler running: cybersec-dask-scheduler-xyz
[OK] Dask workers running: 4
[OK] OTEL Navigator running and ready: otel-navigator-abc

Access OTEL Navigator:
  NodePort: http://192.168.1.100:30506

Access Dask Dashboard:
  NodePort: http://192.168.1.100:30087
```

## Access Services

### NodePort Access (Default)

| Service | Port | URL |
|---------|------|-----|
| OTEL Navigator | 30506 | `http://<node-ip>:30506` |
| Dask Dashboard | 30087 | `http://<node-ip>:30087` |
| Dask Scheduler | 30086 | `tcp://<node-ip>:30086` |

### Ingress Access (Optional)

If RKE2 Traefik ingress is configured:

1. Add to `/etc/hosts`:
   ```
   <node-ip>  navigator.cyberphy.local dask.cyberphy.local
   ```

2. Access:
   - `http://navigator.cyberphy.local`
   - `http://dask.cyberphy.local`

## Configuration

### Zarf Variables

| Variable | Default | Description |
|----------|---------|-------------|
| DASK_WORKER_REPLICAS | 4 | Number of Dask workers |
| S3_ENDPOINT | (empty) | S3 endpoint for MinIO (empty = AWS S3) |
| S3_ACCESS_KEY | (empty) | S3 access key |
| S3_SECRET_KEY | (empty) | S3 secret key |
| INGRESS_CLASS | traefik | Ingress controller |
| INGRESS_DOMAIN | cyberphy.local | Base domain |

### OTEL Navigator Environment

The Navigator reads these from ConfigMap/Secret:

| Variable | Description |
|----------|-------------|
| DASK_SCHEDULER | Dask scheduler address (auto-configured) |
| S3_BUCKET | S3 bucket name |
| OTEL_DATA_PATH | Path to OTEL span Parquet files |
| AWS_REGION | AWS region for S3 |

## S3 Storage Requirements

S3-compatible storage must be accessible from the cluster with OTEL data pre-loaded.

### Data Format

```
s3://bucket/otel-minimal/
  └── spans/
      └── date=2024-01-15/
          └── hour=00/
              └── part-00000.parquet
```

### Options

1. **MinIO in Cluster**: Deploy MinIO and pre-load data
2. **External MinIO**: Point to existing MinIO instance
3. **Ceph Object Gateway**: Use Ceph S3 interface
4. **AWS S3**: If cluster has outbound access to S3

## Troubleshooting

### Pod Not Starting

```bash
# Check pod events
kubectl describe pod -n panel-viz -l app=otel-navigator

# Check logs
kubectl logs -n panel-viz -l app=otel-navigator --tail=100
```

### Dask Connection Failed

```bash
# Verify scheduler is running
kubectl get pods -n dask -l dask.org/component=scheduler

# Test connectivity from Navigator pod
kubectl exec -n panel-viz -l app=otel-navigator -- \
  python -c "from dask.distributed import Client; print(Client('tcp://cybersec-dask-scheduler.dask.svc.cluster.local:8786'))"
```

### S3 Access Failed

```bash
# Check credentials are set
kubectl get secret -n panel-viz otel-navigator-credentials -o yaml

# Test S3 from Navigator pod
kubectl exec -n panel-viz -l app=otel-navigator -- \
  python -c "import s3fs; fs = s3fs.S3FileSystem(); print(fs.ls('bucket-name'))"
```

### Image Pull Errors

```bash
# Check Zarf registry
kubectl get pods -n zarf -l app=zarf-registry

# Verify image is in registry
kubectl exec -n zarf -l app=zarf-registry -- \
  ls /var/lib/registry/docker/registry/v2/repositories/
```

## Testing with k3d

Before deploying to production RKE2, test with local k3d:

```bash
# 1. Provision k3d cluster
devenv tasks run k8s:provision

# 2. Build and deploy
devenv tasks run zarf:image
devenv tasks run zarf:package
zarf init --confirm
devenv tasks run zarf:deploy

# 3. Validate
./zarf/scripts/validate-deployment.sh --full

# 4. Access
open http://localhost:30506  # OTEL Navigator
open http://localhost:30087  # Dask Dashboard
```

## Updating Deployments

To update an existing deployment:

```bash
# 1. Build new image (on connected system)
devenv tasks run zarf:image

# 2. Create new package
devenv tasks run zarf:package

# 3. Transfer and deploy (on air-gap cluster)
zarf package deploy zarf-package-cybersec-dask-*.tar.zst --confirm
```

Zarf will update the deployment in-place, preserving data.
