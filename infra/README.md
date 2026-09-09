# Infrastructure Management

Automation for Kubernetes-based compute (Dask, JupyterHub) across local and cloud environments.

## Choose Your Deployment Mode

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Where are you deploying?                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
              ▼                       ▼                       ▼
┌─────────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐
│   Local Laptop      │   │  Existing Cluster   │   │     AWS Cloud       │
│   (Quick Start)     │   │  (RKE2/K8s)         │   │   (Production)      │
├─────────────────────┤   ├─────────────────────┤   ├─────────────────────┤
│ k3d on Podman       │   │ KUBECONFIG required │   │ OpenTofu + Ansible  │
│ ~5 min setup        │   │ ~10 min setup       │   │ ~30 min setup       │
│ Free                │   │ Free                │   │ AWS charges apply   │
│                     │   │                     │   │                     │
│ devenv tasks run    │   │ export KUBECONFIG   │   │ devenv tasks run    │
│   k8s:provision     │   │ devenv tasks run    │   │   aws:provision     │
│   k8s:deploy-dask   │   │   k8s:deploy-dask   │   │   aws:deploy        │
│   k8s:forward       │   │   k8s:forward       │   │   aws:verify        │
├─────────────────────┤   ├─────────────────────┤   ├─────────────────────┤
│ See LOCAL.md        │   │ See LOCAL.md        │   │ See aws/README.md   │
│ §k3d Development    │   │ §RKE2 Testing       │   │                     │
└─────────────────────┘   └─────────────────────┘   └─────────────────────┘
```

### Quick Reference

| Task | Local k3d | Local RKE2 | AWS |
|------|-----------|------------|-----|
| Provision cluster | `k8s:provision` | (external) | `aws:provision` |
| Deploy Dask | `k8s:deploy-dask` | `k8s:deploy-dask` | `aws:deploy:dask` |
| Deploy JupyterHub | `k8s:deploy-jupyter` | `k8s:deploy-jupyter` | `aws:deploy:jupyterhub` |
| External HTTPS access | — | — | Cloudflare Tunnel (IaC) or ngrok |
| Port forward services | `k8s:forward` | `k8s:forward` | — |
| Check status | `k8s:status` | `k8s:status` | `aws:status` |
| Destroy | `k8s:destroy` | (manual) | `aws:teardown` |

### Key Differences

| Aspect | `k8s:*` Tasks | `aws:*` Tasks |
|--------|---------------|---------------|
| **Target** | Local clusters (k3d, existing RKE2) | AWS cloud infrastructure |
| **Infrastructure** | None (uses existing) or k3d | OpenTofu (EC2, VPC, S3, Cloudflare) |
| **Cluster** | k3d (single-node) or external KUBECONFIG | RKE2 (3 CP + 3 workers) |
| **App Deployment** | Helm charts directly | Ansible playbooks |
| **External Access** | None (localhost port-forward) | Cloudflare Tunnel + Zero Trust (WARP) or ngrok |
| **Cost** | Free | AWS charges |

## Architecture Overview

```
                              ┌──────────────────────────────────────────┐
                              │           devenv up (default)            │
                              │                                          │
                              │  Flink │ Polaris │ MinIO │ PostgreSQL   │
                              │  OTEL  │ Prometheus │ NiFi               │
                              └──────────────────────────────────────────┘
                                                  │
                    ┌─────────────────────────────┼─────────────────────────────┐
                    │                             │                             │
                    ▼                             ▼                             ▼
        ┌───────────────────┐         ┌───────────────────┐         ┌───────────────────┐
        │   Local k3d       │         │   Local RKE2      │         │   AWS RKE2        │
        │   (Podman VM)     │         │   (System K8s)    │         │   (EC2 Fleet)     │
        │                   │         │                   │         │                   │
        │   k8s:provision   │         │  (pre-existing)   │         │   aws:provision   │
        │   k8s:deploy-*    │         │  k8s:deploy-*     │         │   aws:deploy      │
        │   k8s:forward     │         │  k8s:forward      │         │   aws:verify      │
        └───────────────────┘         └───────────────────┘         └───────────────────┘
                │                             │                             │
                └─────────────────────────────┴─────────────────────────────┘
                                              │
                              ┌───────────────┴───────────────┐
                              │                               │
                              ▼                               ▼
                  ┌───────────────────┐           ┌───────────────────┐
                  │       Dask        │           │    JupyterHub     │
                  │  (distributed     │           │  (notebooks with  │
                  │   computing)      │           │   Dask client)    │
                  └───────────────────┘           └───────────────────┘
```

## Task Groups

### `k8s:*` — Local and Existing Cluster Deployment

Use for development on your laptop or testing on an existing Kubernetes cluster.

| Task | Description |
|------|-------------|
| `k8s:status` | Check cluster connectivity and deployed resources |
| `k8s:provision` | Create k3d cluster (k3d only, no-op for RKE2) |
| `k8s:deploy-dask` | Deploy Dask operator and cluster via Helm |
| `k8s:deploy-jupyter` | Deploy JupyterHub via Helm |
| `k8s:forward` | Port-forward Dask (8787) and JupyterHub (8000) |
| `k8s:destroy` | Delete k3d cluster (k3d only) |

### `aws:*` — AWS Cloud Deployment

Use for production or team-accessible deployments on AWS.

| Task | Description |
|------|-------------|
| `aws:provision` | Create VPC, EC2, IAM with OpenTofu |
| `aws:inventory` | Generate Ansible inventory from Tofu output |
| `aws:deploy` | Deploy full stack (RKE2 + Dask + JupyterHub) |
| `aws:deploy:dask` | Deploy Dask operator and cluster via Ansible |
| `aws:deploy:jupyterhub` | Deploy JupyterHub with S3 access |
| `aws:deploy:ngrok` | Deploy ngrok operator for HTTPS ingress |
| `aws:apply` | Apply all Ansible configuration |
| `aws:status` | Show infrastructure and service status |
| `aws:verify` | Run post-deployment verification checks |
| `aws:ssh` | SSH to bastion host |
| `aws:logs:ngrok` | View ngrok operator logs |
| `aws:destroy` | Destroy infrastructure (keeps S3 data) |
| `aws:teardown` | Full teardown including S3 cleanup |
| `aws:s3:clean` | Remove objects from S3 bucket |
| `aws:s3:empty` | Empty S3 bucket completely |

## Service Ports

### Kubernetes Stack (when deployed)

| Service | Port | URL |
|---------|------|-----|
| Dask Scheduler | 8786 | (internal) |
| Dask Dashboard | 8787 | http://localhost:8787 |
| JupyterHub | 8000 | http://localhost:8000 |
| k3d API Server | 6550 | (k3d only) |

### Core Stack (always running via devenv up)

| Service | Port | URL |
|---------|------|-----|
| Flink UI | 8081 | http://localhost:8081 |
| Polaris REST | 8181 | http://localhost:8181 |
| RustFS (local S3) Console | 9011 | http://localhost:9011 |
| PostgreSQL | 5438 | (internal) |
| Prometheus | 9090 | http://localhost:9090 |
| NiFi | 8450 | http://localhost:8450 |

## Directory Structure

```
infra/
├── README.md              # This file (overview)
├── LOCAL.md               # Local k3d and RKE2 workflows
├── aws/
│   ├── README.md          # AWS deployment guide
│   ├── tofu/              # OpenTofu modules (VPC, EC2, IAM, S3)
│   └── ansible/           # Ansible playbooks (RKE2, Dask, JupyterHub)
├── benchmarks/
│   ├── README.md          # Benchmark usage guide
│   └── benchmark-job.yaml # Kubernetes Job for Dask benchmarks
└── dask/
    └── dask-cluster.yaml  # Dask cluster manifest for local
```

## Next Steps

- **Local development**: [LOCAL.md](LOCAL.md) — k3d or RKE2 workflows
- **AWS production**: [aws/README.md](aws/README.md) — Full cloud deployment
- **Policy validation**: Run `devenv tasks run policy:check` for config validation
