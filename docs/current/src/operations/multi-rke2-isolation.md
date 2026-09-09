# Multi-RKE2 Instance Isolation

Deploy the Cyberphy Dask stack to a **separate RKE2 instance** on a machine that already runs RKE2, achieving complete isolation without namespace conflicts.

## Why Multi-Instance?

When deploying alongside an existing Dask/Kubernetes workload, namespace isolation is insufficient because:

| Resource | Scope | Conflict Risk |
|----------|-------|---------------|
| CRDs (DaskCluster, etc.) | Cluster-wide | Both operators manage same CRD |
| Dask Operator | Watches all namespaces | Race conditions on reconciliation |
| NodePorts | Cluster-wide | Port 30087 can only be used once |
| CNI Network | Cluster-wide | Pod CIDR collisions |

**Solution**: Run a completely separate RKE2 instance with its own:
- API server on a different port
- Separate Pod/Service CIDRs
- Separate data directory
- Separate NodePort range
- Separate Traefik ingress ports

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Single Machine                              │
│                                                                      │
│  ┌────────────────────────────┐  ┌────────────────────────────┐    │
│  │     RKE2 Primary           │  │     RKE2 Cyberphy          │    │
│  │     (existing)             │  │     (isolated)             │    │
│  │                            │  │                            │    │
│  │  Config:                   │  │  Config:                   │    │
│  │    /etc/rancher/rke2/      │  │    /etc/rancher/rke2-cs/   │    │
│  │                            │  │                            │    │
│  │  Data:                     │  │  Data:                     │    │
│  │    /var/lib/rancher/rke2/  │  │    /var/lib/rancher/rke2-cs│    │
│  │                            │  │                            │    │
│  │  API: :6443                │  │  API: :6444                │    │
│  │  Traefik: :80/:443         │  │  Traefik: :8080/:8443      │    │
│  │  NodePorts: 30000-30999    │  │  NodePorts: 31000-31999    │    │
│  │                            │  │                            │    │
│  │  Pod CIDR: 10.42.0.0/16    │  │  Pod CIDR: 10.52.0.0/16    │    │
│  │  Svc CIDR: 10.43.0.0/16    │  │  Svc CIDR: 10.53.0.0/16    │    │
│  │                            │  │                            │    │
│  │  kubeconfig:               │  │  kubeconfig:               │    │
│  │    ~/.kube/config          │  │    ~/.kube/rke2-cs.yaml    │    │
│  └────────────────────────────┘  └────────────────────────────┘    │
│                                                                      │
│  Both instances share: Host networking, storage, CPU/RAM            │
└─────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Existing RKE2 installation (primary instance)
- Root access
- Sufficient resources (each RKE2 instance needs ~2GB RAM overhead)
- Non-overlapping port ranges available

## Installation

### Step 1: Create Configuration Directory

```bash
sudo mkdir -p /etc/rancher/rke2-cyberphy
sudo mkdir -p /var/lib/rancher/rke2-cyberphy
```

### Step 2: Create RKE2 Configuration

```bash
sudo tee /etc/rancher/rke2-cyberphy/config.yaml << 'EOF'
# RKE2 Cyberphy Instance Configuration
# Isolated from primary RKE2 instance

# Use different ports to avoid conflicts
tls-san:
  - localhost
  - 127.0.0.1

# API server on alternate port
# Note: RKE2 doesn't directly support --https-listen-port in config
# We'll use a wrapper script to set this

# Network configuration - must not overlap with primary instance
cluster-cidr: 10.52.0.0/16
service-cidr: 10.53.0.0/16

# CNI configuration
cni: canal

# Disable servicelb to avoid port conflicts (use NodePort or external LB)
disable:
  - rke2-ingress-nginx  # We'll configure Traefik separately

# Data directory
data-dir: /var/lib/rancher/rke2-cyberphy

# Write kubeconfig to separate location
write-kubeconfig: /etc/rancher/rke2-cyberphy/rke2.yaml
write-kubeconfig-mode: "0644"

# Node labels for identification
node-label:
  - "rke2-instance=cyberphy"

# Kubelet arguments for NodePort range
kubelet-arg:
  - "node-port-range=31000-31999"
EOF
```

### Step 3: Create Systemd Service

```bash
sudo tee /etc/systemd/system/rke2-cyberphy-server.service << 'EOF'
[Unit]
Description=RKE2 Cyberphy Instance - Kubernetes Server
Documentation=https://github.com/rancher/rke2
Wants=network-online.target
After=network-online.target
Conflicts=rke2-agent.service

[Service]
Type=notify
EnvironmentFile=-/etc/default/rke2-cyberphy
EnvironmentFile=-/etc/sysconfig/rke2-cyberphy
Environment="RKE2_CONFIG_FILE=/etc/rancher/rke2-cyberphy/config.yaml"
KillMode=process
Delegate=yes
LimitNOFILE=1048576
LimitNPROC=infinity
LimitCORE=infinity
TasksMax=infinity
TimeoutStartSec=0
Restart=always
RestartSec=5s
ExecStartPre=-/sbin/modprobe br_netfilter
ExecStartPre=-/sbin/modprobe overlay
ExecStart=/usr/local/bin/rke2 server --config /etc/rancher/rke2-cyberphy/config.yaml

[Install]
WantedBy=multi-user.target
EOF
```

### Step 4: Create Traefik Configuration for Alternate Ports

```bash
sudo mkdir -p /var/lib/rancher/rke2-cyberphy/server/manifests

sudo tee /var/lib/rancher/rke2-cyberphy/server/manifests/traefik-config.yaml << 'EOF'
apiVersion: helm.cattle.io/v1
kind: HelmChartConfig
metadata:
  name: traefik
  namespace: kube-system
spec:
  valuesContent: |-
    ports:
      web:
        port: 8080
        exposedPort: 8080
        nodePort: 31080
      websecure:
        port: 8443
        exposedPort: 8443
        nodePort: 31443
    service:
      type: NodePort
EOF
```

### Step 5: Start the Cyberphy RKE2 Instance

```bash
# Reload systemd
sudo systemctl daemon-reload

# Start the cybersec instance
sudo systemctl enable rke2-cyberphy-server
sudo systemctl start rke2-cyberphy-server

# Monitor startup
sudo journalctl -u rke2-cyberphy-server -f
```

### Step 6: Configure kubectl Access

```bash
# Create kubeconfig symlink
mkdir -p ~/.kube
sudo cp /etc/rancher/rke2-cyberphy/rke2.yaml ~/.kube/rke2-cyberphy.yaml
sudo chown $(whoami) ~/.kube/rke2-cyberphy.yaml

# Test connectivity
export KUBECONFIG=~/.kube/rke2-cyberphy.yaml
kubectl get nodes

# Create alias for convenience
echo 'alias kubectl-cp="kubectl --kubeconfig ~/.kube/rke2-cyberphy.yaml"' >> ~/.bashrc
```

## Deploy Cyberphy Dask Stack

With the isolated RKE2 instance running, deploy the Zarf package:

```bash
# Set kubeconfig to cybersec instance
export KUBECONFIG=~/.kube/rke2-cyberphy.yaml

# Initialize Zarf
cd /path/to/airgap-bundle
zarf init --confirm

# Deploy cybersec-dask package
zarf package deploy zarf-package-cybersec-dask-*.tar.zst \
  --set S3_ENDPOINT=http://minio.example.com:9000 \
  --set S3_ACCESS_KEY=minioadmin \
  --set S3_SECRET_KEY=minioadmin \
  --confirm
```

## Access Services

### Port Mapping

| Service | Primary RKE2 | Cyberphy RKE2 |
|---------|--------------|---------------|
| API Server | 6443 | 6443 (same, different kubeconfig) |
| Traefik HTTP | 80 | 8080 |
| Traefik HTTPS | 443 | 8443 |
| Dask Dashboard | 30087 | 31087 |
| Dask Scheduler | 30086 | 31086 |
| JupyterHub | 30080 | 31080 |
| Panel-Viz | 30506 | 31506 |

### Access URLs

```bash
# Get node IP
NODE_IP=$(kubectl --kubeconfig ~/.kube/rke2-cyberphy.yaml get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')

# Services (using NodePort range 31000-31999)
echo "Dask Dashboard: http://${NODE_IP}:31087"
echo "JupyterHub:     http://${NODE_IP}:31080"
echo "Panel-Viz:      http://${NODE_IP}:31506"

# Or via Traefik ingress (port 8080)
echo "Dask:    http://dask.cyberphy.local:8080"
echo "Jupyter: http://jupyter.cyberphy.local:8080"
echo "Panel:   http://panel.cyberphy.local:8080"
```

## Adjusting NodePorts in Manifests

The default Zarf manifests use NodePorts in the 30000 range. For the cybersec instance, patch them after deployment:

```bash
export KUBECONFIG=~/.kube/rke2-cyberphy.yaml

# Patch Dask scheduler NodePorts
kubectl patch svc cybersec-dask-scheduler -n dask --type='json' -p='[
  {"op": "replace", "path": "/spec/ports/0/nodePort", "value": 31086},
  {"op": "replace", "path": "/spec/ports/1/nodePort", "value": 31087}
]'

# Patch Panel-Viz NodePort
kubectl patch svc otel-navigator -n panel-viz --type='json' -p='[
  {"op": "replace", "path": "/spec/ports/0/nodePort", "value": 31506}
]'

# Verify
kubectl get svc -A | grep NodePort
```

## Management Commands

### Switching Between Instances

```bash
# Use primary RKE2
export KUBECONFIG=~/.kube/config
kubectl get nodes  # Shows primary cluster

# Use cybersec RKE2
export KUBECONFIG=~/.kube/rke2-cyberphy.yaml
kubectl get nodes  # Shows cybersec cluster

# Or use aliases
kubectl-cp get pods -A  # Cyberphy instance
kubectl get pods -A     # Primary instance (default)
```

### Service Management

```bash
# Status
sudo systemctl status rke2-server           # Primary
sudo systemctl status rke2-cyberphy-server  # Cyberphy

# Restart
sudo systemctl restart rke2-cyberphy-server

# Logs
sudo journalctl -u rke2-cyberphy-server -f

# Stop (preserves data)
sudo systemctl stop rke2-cyberphy-server
```

### Complete Removal

```bash
# Stop and disable
sudo systemctl stop rke2-cyberphy-server
sudo systemctl disable rke2-cyberphy-server

# Remove systemd service
sudo rm /etc/systemd/system/rke2-cyberphy-server.service
sudo systemctl daemon-reload

# Remove data and config
sudo rm -rf /var/lib/rancher/rke2-cyberphy
sudo rm -rf /etc/rancher/rke2-cyberphy
rm ~/.kube/rke2-cyberphy.yaml
```

## Resource Considerations

Each RKE2 instance runs its own:
- kube-apiserver
- kube-controller-manager
- kube-scheduler
- etcd
- kubelet
- kube-proxy
- CoreDNS
- Traefik (if enabled)

**Memory overhead**: ~2-3 GB per instance
**CPU overhead**: ~0.5-1 core idle, scales with workload

For resource-constrained environments, consider:
- Reducing etcd compaction interval
- Limiting CoreDNS replicas
- Disabling unused components

## Troubleshooting

### Port Conflicts

```bash
# Check what's using a port
sudo ss -tlnp | grep :6443
sudo ss -tlnp | grep :8080

# If primary RKE2 is using expected ports, verify config
cat /etc/rancher/rke2-cyberphy/config.yaml
```

### Network CIDR Conflicts

```bash
# Check current CIDRs
kubectl --kubeconfig ~/.kube/config get nodes -o jsonpath='{.items[*].spec.podCIDR}'
kubectl --kubeconfig ~/.kube/rke2-cyberphy.yaml get nodes -o jsonpath='{.items[*].spec.podCIDR}'

# Should show different ranges (10.42.x.x vs 10.52.x.x)
```

### Instance Won't Start

```bash
# Check logs
sudo journalctl -u rke2-cyberphy-server --no-pager | tail -100

# Common issues:
# - Port 6443 conflict: Primary RKE2 already using it
# - Data directory permissions
# - CNI conflicts

# Verify data directory
ls -la /var/lib/rancher/rke2-cyberphy/
```

### Pods Can't Communicate

```bash
# Verify CNI is running
kubectl --kubeconfig ~/.kube/rke2-cyberphy.yaml get pods -n kube-system | grep canal

# Check pod networking
kubectl --kubeconfig ~/.kube/rke2-cyberphy.yaml run test --image=busybox --rm -it -- ping -c 3 10.53.0.1
```

## Air-Gap Considerations

For air-gapped deployments, ensure the RKE2 images are available:

```bash
# Copy images to cybersec data directory
sudo mkdir -p /var/lib/rancher/rke2-cyberphy/agent/images/
sudo cp /path/to/rke2-images.linux-amd64.tar.zst /var/lib/rancher/rke2-cyberphy/agent/images/
```

The Zarf package deployment process remains the same - just ensure `KUBECONFIG` points to the cybersec instance.

## Related Documentation

- [Air-Gap Deployment](./airgap-deployment.md) - Main deployment guide
- [RKE2 Documentation](https://docs.rke2.io/) - Upstream RKE2 docs
- [Zarf Documentation](https://docs.zarf.dev/) - Zarf packaging system
