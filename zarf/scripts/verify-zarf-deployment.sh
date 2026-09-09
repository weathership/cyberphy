#!/bin/bash
# =============================================================================
# Zarf Deployment & Verification Script — v1.2.1
# =============================================================================
# Unified deployment script that works identically on local dev (tinybox) and
# air-gap (usfwdbig01). All environment differences are resolved through env
# vars (sourced from .env/direnv or manually exported).
#
# BUILD MODE (connected environment):
#   ./verify-zarf-deployment.sh
#   - Builds custom images (requires network for base images)
#   - Creates Zarf package, deploys to cluster
#
# DEPLOY MODE (air-gap environment):
#   ./verify-zarf-deployment.sh --skip-build
#   - Uses pre-built Zarf package and init package
#   - Deploys to cluster without network access
#
# VERIFY MODE:
#   ./verify-zarf-deployment.sh --verify-only
#   - Runs verification checks against existing deployment
#   - No cluster changes, safe to run anytime
#
# Requirements:
#   - Kubernetes cluster running (RKE2, k3d, etc.)
#   - Zarf CLI available
#   - kubectl available and KUBECONFIG resolvable
#   - Podman for building custom images (BUILD MODE only)
# =============================================================================

set -euo pipefail

# =============================================================================
# Paths & Constants
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ZARF_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$ZARF_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# =============================================================================
# CLI Flags
# =============================================================================
SKIP_INIT=false
SKIP_BUILD=false
DRY_RUN=false
DISK_LIGHT=false
DISK_LIGHT_AUTO=false
VERIFY_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-init)   SKIP_INIT=true;   shift ;;
        --skip-build)  SKIP_BUILD=true;  shift ;;
        --dry-run)     DRY_RUN=true;     shift ;;
        --disk-light)  DISK_LIGHT=true;  shift ;;
        --verify-only) VERIFY_ONLY=true; shift ;;
        -h|--help)
            cat <<'USAGE'
Usage: verify-zarf-deployment.sh [OPTIONS]

Options:
  --skip-build     Skip image/package build (use pre-built packages)
  --skip-init      Skip zarf init (already initialized)
  --dry-run        Show what would be done without executing
  --disk-light     Small registry PV (5Gi), emptyDir for spill volumes
                   (auto-enabled when <10% disk free or DiskPressure)
  --verify-only    Jump straight to verification (skip init/deploy)

Environment Variables (override via .env or export):
  KUBECONFIG              Path to kubeconfig (auto-detected if unset)
  S3_ENDPOINT             S3-compatible endpoint URL
  S3_ACCESS_KEY           S3 access key (default: minioadmin)
  S3_SECRET_KEY           S3 secret key (default: minioadmin)
  S3_BUCKET               S3 bucket name (default: cybersec-dask-data)
  S3_REGION               S3 region (default: us-east-1)
  DASK_SPILL_DIR          Host path for spill-to-disk (empty = emptyDir)
  DASK_WORKER_REPLICAS    Number of Dask workers (default: 4)
  LOCAL_S3_PORT           MinIO NodePort (default: 9010)
  REGISTRY_STORAGE_PATH   Registry host path (default: /var/lib/zarf-registry)
  REGISTRY_PVC_SIZE       Registry PV size (default: 5Gi)
USAGE
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# =============================================================================
# Logging
# =============================================================================
log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }
log_step()    { echo -e "\n${CYAN}${BOLD}=== $1 ===${NC}"; }

# =============================================================================
# Source .env (if present)
# =============================================================================
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROJECT_ROOT/.env"
    set +a
fi

# =============================================================================
# Kubeconfig Resolution (5-tier fallback, ported from devenv.nix:2574-2624)
# =============================================================================
_resolve_kubeconfig() {
    # 1. Explicit KUBECONFIG env var
    if [[ -n "${KUBECONFIG:-}" ]] && [[ -f "$KUBECONFIG" ]]; then
        if [[ -r "$KUBECONFIG" ]]; then
            log_info "Using KUBECONFIG=$KUBECONFIG"
            return 0
        else
            log_warn "KUBECONFIG=$KUBECONFIG exists but is not readable"
        fi
    fi
    # 2. User-readable copy at ~/.kube/rke2.yaml
    if [[ -f "$HOME/.kube/rke2.yaml" ]] && [[ -r "$HOME/.kube/rke2.yaml" ]]; then
        KUBECONFIG="$HOME/.kube/rke2.yaml"
        export KUBECONFIG
        log_info "Using user kubeconfig: $KUBECONFIG"
        return 0
    fi
    # 3. k3d kubeconfig
    if [[ "${CYBERSEC_K8S_TARGET:-}" == "k3d" ]]; then
        KUBECONFIG="${DEVENV_STATE:-.devenv/state}/kubeconfig"
        export KUBECONFIG
        log_info "Using k3d kubeconfig: $KUBECONFIG"
        return 0
    fi
    # 4. System RKE2 kubeconfig (may need permission fix)
    if [[ -f "/etc/rancher/rke2/rke2.yaml" ]]; then
        if [[ -r "/etc/rancher/rke2/rke2.yaml" ]]; then
            KUBECONFIG="/etc/rancher/rke2/rke2.yaml"
            export KUBECONFIG
            log_info "Using RKE2 kubeconfig: $KUBECONFIG"
            return 0
        else
            log_error "RKE2 kubeconfig exists but is not readable: /etc/rancher/rke2/rke2.yaml"
            echo ""
            echo "Fix with:"
            echo "  sudo cp /etc/rancher/rke2/rke2.yaml ~/.kube/rke2.yaml"
            echo "  sudo chown \$(id -u):\$(id -g) ~/.kube/rke2.yaml"
            echo "  export KUBECONFIG=~/.kube/rke2.yaml"
            return 1
        fi
    fi
    # 5. Default kubeconfig
    if [[ -f "$HOME/.kube/config" ]] && [[ -r "$HOME/.kube/config" ]]; then
        KUBECONFIG="$HOME/.kube/config"
        export KUBECONFIG
        log_info "Using default kubeconfig: $KUBECONFIG"
        return 0
    fi
    log_error "No kubeconfig found. Set KUBECONFIG or install a local cluster."
    return 1
}

# =============================================================================
# Node IP Detection (IPv4 only — see MEMORY.md multi-IP gotcha)
# =============================================================================
detect_node_ip() {
    NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null | awk '{print $1}')
    if [[ -z "$NODE_IP" ]]; then
        log_warn "Could not detect node IP from cluster, falling back to 127.0.0.1"
        NODE_IP="127.0.0.1"
    fi
    export NODE_IP
}

# =============================================================================
# Resolve Environment Variable Defaults
# =============================================================================
resolve_env_defaults() {
    # S3 / MinIO
    LOCAL_S3_PORT="${LOCAL_S3_PORT:-9010}"
    S3_ENDPOINT="${S3_ENDPOINT:-http://${NODE_IP}:${LOCAL_S3_PORT}}"
    S3_ACCESS_KEY="${S3_ACCESS_KEY:-${MINIO_ACCESS_KEY:-minioadmin}}"
    S3_SECRET_KEY="${S3_SECRET_KEY:-${MINIO_SECRET_KEY:-minioadmin}}"
    S3_BUCKET="${S3_BUCKET:-cybersec-dask-data}"
    S3_REGION="${S3_REGION:-us-east-1}"

    # Dask
    DASK_WORKER_REPLICAS="${DASK_WORKER_REPLICAS:-4}"
    DASK_SPILL_DIR="${DASK_SPILL_DIR:-}"

    # Registry
    REGISTRY_STORAGE_PATH="${REGISTRY_STORAGE_PATH:-/var/lib/zarf-registry}"
    REGISTRY_PVC_SIZE="${REGISTRY_PVC_SIZE:-5Gi}"

    export LOCAL_S3_PORT S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET S3_REGION
    export DASK_WORKER_REPLICAS DASK_SPILL_DIR
    export REGISTRY_STORAGE_PATH REGISTRY_PVC_SIZE
}

# =============================================================================
# Disk Constraint Detection (auto-enables --disk-light)
# =============================================================================
detect_disk_constraints() {
    if [[ "$DISK_LIGHT" == "true" ]]; then
        return 0
    fi

    # Check free disk on /var/lib/rancher or /
    local check_path="/var/lib/rancher"
    if [[ ! -d "$check_path" ]]; then
        check_path="/"
    fi
    local pct_free
    pct_free=$(df "$check_path" 2>/dev/null | awk 'NR==2 {gsub(/%/,"",$5); print 100-$5}') || true
    if [[ -n "$pct_free" ]] && [[ "$pct_free" -lt 10 ]]; then
        log_warn "Low disk: ${pct_free}% free on $check_path — enabling disk-light mode"
        DISK_LIGHT=true
        DISK_LIGHT_AUTO=true
    fi

    # Check for DiskPressure condition on any node
    local disk_pressure=0
    disk_pressure=$(kubectl get nodes -o json 2>/dev/null \
        | grep -c '"node.kubernetes.io/disk-pressure"') || true
    if [[ "$disk_pressure" -gt 0 ]]; then
        log_warn "DiskPressure taint detected — enabling disk-light mode"
        DISK_LIGHT=true
        DISK_LIGHT_AUTO=true
    fi
}

# =============================================================================
# Config Summary
# =============================================================================
print_config_summary() {
    echo ""
    echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║  Zarf Deployment — v1.2.1                                    ║${NC}"
    echo -e "${BOLD}╠══════════════════════════════════════════════════════════════╣${NC}"
    printf "${BOLD}║${NC} %-18s %s\n" "KUBECONFIG:" "$KUBECONFIG"
    printf "${BOLD}║${NC} %-18s %s\n" "NODE_IP:" "$NODE_IP"
    printf "${BOLD}║${NC} %-18s %s\n" "S3_ENDPOINT:" "$S3_ENDPOINT"
    printf "${BOLD}║${NC} %-18s %s\n" "S3_BUCKET:" "$S3_BUCKET"
    printf "${BOLD}║${NC} %-18s %s\n" "Workers:" "$DASK_WORKER_REPLICAS"
    printf "${BOLD}║${NC} %-18s %s\n" "Spill Dir:" "${DASK_SPILL_DIR:-<emptyDir>}"
    printf "${BOLD}║${NC} %-18s %s\n" "Registry PV:" "$REGISTRY_PVC_SIZE @ $REGISTRY_STORAGE_PATH"
    printf "${BOLD}║${NC} %-18s %s\n" "Flags:" "skip-build=$SKIP_BUILD skip-init=$SKIP_INIT dry-run=$DRY_RUN disk-light=$DISK_LIGHT verify-only=$VERIFY_ONLY"
    echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"

    if [[ "$DISK_LIGHT" == "true" ]]; then
        local trigger="--disk-light flag"
        if [[ "$DISK_LIGHT_AUTO" == "true" ]]; then
            trigger="auto-detected disk constraints"
        fi
        echo ""
        echo -e "${YELLOW}  DISK-LIGHT MODE ACTIVE (${trigger})${NC}"
        echo -e "${YELLOW}  Registry: hostPath PV ($REGISTRY_PVC_SIZE label, ~2 GB actual)${NC}"
        echo -e "${YELLOW}  Spill: emptyDir 512Mi (post-deploy patch)${NC}"
    fi
    echo ""
}

# =============================================================================
# Step 0: Baseline Check
# =============================================================================
check_baseline() {
    log_step "Step 0: Environment Baseline"

    # Disk usage on key paths
    log_info "Disk usage:"
    df -h / 2>/dev/null | head -2
    for path in /raid /var/lib/rancher; do
        if [[ -d "$path" ]]; then
            df -h "$path" 2>/dev/null | tail -1
        fi
    done

    # Node status
    log_info "Cluster nodes:"
    kubectl get nodes -o wide 2>/dev/null || log_warn "Cannot reach cluster"

    # DiskPressure per node
    log_info "DiskPressure status:"
    kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}: DiskPressure={range .status.conditions[?(@.type=="DiskPressure")]}{.status}{end}{"\n"}{end}' 2>/dev/null || true

    # Existing non-system pods
    local pod_count
    pod_count=$(kubectl get pods -A --no-headers 2>/dev/null \
        | grep -v -E '^kube-system\s' | wc -l) || pod_count=0
    log_info "Non-system pods: $pod_count"
}

# =============================================================================
# Step 1: Eviction Config Check
# =============================================================================
check_eviction_config() {
    log_step "Step 1: Kubelet Eviction Config"

    # Only relevant if disk is above 85% used
    local check_path="/var/lib/rancher"
    if [[ ! -d "$check_path" ]]; then
        check_path="/"
    fi
    local pct_used
    pct_used=$(df "$check_path" 2>/dev/null | awk 'NR==2 {gsub(/%/,"",$5); print $5}') || true

    if [[ -z "$pct_used" ]] || [[ "$pct_used" -lt 85 ]]; then
        log_success "Disk usage ${pct_used:-unknown}% — default eviction thresholds OK"
        return 0
    fi

    log_warn "Disk usage ${pct_used}% — checking custom eviction thresholds"

    local config="/etc/rancher/rke2/config.yaml"
    if [[ -f "$config" ]] && grep -q "eviction-hard" "$config" 2>/dev/null; then
        log_success "Custom eviction thresholds found in $config"
        grep -E "eviction|image-gc" "$config" 2>/dev/null | head -10
        return 0
    fi

    echo ""
    echo -e "${YELLOW}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${YELLOW}║  WARNING: Disk ${pct_used}% used with default eviction thresholds    ${NC}"
    echo -e "${YELLOW}║  Kubelet may evict pods at 95% (default hard=5% free).      ${NC}"
    echo -e "${YELLOW}║                                                              ${NC}"
    echo -e "${YELLOW}║  Recommended: add to /etc/rancher/rke2/config.yaml:          ${NC}"
    echo -e "${YELLOW}║                                                              ${NC}"
    echo -e "${YELLOW}║  kubelet-arg:                                                ${NC}"
    echo -e "${YELLOW}║    - eviction-hard=nodefs.available<2%,imagefs.available<2%  ${NC}"
    echo -e "${YELLOW}║    - eviction-soft=nodefs.available<5%,imagefs.available<5%  ${NC}"
    echo -e "${YELLOW}║    - image-gc-high-threshold=99                              ${NC}"
    echo -e "${YELLOW}║                                                              ${NC}"
    echo -e "${YELLOW}║  Then: sudo systemctl restart rke2-server                    ${NC}"
    echo -e "${YELLOW}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    return 0  # Non-fatal: warn only
}

# =============================================================================
# Step 2: Check Prerequisites
# =============================================================================
check_prerequisites() {
    log_step "Step 2: Checking Prerequisites"

    local errors=0

    # Check kubectl connectivity
    if kubectl get nodes &>/dev/null; then
        log_success "kubectl connected to cluster"
    else
        log_error "kubectl cannot connect to cluster (KUBECONFIG=$KUBECONFIG)"
        ((errors++))
    fi

    # Check Zarf CLI
    if command -v zarf &>/dev/null; then
        log_success "Zarf CLI: $(zarf version 2>/dev/null || echo 'unknown')"
    else
        log_error "Zarf CLI not found in PATH"
        ((errors++))
    fi

    # Check Podman (only required for build mode)
    if [[ "$SKIP_BUILD" == "false" ]]; then
        if command -v podman &>/dev/null; then
            log_success "Podman: $(podman --version 2>/dev/null)"
        else
            log_warn "Podman not found — image builds will fail"
        fi
    fi

    # Check zarf.yaml exists
    if [[ -f "$ZARF_DIR/zarf.yaml" ]]; then
        log_success "zarf.yaml found"
    else
        log_error "zarf.yaml not found at $ZARF_DIR/zarf.yaml"
        ((errors++))
    fi

    if [[ $errors -gt 0 ]]; then
        log_error "Prerequisites check failed ($errors error(s))"
        return 1
    fi

    log_success "All prerequisites passed"
}

# =============================================================================
# Step 3: Verify Injector Prerequisites (kube-system pods for Zarf bootstrap)
# =============================================================================
verify_injector_prerequisites() {
    log_step "Step 3: Verifying Injector Prerequisites"

    log_info "Checking for running kube-system pods (required for Zarf injector)..."

    local max_wait=120
    local waited=0
    local running_pods=0

    while [[ $waited -lt $max_wait ]]; do
        running_pods=$(kubectl get pods -n kube-system --no-headers 2>/dev/null \
            | grep -c "Running" || echo "0")
        if [[ "$running_pods" -ge 2 ]]; then
            log_success "Found $running_pods running pods in kube-system"
            break
        fi
        log_info "Waiting for kube-system pods... ($waited/${max_wait}s)"
        sleep 5
        ((waited+=5))
    done

    if [[ "$running_pods" -lt 2 ]]; then
        log_error "Insufficient running pods in kube-system (found: $running_pods, need: 2+)"
        kubectl get pods -n kube-system 2>/dev/null || true
        return 1
    fi

    # Check for common injector targets
    local coredns_count
    coredns_count=$(kubectl get pods -n kube-system -l k8s-app=kube-dns --no-headers 2>/dev/null \
        | grep -c "Running" || echo "0")
    if [[ "$coredns_count" -ge 1 ]]; then
        log_success "CoreDNS running — suitable for Zarf injector"
    fi
}

# =============================================================================
# Step 4: Setup Storage for Zarf Registry
# =============================================================================
setup_storage() {
    log_step "Step 4: Setting Up Registry Storage"

    local PV_SIZE="$REGISTRY_PVC_SIZE"
    local STORAGE_PATH="$REGISTRY_STORAGE_PATH"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would create $STORAGE_PATH and ${PV_SIZE} PV"
        return 0
    fi

    # Create storage directory (sudo only for mkdir/chown on host path)
    log_info "Creating storage directory: $STORAGE_PATH"
    sudo mkdir -p "$STORAGE_PATH"
    sudo chown 1000:2000 "$STORAGE_PATH"
    sudo chmod 777 "$STORAGE_PATH"
    # SELinux context (RHEL/Rocky — harmless no-op elsewhere)
    sudo chcon -R -t container_file_t "$STORAGE_PATH" 2>/dev/null || true

    # Clean up stuck PVC/PV from previous failed init (Lost phase, stuck finalizers)
    local pvc_phase
    pvc_phase=$(kubectl get pvc zarf-docker-registry -n zarf \
        -o jsonpath='{.status.phase}' 2>/dev/null) || true
    if [[ "$pvc_phase" == "Lost" ]]; then
        log_warn "Found PVC in Lost phase — cleaning up stuck state"
        kubectl patch pvc zarf-docker-registry -n zarf \
            -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
        kubectl delete pvc zarf-docker-registry -n zarf \
            --force --grace-period=0 2>/dev/null || true
        kubectl patch pv zarf-registry-pv \
            -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
        kubectl delete pv zarf-registry-pv \
            --force --grace-period=0 2>/dev/null || true
        log_success "Cleaned up stuck PVC/PV"
    fi

    # Check if PV already exists and is healthy
    local pv_phase
    pv_phase=$(kubectl get pv zarf-registry-pv \
        -o jsonpath='{.status.phase}' 2>/dev/null) || true
    if [[ "$pv_phase" == "Available" || "$pv_phase" == "Bound" ]]; then
        log_success "PV zarf-registry-pv already exists (phase: $pv_phase)"
        return 0
    fi

    # Create PV with claimRef pre-binding to zarf-docker-registry PVC
    log_info "Creating PersistentVolume ($PV_SIZE)..."
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolume
metadata:
  name: zarf-registry-pv
spec:
  capacity:
    storage: $PV_SIZE
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  hostPath:
    path: $STORAGE_PATH
    type: DirectoryOrCreate
  claimRef:
    namespace: zarf
    name: zarf-docker-registry
EOF
    log_success "Created PV for Zarf registry ($PV_SIZE)"

    # Also install local-path-provisioner if no default StorageClass exists.
    # This ensures future PVCs (JupyterHub persistent DB, etc.) can bind dynamically.
    local default_sc
    default_sc=$(kubectl get storageclass \
        -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")]}{.metadata.name}{end}' \
        2>/dev/null) || true
    if [[ -z "$default_sc" ]]; then
        local manifest_path="$ZARF_DIR/manifests/local-path-provisioner.yaml"
        if [[ -f "$manifest_path" ]]; then
            log_info "No default StorageClass — installing local-path-provisioner from vendored manifest"
            kubectl apply -f "$manifest_path" 2>/dev/null || true
            kubectl wait --for=condition=ready pod -l app=local-path-provisioner \
                -n local-path-storage --timeout=60s 2>/dev/null || true
            log_success "local-path StorageClass installed and set as default"
        fi
    fi
}

# =============================================================================
# Step 5: Initialize Zarf
# =============================================================================
initialize_zarf() {
    log_step "Step 5: Initializing Zarf"

    if [[ "$SKIP_INIT" == "true" ]]; then
        log_info "Skipping Zarf init (--skip-init)"
        return 0
    fi

    # Check if already initialized
    if kubectl get svc zarf-docker-registry -n zarf &>/dev/null; then
        local registry_phase
        registry_phase=$(kubectl get pods -n zarf -l app=docker-registry \
            -o jsonpath='{.items[*].status.phase}' 2>/dev/null) || true
        if [[ "$registry_phase" == *"Running"* ]]; then
            log_success "Zarf already initialized — registry is running"
            return 0
        fi
    fi

    cd "$ZARF_DIR"

    # Find or download init package
    local init_pkg
    init_pkg=$(ls -t "$ZARF_DIR"/zarf-init-amd64-*.tar.zst 2>/dev/null | head -1) || true

    if [[ -z "$init_pkg" ]]; then
        if [[ "$SKIP_BUILD" == "true" ]]; then
            log_error "No zarf-init package found and --skip-build set"
            return 1
        fi
        if [[ -n "${AIRGAP:-}" ]]; then
            log_error "No zarf-init package found in $ZARF_DIR and AIRGAP mode is set."
            log_error "Pre-download in a connected environment: zarf tools download-init"
            return 1
        fi
        log_info "Downloading Zarf init package..."
        if [[ "$DRY_RUN" == "true" ]]; then
            log_info "[DRY-RUN] Would run: zarf tools download-init"
            return 0
        fi
        zarf tools download-init
        init_pkg=$(ls -t "$ZARF_DIR"/zarf-init-amd64-*.tar.zst 2>/dev/null | head -1) || true
        if [[ -z "$init_pkg" ]]; then
            log_error "Failed to download init package"
            return 1
        fi
    fi
    log_success "Init package: $(basename "$init_pkg")"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would run: zarf init --confirm --set REGISTRY_PVC_SIZE=$REGISTRY_PVC_SIZE"
        return 0
    fi

    log_info "Running zarf init..."
    if zarf init --confirm --set REGISTRY_PVC_SIZE="$REGISTRY_PVC_SIZE" 2>&1 \
        | tee /tmp/zarf-init.log | tail -30; then
        log_success "Zarf initialized successfully"
    else
        log_error "Zarf init failed — see /tmp/zarf-init.log"
        tail -50 /tmp/zarf-init.log
        return 1
    fi

    # Verify
    if kubectl get ns zarf &>/dev/null; then
        log_success "Zarf namespace created"
        kubectl get pods -n zarf
    else
        log_error "Zarf namespace not found after init"
        return 1
    fi
}

# =============================================================================
# Auto-detect Package Version (glob instead of hardcoded)
# =============================================================================
detect_package_file() {
    PKG_FILE=$(ls -t "$ZARF_DIR"/zarf-package-cybersec-dask-amd64-*.tar.zst 2>/dev/null | head -1) || true
    if [[ -z "$PKG_FILE" ]]; then
        log_error "No cybersec-dask package found in $ZARF_DIR"
        log_info "Expected: zarf-package-cybersec-dask-amd64-*.tar.zst"
        return 1
    fi
    export PKG_FILE
    log_success "Package: $(basename "$PKG_FILE")"
}

# =============================================================================
# Step 6: Deploy Zarf Package (with S3/Dask env vars)
# =============================================================================
deploy_package() {
    log_step "Step 6: Deploying Cyberphy Dask Package"

    log_info "Workers: $DASK_WORKER_REPLICAS"
    log_info "Spill dir: ${DASK_SPILL_DIR:-<emptyDir post-patch>}"
    log_info "S3 endpoint: $S3_ENDPOINT"
    log_info "S3 bucket: $S3_BUCKET"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would run:"
        echo "  zarf package deploy $(basename "$PKG_FILE") --confirm \\"
        echo "    --set DASK_WORKER_REPLICAS=$DASK_WORKER_REPLICAS \\"
        echo "    --set S3_ENDPOINT=$S3_ENDPOINT \\"
        echo "    --set S3_BUCKET=$S3_BUCKET \\"
        echo "    --set S3_REGION=$S3_REGION \\"
        echo "    --set S3_ACCESS_KEY=*** \\"
        echo "    --set S3_SECRET_KEY=*** \\"
        if [[ -n "$DASK_SPILL_DIR" ]]; then
            echo "    --set DASK_SPILL_DIR=$DASK_SPILL_DIR"
        else
            echo "    (DASK_SPILL_DIR unset — will patch to emptyDir post-deploy)"
        fi
        return 0
    fi

    cd "$ZARF_DIR"

    # Build the deploy command with conditional DASK_SPILL_DIR
    if zarf package deploy "$PKG_FILE" --confirm \
        --set DASK_WORKER_REPLICAS="$DASK_WORKER_REPLICAS" \
        --set S3_ENDPOINT="$S3_ENDPOINT" \
        --set S3_BUCKET="$S3_BUCKET" \
        --set S3_REGION="$S3_REGION" \
        --set S3_ACCESS_KEY="$S3_ACCESS_KEY" \
        --set S3_SECRET_KEY="$S3_SECRET_KEY" \
        ${DASK_SPILL_DIR:+--set DASK_SPILL_DIR="$DASK_SPILL_DIR"} \
        2>&1 | tee /tmp/zarf-deploy.log | tail -30; then
        log_success "Package deployed"
    else
        log_error "Deploy failed — see /tmp/zarf-deploy.log"
        tail -50 /tmp/zarf-deploy.log
        return 1
    fi
}

# =============================================================================
# Post-Deploy: Spill Volume Patch (emptyDir fallback)
# Ported from devenv.nix:2861-2880
# =============================================================================
post_deploy_spill_patch() {
    # If DASK_SPILL_DIR is empty or the path doesn't exist on the node,
    # patch DaskCluster to use emptyDir instead of hostPath
    if [[ -n "$DASK_SPILL_DIR" ]]; then
        log_info "Spill dir set ($DASK_SPILL_DIR) — keeping hostPath volume"
        return 0
    fi

    log_step "Post-Deploy: Patching spill volume to emptyDir"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would patch daskcluster spill volume to emptyDir (512Mi)"
        return 0
    fi

    if kubectl get daskcluster cybersec-dask -n dask &>/dev/null; then
        if kubectl patch daskcluster cybersec-dask -n dask --type=json \
            -p '[{"op":"replace","path":"/spec/worker/spec/volumes/0","value":{"name":"dask-spill","emptyDir":{"sizeLimit":"512Mi"}}}]' \
            2>/dev/null; then
            log_success "Patched spill volume to emptyDir (512Mi)"
        else
            log_warn "Could not patch spill volume (may not have volume at index 0)"
            return 0
        fi

        # Restart workers to pick up volume change
        log_info "Restarting dask workers..."
        kubectl rollout restart deployment -n dask -l dask.org/component=worker 2>/dev/null \
            || kubectl delete pods -n dask -l dask.org/component=worker 2>/dev/null \
            || true
        log_success "Workers restarting with emptyDir spill"
    else
        log_warn "DaskCluster not found — skipping spill patch"
    fi
}

# =============================================================================
# Step 7: Deploy Kubernetes Dashboard (optional, non-fatal)
# =============================================================================
deploy_dashboard() {
    log_step "Step 7: Kubernetes Dashboard (optional)"

    local dashboard_dir="$ZARF_DIR/kubernetes-dashboard"
    local dashboard_pkg
    dashboard_pkg=$(ls -t "$dashboard_dir"/zarf-package-cybersec-k8s-dashboard-*.tar.zst 2>/dev/null | head -1) || true

    if [[ -z "$dashboard_pkg" ]]; then
        log_info "No dashboard package found — skipping"
        return 0
    fi

    # Check if already deployed
    if kubectl get ns kubernetes-dashboard &>/dev/null; then
        log_success "Kubernetes Dashboard already deployed"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would deploy: $(basename "$dashboard_pkg")"
        return 0
    fi

    log_info "Deploying Kubernetes Dashboard..."
    if zarf package deploy "$dashboard_pkg" --confirm 2>&1 | tail -10; then
        log_success "Kubernetes Dashboard deployed"
    else
        log_warn "Dashboard deploy failed (non-fatal)"
    fi
}

# =============================================================================
# Step 8: Comprehensive Verification
# =============================================================================
verify_deployment() {
    log_step "Step 8: Verifying Deployment"

    local errors=0
    local warnings=0

    # --- Namespaces ---
    log_info "Checking namespaces..."
    local expected_ns="zarf dask-operator dask jupyterhub panel-viz"
    for ns in $expected_ns; do
        if kubectl get ns "$ns" &>/dev/null; then
            log_success "Namespace: $ns"
        else
            log_warn "Namespace missing: $ns"
            ((warnings++))
        fi
    done

    # --- Pod Status Checks ---
    log_info "Checking pod status..."

    # Helper: check pod phase by label
    _check_pod() {
        local ns="$1" label="$2" name="$3" required="${4:-true}"
        local phase
        phase=$(kubectl get pods -n "$ns" -l "$label" \
            -o jsonpath='{.items[0].status.phase}' 2>/dev/null) || true
        if [[ "$phase" == "Running" ]]; then
            log_success "$name: Running"
            return 0
        elif [[ "$required" == "true" ]]; then
            log_error "$name: ${phase:-NotFound}"
            ((errors++))
            return 1
        else
            log_warn "$name: ${phase:-NotFound}"
            ((warnings++))
            return 1
        fi
    }

    _check_pod "zarf"          "app=docker-registry"                               "Zarf Registry"
    _check_pod "dask-operator" "app.kubernetes.io/name=dask-kubernetes-operator"    "Dask Operator"
    _check_pod "dask"          "dask.org/component=scheduler"                       "Dask Scheduler"
    _check_pod "jupyterhub"    "component=hub"                                      "JupyterHub"        "false"
    _check_pod "jupyterhub"    "component=proxy"                                    "JupyterHub Proxy"  "false"
    _check_pod "panel-viz"     "app=otel-navigator"                                 "OTEL Navigator"    "false"
    _check_pod "panel-viz"     "app=navigator-engine"                               "Navigator Engine"  "false"

    # Worker count
    log_info "Checking Dask workers..."
    local actual_workers
    actual_workers=$(kubectl get pods -n dask -l dask.org/component=worker \
        --no-headers 2>/dev/null | grep -c "Running" || echo "0")
    if [[ "$actual_workers" -ge "$DASK_WORKER_REPLICAS" ]]; then
        log_success "Dask Workers: $actual_workers/$DASK_WORKER_REPLICAS running"
    else
        log_warn "Dask Workers: $actual_workers/$DASK_WORKER_REPLICAS running"
        ((warnings++))
    fi

    # --- HTTP Endpoint Checks ---
    log_info "Checking HTTP endpoints (via NODE_IP=$NODE_IP)..."

    _check_http() {
        local url="$1" name="$2"
        local code
        code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$url" 2>/dev/null || echo "000")
        if echo "$code" | grep -qE "^(200|301|302)$"; then
            log_success "$name: HTTP $code ($url)"
        else
            log_warn "$name: HTTP $code ($url)"
            ((warnings++))
        fi
    }

    _check_http "http://${NODE_IP}:30087/" "Dask Dashboard"
    _check_http "http://${NODE_IP}:30506/" "OTEL Navigator"
    _check_http "http://${NODE_IP}:30080/" "JupyterHub"

    # --- S3 Bucket Check ---
    log_info "Checking S3 bucket ($S3_BUCKET)..."
    local scheduler_pod
    scheduler_pod=$(kubectl get pod -n dask -l dask.org/component=scheduler \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true
    if [[ -n "$scheduler_pod" ]]; then
        if kubectl exec -n dask "$scheduler_pod" -- python -c "
import s3fs, os
fs = s3fs.S3FileSystem(
    key=os.environ.get('AWS_ACCESS_KEY_ID', ''),
    secret=os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
    client_kwargs={'endpoint_url': os.environ.get('S3_ENDPOINT', '')}
)
try:
    fs.ls('$S3_BUCKET')
    print('exists')
except Exception:
    print('missing')
" 2>/dev/null | grep -q "exists"; then
            log_success "S3 bucket '$S3_BUCKET' exists"
        else
            log_warn "S3 bucket '$S3_BUCKET' not found or not accessible"
            log_info "Create manually: kubectl exec -n dask $scheduler_pod -- python -c \"import s3fs, os; fs = s3fs.S3FileSystem(key=os.environ['AWS_ACCESS_KEY_ID'], secret=os.environ['AWS_SECRET_ACCESS_KEY'], client_kwargs={'endpoint_url': os.environ['S3_ENDPOINT']}); fs.mkdir('$S3_BUCKET')\""
            ((warnings++))
        fi
    else
        log_warn "No scheduler pod — skipping S3 check"
        ((warnings++))
    fi

    # --- Dask Connectivity Check ---
    log_info "Checking Dask cluster connectivity..."
    local engine_pod
    engine_pod=$(kubectl get pod -n panel-viz -l app=navigator-engine \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true
    if [[ -n "$engine_pod" ]]; then
        local worker_count
        worker_count=$(kubectl exec -n panel-viz "$engine_pod" -- python -c "
from dask.distributed import Client
c = Client('tcp://cybersec-dask-scheduler.dask.svc.cluster.local:8786', timeout='10s')
print(len(c.scheduler_info()['workers']))
c.close()
" 2>/dev/null) || true
        if [[ -n "$worker_count" ]] && [[ "$worker_count" -gt 0 ]]; then
            log_success "Dask cluster: $worker_count workers connected"
        else
            log_warn "Dask cluster connectivity check failed"
            ((warnings++))
        fi
    else
        log_warn "No navigator-engine pod — skipping Dask connectivity check"
        ((warnings++))
    fi

    # --- DiskPressure Check ---
    log_info "Checking DiskPressure..."
    local dp_nodes
    dp_nodes=$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}={range .status.conditions[?(@.type=="DiskPressure")]}{.status}{end}{" "}{end}' 2>/dev/null) || true
    local dp_found=false
    for entry in $dp_nodes; do
        local node_name="${entry%%=*}"
        local dp_status="${entry##*=}"
        if [[ "$dp_status" == "True" ]]; then
            log_warn "DiskPressure=True on $node_name"
            dp_found=true
            ((warnings++))
        fi
    done
    if [[ "$dp_found" == "false" ]]; then
        log_success "No DiskPressure on any node"
    fi

    # --- DaskCluster Status ---
    log_info "DaskCluster status:"
    kubectl get daskcluster -n dask 2>/dev/null || log_warn "No DaskCluster found"

    # --- Summary Table ---
    echo ""
    echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║  Deployment Summary                                          ║${NC}"
    echo -e "${BOLD}╠══════════════════════════════════════════════════════════════╣${NC}"

    _summary_line() {
        local label="$1" ns="$2" selector="$3"
        local phase
        phase=$(kubectl get pods -n "$ns" -l "$selector" \
            -o jsonpath='{.items[0].status.phase}' 2>/dev/null) || phase="N/A"
        printf "${BOLD}║${NC}  %-22s %s\n" "$label:" "$phase"
    }

    _summary_line "Zarf Registry"     "zarf"          "app=docker-registry"
    _summary_line "Dask Operator"     "dask-operator"  "app.kubernetes.io/name=dask-kubernetes-operator"
    _summary_line "Dask Scheduler"    "dask"           "dask.org/component=scheduler"
    printf "${BOLD}║${NC}  %-22s %s\n" "Dask Workers:" "$actual_workers/$DASK_WORKER_REPLICAS"
    _summary_line "JupyterHub"        "jupyterhub"     "component=hub"
    _summary_line "OTEL Navigator"    "panel-viz"      "app=otel-navigator"
    _summary_line "Navigator Engine"  "panel-viz"      "app=navigator-engine"

    echo -e "${BOLD}╠══════════════════════════════════════════════════════════════╣${NC}"
    printf "${BOLD}║${NC}  %-22s %s\n" "Dask Dashboard:" "http://${NODE_IP}:30087"
    printf "${BOLD}║${NC}  %-22s %s\n" "OTEL Navigator:" "http://${NODE_IP}:30506"
    printf "${BOLD}║${NC}  %-22s %s\n" "JupyterHub:" "http://${NODE_IP}:30080"
    echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    if [[ $errors -gt 0 ]]; then
        log_error "Verification: $errors error(s), $warnings warning(s)"
        return 1
    elif [[ $warnings -gt 0 ]]; then
        log_warn "Verification: $warnings warning(s), 0 errors"
        return 0
    else
        log_success "Verification: all checks passed"
        return 0
    fi
}

# =============================================================================
# Build-Mode Steps (gated behind --skip-build)
# =============================================================================
start_local_registry() {
    log_step "Build: Starting Local Registry"

    if [[ "$SKIP_BUILD" == "true" ]]; then
        log_info "Skipping (--skip-build)"
        return 0
    fi

    if ! command -v podman &>/dev/null; then
        log_warn "Podman not available — skipping local registry"
        return 0
    fi

    if podman ps --format '{{.Names}}' | grep -q '^registry$'; then
        log_success "Local registry already running"
        return 0
    fi

    if podman ps -a --format '{{.Names}}' | grep -q '^registry$'; then
        log_info "Starting existing registry container..."
        podman start registry
    else
        log_info "Creating new registry container..."
        podman run -d --name registry -p 5555:5000 registry:2
    fi

    sleep 2
    if curl -s http://localhost:5555/v2/ &>/dev/null; then
        log_success "Local registry running on port 5555"
    else
        log_error "Local registry failed to start"
        return 1
    fi
}

build_custom_image() {
    log_step "Build: Custom Dask Image"

    if [[ "$SKIP_BUILD" == "true" ]]; then
        log_info "Skipping (--skip-build)"
        return 0
    fi

    if ! command -v podman &>/dev/null; then
        log_error "Podman required for building images"
        return 1
    fi

    local DOCKERFILE="$ZARF_DIR/images/Dockerfile.cybersec-dask"
    # Must match zarf.yaml / manifests (h5py + holoviews baked — no runtime pip)
    local IMAGE_TAG="localhost:5555/cybersec-dask:2025.2.0-notebook"

    if [[ ! -f "$DOCKERFILE" ]]; then
        log_error "Dockerfile not found: $DOCKERFILE"
        return 1
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would build: $IMAGE_TAG"
        return 0
    fi

    log_info "Building: $IMAGE_TAG"
    cd "$ZARF_DIR/images"

    if podman build -t "$IMAGE_TAG" -f Dockerfile.cybersec-dask . 2>&1 | tail -10; then
        log_success "Image built: $IMAGE_TAG"
    else
        log_error "Image build failed"
        return 1
    fi

    log_info "Pushing to local registry..."
    if podman push --tls-verify=false "$IMAGE_TAG" 2>&1 | tail -5; then
        log_success "Image pushed"
    else
        log_error "Image push failed"
        return 1
    fi

    cd "$ZARF_DIR"
}

build_zarf_package() {
    log_step "Build: Zarf Package"

    if [[ "$SKIP_BUILD" == "true" ]]; then
        log_info "Skipping (--skip-build)"
        return 0
    fi

    # Check if package already exists
    if ls "$ZARF_DIR"/zarf-package-cybersec-dask-amd64-*.tar.zst &>/dev/null; then
        log_success "Package already exists"
        ls -lh "$ZARF_DIR"/zarf-package-cybersec-dask-amd64-*.tar.zst
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would run: zarf package create . --confirm"
        return 0
    fi

    cd "$ZARF_DIR"

    log_info "Building Zarf package..."
    if zarf package create . --confirm --insecure-skip-tls-verify 2>&1 \
        | tee /tmp/zarf-build.log | tail -20; then
        log_success "Package built"
        ls -lh "$ZARF_DIR"/*.tar.zst
    else
        log_error "Package build failed — see /tmp/zarf-build.log"
        tail -30 /tmp/zarf-build.log
        return 1
    fi
}

# =============================================================================
# Final Banner
# =============================================================================
print_final_banner() {
    echo ""
    echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}${BOLD}║  Deployment Complete                                         ║${NC}"
    echo -e "${GREEN}${BOLD}╠══════════════════════════════════════════════════════════════╣${NC}"
    echo -e "${GREEN}${BOLD}║${NC}"
    echo -e "${GREEN}${BOLD}║${NC}  Dask Dashboard:  http://${NODE_IP}:30087"
    echo -e "${GREEN}${BOLD}║${NC}  OTEL Navigator:  http://${NODE_IP}:30506"
    echo -e "${GREEN}${BOLD}║${NC}  JupyterHub:      http://${NODE_IP}:30080"
    if kubectl get ns kubernetes-dashboard &>/dev/null; then
        echo -e "${GREEN}${BOLD}║${NC}  K8s Dashboard:   https://${NODE_IP}:10443"
    fi
    echo -e "${GREEN}${BOLD}║${NC}"
    echo -e "${GREEN}${BOLD}║${NC}  Run verification:  $0 --verify-only"
    echo -e "${GREEN}${BOLD}║${NC}"
    echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
}

# =============================================================================
# Main
# =============================================================================
main() {
    # --- Environment setup ---
    if ! _resolve_kubeconfig; then
        exit 1
    fi
    detect_node_ip
    resolve_env_defaults
    detect_disk_constraints
    print_config_summary

    # --- Verify-only mode: jump to verification ---
    if [[ "$VERIFY_ONLY" == "true" ]]; then
        verify_deployment
        exit $?
    fi

    # --- Full deployment flow ---
    local failed=0

    check_baseline

    check_eviction_config

    if ! check_prerequisites; then
        exit 1
    fi

    if ! verify_injector_prerequisites; then
        exit 1
    fi

    # Build-mode steps (skipped with --skip-build)
    if [[ "$SKIP_BUILD" == "false" ]]; then
        if ! start_local_registry; then ((failed++)); fi
        if [[ $failed -eq 0 ]]; then
            if ! build_custom_image; then ((failed++)); fi
        fi
        if [[ $failed -eq 0 ]]; then
            if ! build_zarf_package; then ((failed++)); fi
        fi
    fi

    if [[ $failed -gt 0 ]]; then
        log_error "Build phase failed"
        exit 1
    fi

    # Storage & Init
    if ! setup_storage; then
        exit 1
    fi

    if ! initialize_zarf; then
        exit 1
    fi

    # Deploy
    if ! detect_package_file; then
        exit 1
    fi

    if ! deploy_package; then
        exit 1
    fi

    post_deploy_spill_patch

    # Dashboard (non-fatal)
    deploy_dashboard || true

    # Verification
    verify_deployment

    print_final_banner
}

main "$@"
