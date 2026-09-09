#!/bin/bash
# Install a secondary RKE2 instance for isolated Cyberphy Dask deployment
#
# This script creates a completely separate RKE2 instance that can run
# alongside an existing RKE2 installation without conflicts.
#
# Usage:
#   sudo ./install-rke2-secondary.sh              # Install with defaults
#   sudo ./install-rke2-secondary.sh --dry-run    # Show what would be done
#   sudo ./install-rke2-secondary.sh --uninstall  # Remove the instance
#
# Configuration can be customized via environment variables:
#   INSTANCE_NAME   - Instance identifier (default: cybersec)
#   API_PORT        - API server port (default: 6444)
#   TRAEFIK_HTTP    - Traefik HTTP port (default: 8080)
#   TRAEFIK_HTTPS   - Traefik HTTPS port (default: 8443)
#   NODEPORT_START  - Start of NodePort range (default: 31000)
#   NODEPORT_END    - End of NodePort range (default: 31999)
#   POD_CIDR        - Pod network CIDR (default: 10.52.0.0/16)
#   SVC_CIDR        - Service network CIDR (default: 10.53.0.0/16)

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Configuration with defaults
INSTANCE_NAME="${INSTANCE_NAME:-cybersec}"
API_PORT="${API_PORT:-6444}"
SUPERVISOR_PORT="${SUPERVISOR_PORT:-9346}"
TRAEFIK_HTTP="${TRAEFIK_HTTP:-8080}"
TRAEFIK_HTTPS="${TRAEFIK_HTTPS:-8443}"
NODEPORT_START="${NODEPORT_START:-31000}"
NODEPORT_END="${NODEPORT_END:-31999}"
POD_CIDR="${POD_CIDR:-10.52.0.0/16}"
SVC_CIDR="${SVC_CIDR:-10.53.0.0/16}"

# Derived paths
CONFIG_DIR="/etc/rancher/rke2-${INSTANCE_NAME}"
DATA_DIR="/var/lib/rancher/rke2-${INSTANCE_NAME}"
SERVICE_NAME="rke2-${INSTANCE_NAME}-server"
KUBECONFIG_PATH="${CONFIG_DIR}/rke2.yaml"

DRY_RUN=false
UNINSTALL=false

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Install a secondary RKE2 instance for isolated deployments.

Options:
    --dry-run       Show what would be done without making changes
    --uninstall     Remove the secondary RKE2 instance
    -h, --help      Show this help message

Environment Variables:
    INSTANCE_NAME   Instance identifier (default: cybersec)
    API_PORT        API server port (default: 6444)
    TRAEFIK_HTTP    Traefik HTTP port (default: 8080)
    TRAEFIK_HTTPS   Traefik HTTPS port (default: 8443)
    NODEPORT_START  NodePort range start (default: 31000)
    NODEPORT_END    NodePort range end (default: 31999)
    POD_CIDR        Pod network CIDR (default: 10.52.0.0/16)
    SVC_CIDR        Service network CIDR (default: 10.53.0.0/16)

Examples:
    # Install with defaults
    sudo $0

    # Custom instance name and ports
    INSTANCE_NAME=analytics API_PORT=6445 sudo $0

    # Dry run to see configuration
    sudo $0 --dry-run

    # Uninstall
    sudo $0 --uninstall
EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --uninstall)
                UNINSTALL=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    # Must be root
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root"
        exit 1
    fi

    # RKE2 must be installed
    if ! command -v rke2 &>/dev/null; then
        log_error "RKE2 is not installed. Install RKE2 first."
        exit 1
    fi

    # Check for port conflicts
    for port in $API_PORT $SUPERVISOR_PORT $TRAEFIK_HTTP $TRAEFIK_HTTPS; do
        if ss -tlnp | grep -q ":${port} "; then
            log_error "Port ${port} is already in use"
            ss -tlnp | grep ":${port} "
            exit 1
        fi
    done

    log_success "Prerequisites check passed"
}

show_config() {
    echo ""
    echo "=============================================="
    echo "RKE2 Secondary Instance Configuration"
    echo "=============================================="
    echo ""
    echo "Instance Name:    ${INSTANCE_NAME}"
    echo "Config Directory: ${CONFIG_DIR}"
    echo "Data Directory:   ${DATA_DIR}"
    echo "Service Name:     ${SERVICE_NAME}"
    echo ""
    echo "Network Configuration:"
    echo "  API Port:       ${API_PORT}"
    echo "  Supervisor:     ${SUPERVISOR_PORT}"
    echo "  Pod CIDR:       ${POD_CIDR}"
    echo "  Service CIDR:   ${SVC_CIDR}"
    echo "  NodePort Range: ${NODEPORT_START}-${NODEPORT_END}"
    echo ""
    echo "Traefik Ingress:"
    echo "  HTTP Port:      ${TRAEFIK_HTTP}"
    echo "  HTTPS Port:     ${TRAEFIK_HTTPS}"
    echo ""
    echo "Kubeconfig:       ${KUBECONFIG_PATH}"
    echo ""
}

create_config() {
    log_info "Creating configuration directory..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would create: ${CONFIG_DIR}"
        return
    fi

    mkdir -p "${CONFIG_DIR}"
    mkdir -p "${DATA_DIR}"

    # Main RKE2 config
    cat > "${CONFIG_DIR}/config.yaml" << EOF
# RKE2 ${INSTANCE_NAME} Instance Configuration
# Generated by install-rke2-secondary.sh

# TLS SANs for API server access
tls-san:
  - localhost
  - 127.0.0.1
  - $(hostname)

# Network configuration - isolated from primary instance
cluster-cidr: ${POD_CIDR}
service-cidr: ${SVC_CIDR}

# CNI
cni: canal

# Data directory
data-dir: ${DATA_DIR}

# Kubeconfig location
write-kubeconfig: ${KUBECONFIG_PATH}
write-kubeconfig-mode: "0644"

# Node identification
node-label:
  - "rke2-instance=${INSTANCE_NAME}"

# NodePort range (must not overlap with primary)
kubelet-arg:
  - "node-port-range=${NODEPORT_START}-${NODEPORT_END}"

# API Server ports - isolated from primary (6443/9345)
https-listen-port: ${API_PORT}
supervisor-port: ${SUPERVISOR_PORT}
EOF

    log_success "Created ${CONFIG_DIR}/config.yaml"
}

create_traefik_config() {
    log_info "Creating Traefik configuration..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would create Traefik HelmChartConfig"
        return
    fi

    mkdir -p "${DATA_DIR}/server/manifests"

    cat > "${DATA_DIR}/server/manifests/traefik-config.yaml" << EOF
apiVersion: helm.cattle.io/v1
kind: HelmChartConfig
metadata:
  name: traefik
  namespace: kube-system
spec:
  valuesContent: |-
    ports:
      web:
        port: ${TRAEFIK_HTTP}
        exposedPort: ${TRAEFIK_HTTP}
        nodePort: $((NODEPORT_START + 80))
      websecure:
        port: ${TRAEFIK_HTTPS}
        exposedPort: ${TRAEFIK_HTTPS}
        nodePort: $((NODEPORT_START + 443))
    service:
      type: NodePort
EOF

    log_success "Created Traefik configuration"
}

create_systemd_service() {
    log_info "Creating systemd service..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would create /etc/systemd/system/${SERVICE_NAME}.service"
        return
    fi

    cat > "/etc/systemd/system/${SERVICE_NAME}.service" << EOF
[Unit]
Description=RKE2 ${INSTANCE_NAME} Instance - Kubernetes Server
Documentation=https://github.com/rancher/rke2
Wants=network-online.target
After=network-online.target

[Service]
Type=notify
Environment="RKE2_CONFIG_FILE=${CONFIG_DIR}/config.yaml"
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
ExecStart=/usr/local/bin/rke2 server --config ${CONFIG_DIR}/config.yaml

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    log_success "Created systemd service: ${SERVICE_NAME}"
}

start_service() {
    log_info "Starting RKE2 ${INSTANCE_NAME} instance..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would enable and start ${SERVICE_NAME}"
        return
    fi

    systemctl enable "${SERVICE_NAME}"
    systemctl start "${SERVICE_NAME}"

    log_info "Waiting for RKE2 to be ready (this may take 2-3 minutes)..."

    local retries=60
    while [[ $retries -gt 0 ]]; do
        if [[ -f "${KUBECONFIG_PATH}" ]] && \
           kubectl --kubeconfig "${KUBECONFIG_PATH}" get nodes &>/dev/null; then
            log_success "RKE2 ${INSTANCE_NAME} instance is ready!"
            return 0
        fi
        sleep 5
        retries=$((retries - 1))
        echo -n "."
    done

    log_error "Timeout waiting for RKE2 to be ready"
    log_info "Check logs: journalctl -u ${SERVICE_NAME} -f"
    exit 1
}

setup_kubeconfig() {
    log_info "Setting up kubeconfig access..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would copy kubeconfig to user home directory"
        return
    fi

    local user_home
    user_home=$(getent passwd "${SUDO_USER:-root}" | cut -d: -f6)
    local user_kubeconfig="${user_home}/.kube/rke2-${INSTANCE_NAME}.yaml"

    mkdir -p "${user_home}/.kube"
    cp "${KUBECONFIG_PATH}" "${user_kubeconfig}"
    chown "${SUDO_USER:-root}:${SUDO_USER:-root}" "${user_kubeconfig}"

    log_success "Kubeconfig available at: ${user_kubeconfig}"
    echo ""
    echo "To use this cluster:"
    echo "  export KUBECONFIG=${user_kubeconfig}"
    echo "  kubectl get nodes"
    echo ""
    echo "Or add an alias to ~/.bashrc:"
    echo "  alias kubectl-${INSTANCE_NAME}='kubectl --kubeconfig ${user_kubeconfig}'"
}

uninstall() {
    log_warn "Uninstalling RKE2 ${INSTANCE_NAME} instance..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would stop and remove ${SERVICE_NAME}"
        log_info "[DRY-RUN] Would remove ${CONFIG_DIR}"
        log_info "[DRY-RUN] Would remove ${DATA_DIR}"
        return
    fi

    # Stop and disable service
    if systemctl is-active "${SERVICE_NAME}" &>/dev/null; then
        systemctl stop "${SERVICE_NAME}"
    fi
    if systemctl is-enabled "${SERVICE_NAME}" &>/dev/null; then
        systemctl disable "${SERVICE_NAME}"
    fi

    # Remove systemd service
    rm -f "/etc/systemd/system/${SERVICE_NAME}.service"
    systemctl daemon-reload

    # Remove config and data
    rm -rf "${CONFIG_DIR}"
    rm -rf "${DATA_DIR}"

    # Remove user kubeconfig
    local user_home
    user_home=$(getent passwd "${SUDO_USER:-root}" | cut -d: -f6)
    rm -f "${user_home}/.kube/rke2-${INSTANCE_NAME}.yaml"

    log_success "RKE2 ${INSTANCE_NAME} instance removed"
}

main() {
    parse_args "$@"

    echo ""
    echo "=============================================="
    echo "RKE2 Secondary Instance Installer"
    echo "=============================================="

    if [[ "$UNINSTALL" == "true" ]]; then
        show_config
        uninstall
        exit 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_warn "DRY-RUN MODE - No changes will be made"
    fi

    show_config
    check_prerequisites
    create_config
    create_traefik_config
    create_systemd_service

    if [[ "$DRY_RUN" != "true" ]]; then
        start_service
        setup_kubeconfig
    fi

    echo ""
    log_success "Installation complete!"
    echo ""
    echo "Next steps:"
    echo "  1. Set KUBECONFIG and verify:"
    echo "     export KUBECONFIG=~/.kube/rke2-${INSTANCE_NAME}.yaml"
    echo "     kubectl get nodes"
    echo ""
    echo "  2. Initialize Zarf:"
    echo "     zarf init --confirm"
    echo ""
    echo "  3. Deploy Cyberphy Dask:"
    echo "     zarf package deploy zarf-package-cybersec-dask-*.tar.zst --confirm"
    echo ""
    echo "  4. Patch NodePorts (if needed):"
    echo "     kubectl patch svc cybersec-dask-scheduler -n dask --type='json' \\"
    echo "       -p='[{\"op\":\"replace\",\"path\":\"/spec/ports/0/nodePort\",\"value\":${NODEPORT_START}86}]'"
    echo ""
}

main "$@"
