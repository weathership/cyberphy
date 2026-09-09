#!/bin/bash
# =============================================================================
# Full E2E Deployment Script
# =============================================================================
#
# Deploys the Cyberphy Dask stack (Dask/JupyterHub/Panel) on AWS RKE2 with
# Cloudflare Zero Trust for secure access via WARP device posture.
#
# Prerequisites:
#   - AWS credentials configured (aws configure or AWS_PROFILE)
#   - Cloudflare API token with required permissions
#   - SSH key at ~/.ssh/cybersec-dask.pem
#   - devenv installed and active
#
# Usage:
#   ./scripts/deploy-e2e.sh              # Full deployment
#   ./scripts/deploy-e2e.sh --skip-infra # Skip infrastructure, deploy apps only
#   ./scripts/deploy-e2e.sh --verify     # Verify existing deployment
#   ./scripts/deploy-e2e.sh --datagen    # Generate validation dataset only
#
# =============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

print_banner() {
    echo ""
    echo "=============================================================="
    echo "  Cyberphy Dask Stack - E2E Deployment"
    echo "=============================================================="
    echo ""
}

# Parse arguments
SKIP_INFRA=false
VERIFY_ONLY=false
DATAGEN_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-infra) SKIP_INFRA=true; shift ;;
        --verify) VERIFY_ONLY=true; shift ;;
        --datagen) DATAGEN_ONLY=true; shift ;;
        *) log_error "Unknown option: $1"; exit 1 ;;
    esac
done

print_banner

# =============================================================================
# Pre-flight Checks
# =============================================================================

log_info "Running pre-flight checks..."

# Check devenv
if ! command -v devenv &> /dev/null; then
    log_error "devenv not found. Please install devenv and activate the shell."
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    log_error "AWS credentials not configured. Run 'aws configure' or set AWS_PROFILE."
    exit 1
fi

AWS_IDENTITY=$(aws sts get-caller-identity --query 'Arn' --output text)
log_success "AWS identity: $AWS_IDENTITY"

# Check Cloudflare (only if not skipping infra and not verify-only)
if [ "$SKIP_INFRA" = false ] && [ "$VERIFY_ONLY" = false ] && [ "$DATAGEN_ONLY" = false ]; then
    if [ -z "${CLOUDFLARE_API_TOKEN:-}" ]; then
        log_warn "CLOUDFLARE_API_TOKEN not set. Cloudflare tunnel will not be configured."
        log_info "For full deployment, set:"
        log_info "  export CLOUDFLARE_API_TOKEN='...'"
        log_info "  export CLOUDFLARE_ACCOUNT_ID='...'"
        log_info "  export CLOUDFLARE_ZONE_ID='...'"
    else
        log_success "Cloudflare API token configured"
    fi
fi

# Check SSH key
SSH_KEY="$HOME/.ssh/cybersec-dask.pem"
if [ ! -f "$SSH_KEY" ]; then
    log_warn "SSH key not found at $SSH_KEY"
    log_info "The aws:provision task will create it automatically."
fi

echo ""

# =============================================================================
# Datagen Only Mode
# =============================================================================

if [ "$DATAGEN_ONLY" = true ]; then
    log_info "=== Generating Validation Dataset ==="
    devenv tasks run zarf:datagen minimal
    log_success "Dataset generation complete"
    exit 0
fi

# =============================================================================
# Verify Only Mode
# =============================================================================

if [ "$VERIFY_ONLY" = true ]; then
    log_info "=== Verifying Existing Deployment ==="
    devenv tasks run aws:verify
    exit 0
fi

# =============================================================================
# Phase 1: Infrastructure Provisioning
# =============================================================================

if [ "$SKIP_INFRA" = false ]; then
    log_info "=== Phase 1: Provisioning AWS Infrastructure ==="
    echo ""

    # Check for existing infrastructure
    cd infra/aws/tofu
    if tofu state list &> /dev/null 2>&1; then
        EXISTING_RESOURCES=$(tofu state list 2>/dev/null | wc -l)
        if [ "$EXISTING_RESOURCES" -gt 0 ]; then
            log_warn "Existing infrastructure detected ($EXISTING_RESOURCES resources)"
            log_info "Running 'tofu apply' to ensure desired state..."
        fi
    fi
    cd - > /dev/null

    devenv tasks run aws:provision
    log_success "Infrastructure provisioned"
    echo ""
fi

# =============================================================================
# Phase 2: Generate Ansible Inventory
# =============================================================================

log_info "=== Phase 2: Generating Ansible Inventory ==="
devenv tasks run aws:inventory
log_success "Inventory generated at infra/aws/ansible/inventory/hosts"
echo ""

# =============================================================================
# Phase 3: Deploy RKE2 + Stack
# =============================================================================

log_info "=== Phase 3: Deploying RKE2 + Dask Stack ==="
echo ""
log_info "This will deploy:"
log_info "  - RKE2 Kubernetes cluster"
log_info "  - Dask Operator + Cluster"
log_info "  - JupyterHub with sample notebooks"
log_info "  - Panel visualization dashboard"
log_info "  - Cloudflare Tunnel for external access"
echo ""

devenv tasks run aws:deploy
log_success "Stack deployed"
echo ""

# =============================================================================
# Phase 4: Verify Deployment
# =============================================================================

log_info "=== Phase 4: Verifying Deployment ==="
devenv tasks run aws:verify
echo ""

# =============================================================================
# Phase 5: Generate Validation Dataset
# =============================================================================

log_info "=== Phase 5: Generating Validation Dataset ==="
log_info "Generating minimal dataset (50K spans) for immediate testing..."

# The datagen role in Ansible already runs during aws:deploy
# This is just a verification
if command -v kubectl &> /dev/null && [ -n "${KUBECONFIG:-}" ]; then
    if kubectl get job datagen-minimal -n datagen &> /dev/null 2>&1; then
        STATUS=$(kubectl get job datagen-minimal -n datagen -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "0")
        if [ "$STATUS" = "1" ]; then
            log_success "Minimal dataset already generated"
        else
            log_info "Datagen job is running. Monitor with:"
            log_info "  kubectl logs -f job/datagen-minimal -n datagen"
        fi
    fi
else
    log_info "To generate dataset manually:"
    log_info "  devenv tasks run zarf:datagen minimal"
fi

echo ""

# =============================================================================
# Summary
# =============================================================================

log_success "=============================================================="
log_success "  Deployment Complete!"
log_success "=============================================================="
echo ""
log_info "Access URLs (requires WARP enrollment):"
echo ""
echo "  JupyterHub:     https://jupyter.dev.aws.zndx.org"
echo "  Dask Dashboard: https://dask.dev.aws.zndx.org"
echo "  Panel Viz:      https://viz.dev.aws.zndx.org"
echo "  K8s Dashboard:  https://k8s.dev.aws.zndx.org"
echo ""
log_info "SSH access:"
echo ""
echo "  devenv tasks run aws:ssh"
echo ""
log_info "Verify WARP connectivity:"
echo ""
echo "  warp-cli status"
echo "  curl -sI https://jupyter.dev.aws.zndx.org"
echo ""
log_info "For troubleshooting, see: infra/aws/README.md"
echo ""
