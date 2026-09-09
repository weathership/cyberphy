#!/usr/bin/env bash
# Laptop k3d dev stack — a manifest-direct mirror of the air-gap Zarf deploy,
# with Tilt live-reload on the two app Deployments (otel-navigator + engine).
#
# This is the "laptop local" (k3d) — distinct from the "workstation local"
# (RKE2 on a GPU box) and the air-gap deploy (Zarf). Same workloads, fast loop.
#
# Topology:
#   k3d cluster `cybersec-dev` (registry-less — images are delivered via
#   `k3d image import`, robust on the macOS podman VM). Dask operator (bundled
#   chart) + DaskCluster. panel-viz: navigator-engine + otel-navigator
#   (Tilt-owned). S3 = the laptop's devenv MinIO, reached from pods via
#   host.k3d.internal (binds 0.0.0.0:9010).
#
# Subcommands: preflight | cluster | image | deploy | seed | tilt | up | status | down
# Driven by `just dev-*`. All kubectl uses the throwaway kubeconfig under build/
# — it never touches ~/.kube or the devenv kubeconfig.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# ---- knobs (env-overridable; dev defaults, nothing hardcoded downstream) ----
CLUSTER="${DEV_CLUSTER:-cybersec-dev}"
KUBECONFIG_DEV="${DEV_KUBECONFIG:-$ROOT/build/k3d/kubeconfig}"
IMAGE_TAG="${DEV_IMAGE_TAG:-dev}"
DASK_IMAGE="${DEV_DASK_IMAGE:-cybersec-dask:${IMAGE_TAG}}"  # imported via k3d, not pulled
WORKERS="${DEV_DASK_WORKERS:-1}"   # laptop default; bump for a bigger box

S3_BUCKET="${DEV_S3_BUCKET:-cybersec-dask-data}"
SEED_MODE="${DEV_SEED_MODE:-minimal}"
S3_PORT="${LOCAL_S3_PORT:-9010}"
S3_ENDPOINT_HOST="${DEV_S3_ENDPOINT_HOST:-http://localhost:${S3_PORT}}"      # host (seeding)
# The in-cluster route to the laptop MinIO is auto-detected at deploy (see
# _detect_cluster_s3) — it differs by runtime (podman-mac: host.containers.internal;
# linux/docker k3d: host.k3d.internal). Override with DEV_S3_ENDPOINT_CLUSTER.
AWS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
AWS_SECRET="${MINIO_SECRET_KEY:-minioadmin}"
AWS_REGION="${AWS_REGION:-us-east-1}"

export KUBECONFIG="$KUBECONFIG_DEV"
KC() { kubectl --kubeconfig "$KUBECONFIG_DEV" "$@"; }
log()  { printf '\033[36m[dev-k3d]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[dev-k3d] WARN:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31m[dev-k3d] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

_builder() { command -v podman >/dev/null 2>&1 && echo podman || echo docker; }

# Ensure a container runtime is wired for k3d (standalone — `just dev-up` must not
# depend on being inside the devenv shell). On macOS+podman: start the machine if
# down and export DOCKER_HOST from the default connection (mirrors devenv.nix).
_ensure_runtime() {
  [ "$(uname -s)" = "Darwin" ] || return 0
  command -v podman >/dev/null 2>&1 || return 0
  if ! podman info >/dev/null 2>&1; then
    log "starting podman machine..."
    podman machine start >/dev/null 2>&1 || podman machine start podman-machine-default >/dev/null 2>&1 || \
      die "could not start the podman machine — start it manually and retry"
  fi
  if [ -z "${DOCKER_HOST:-}" ] && command -v jq >/dev/null 2>&1; then
    local uri
    uri=$(podman system connection list --format json 2>/dev/null | jq -r 'map(select(.Default==true)) | .[0].URI // empty')
    [ -z "$uri" ] && uri=$(podman system connection list --format json 2>/dev/null | jq -r 'map(select(.ReadWrite==true)) | .[0].URI // empty')
    [ -n "$uri" ] && { export DOCKER_HOST="$uri"; export K3D_HIDE_WARNING_ROOTLESS=1; log "DOCKER_HOST=$DOCKER_HOST"; }
  fi
}

# k3d adds `host.k3d.internal:host-gateway` to its nodes; podman can't resolve the
# `host-gateway` literal unless it knows the host-internal IP, so cluster create
# fails with "host containers internal IP address is empty". Set it once (the IP
# podman itself reports for host.containers.internal). Idempotent; no restart needed.
_ensure_podman_hostgw() {
  [ "$(uname -s)" = "Darwin" ] || return 0
  command -v podman >/dev/null 2>&1 || return 0
  local machine="${DEV_PODMAN_MACHINE:-podman-machine-default}"
  podman machine ssh "$machine" 'test -f /etc/containers/containers.conf.d/99-k3d-hostgw.conf' 2>/dev/null && return 0
  local gw
  gw="$(podman run --rm alpine getent hosts host.containers.internal 2>/dev/null | awk '{print $1}' | head -1)"
  [ -n "$gw" ] || { warn "could not derive podman host-gateway IP; k3d cluster create may fail"; return 0; }
  log "configuring podman host-gateway IP ($gw) for k3d (one-time)"
  podman machine ssh "$machine" \
    "sudo mkdir -p /etc/containers/containers.conf.d && printf '[containers]\nhost_containers_internal_ip=\"$gw\"\n' | sudo tee /etc/containers/containers.conf.d/99-k3d-hostgw.conf >/dev/null" \
    || warn "failed to write podman host-gateway drop-in"
}

# ---------------------------------------------------------------- preflight
cmd_preflight() {
  command -v k3d   >/dev/null || die "k3d not found (nix devenv provides it)"
  command -v helm  >/dev/null || die "helm not found"
  command -v tilt  >/dev/null || warn "tilt not found — 'up'/'tilt' will fail until installed"
  command -v uv    >/dev/null || die "uv not found"
  # devenv RustFS (local S3) must be live on the host (pods reach it via
  # host.k3d.internal). 0.0.0.0 bind is set in devenv.nix services.rustfs.
  if ! curl -sf --max-time 5 "${S3_ENDPOINT_HOST}/health" >/dev/null 2>&1 \
     && ! curl -sf --max-time 5 "${S3_ENDPOINT_HOST}/minio/health/live" >/dev/null 2>&1; then
    die "devenv RustFS (local S3) not reachable at ${S3_ENDPOINT_HOST} — run 'devenv up' first."
  fi
  log "preflight OK (k3d/helm/uv present; RustFS live at ${S3_ENDPOINT_HOST})"
}

# ---------------------------------------------------------------- cluster
cmd_cluster() {
  _ensure_runtime
  mkdir -p "$(dirname "$KUBECONFIG_DEV")"
  if k3d cluster list 2>/dev/null | grep -qE "^${CLUSTER}\b"; then
    log "cluster $CLUSTER exists"
  else
    _ensure_podman_hostgw
    # Registry-less by design: Tilt and `dev-image` deliver images via
    # `k3d image import`, which sidesteps the k3d-registry-on-podman 'bridge'
    # network gap and the macOS podman-VM push obstacle.
    log "creating cluster $CLUSTER (traefik/servicelb off; NodePort host-maps)"
    k3d cluster create "$CLUSTER" \
      --servers 1 --agents 0 \
      --api-port 6555 \
      --k3s-arg "--disable=traefik@server:0" \
      --k3s-arg "--disable=servicelb@server:0" \
      --port "30506:30506@server:0" \
      --port "30765:30765@server:0" \
      --port "30087:30087@server:0" \
      --wait
  fi
  k3d kubeconfig get "$CLUSTER" > "$KUBECONFIG_DEV"
  KC wait --for=condition=Ready nodes --all --timeout=120s
  log "cluster ready — KUBECONFIG=$KUBECONFIG_DEV"
}

# ---------------------------------------------------------------- image
# The Dask scheduler/workers are NOT Tilt-managed, so they need a real image in
# the cluster. We build once and `k3d image import` it (bulletproof on macOS —
# no registry push from the podman VM). Tilt builds the app image separately and
# pushes it to the k3d registry (default_registry in the Tiltfile).
cmd_image() {
  _ensure_runtime
  local b; b="$(_builder)"
  log "building $DASK_IMAGE via $b (one-time Dask baseline; ~1.3 GiB)"
  "$b" build -t "$DASK_IMAGE" -f zarf/images/Dockerfile.cybersec-dask .
  # podman stores bare tags under localhost/; containerd normalizes the pod ref
  # (cybersec-dask:dev) to docker.io/library/. Retag to the qualified name so the
  # imported image is what the IfNotPresent pod ref actually resolves to.
  local qualified="docker.io/library/${DASK_IMAGE}"
  "$b" tag "$DASK_IMAGE" "$qualified" 2>/dev/null || true
  log "importing $qualified into k3d cluster $CLUSTER"
  k3d image import "$qualified" -c "$CLUSTER"
  log "image imported"
}

# Detect the in-cluster route to the laptop's MinIO. host.k3d.internal points at
# the k3d-network gateway, which on podman-mac is NOT the macOS host — so we probe
# the candidates and pick the one that actually reaches MinIO. Echoes the base URL.
_detect_cluster_s3() {
  if [ -n "${DEV_S3_ENDPOINT_CLUSTER:-}" ]; then echo "$DEV_S3_ENDPOINT_CLUSTER"; return; fi
  log "detecting in-cluster route to MinIO (port ${S3_PORT})..." >&2
  KC run s3probe --image=alpine --restart=Never --command -- sleep 60 >/dev/null 2>&1 || true
  KC wait --for=condition=Ready pod/s3probe --timeout=90s >/dev/null 2>&1 || true
  local pick="" ep
  for ep in host.containers.internal host.k3d.internal host.docker.internal; do
    if KC exec s3probe -- sh -c "wget -qO- --timeout=4 http://$ep:${S3_PORT}/minio/health/live >/dev/null 2>&1"; then
      pick="http://$ep:${S3_PORT}"; break
    fi
  done
  KC delete pod s3probe --force --grace-period=0 >/dev/null 2>&1 || true
  [ -n "$pick" ] || die "no in-cluster route to MinIO at :${S3_PORT} (tried host.containers.internal/host.k3d.internal/host.docker.internal). Is devenv MinIO up + bound 0.0.0.0? Override DEV_S3_ENDPOINT_CLUSTER."
  log "MinIO reachable from pods at $pick" >&2
  echo "$pick"
}

# ---------------------------------------------------------------- deploy
cmd_deploy() {
  log "namespaces + services"
  KC apply -f tilt/k3d/namespaces.yaml
  KC apply -f tilt/k3d/services.yaml

  local S3_ENDPOINT_CLUSTER; S3_ENDPOINT_CLUSTER="$(_detect_cluster_s3)"

  log "otel-navigator-config ConfigMap (S3 endpoint -> ${S3_ENDPOINT_CLUSTER})"
  KC -n panel-viz create configmap otel-navigator-config \
    --from-literal=S3_BUCKET="$S3_BUCKET" \
    --from-literal=OTEL_DATA_PATH="s3://${S3_BUCKET}/" \
    --from-literal=AWS_REGION="$AWS_REGION" \
    --from-literal=AGENT_BACKEND="" \
    --from-literal=AGENT_ACP_COMMAND="" \
    --from-literal=AGENT_MODEL_BASE_URL="" \
    --from-literal=AGENT_MODEL="" \
    --dry-run=client -o yaml | KC apply -f -

  log "otel-navigator-credentials Secret (devenv MinIO creds)"
  KC -n panel-viz create secret generic otel-navigator-credentials \
    --from-literal=AWS_ACCESS_KEY_ID="$AWS_KEY" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET" \
    --from-literal=AWS_SESSION_TOKEN="" \
    --from-literal=S3_ENDPOINT="$S3_ENDPOINT_CLUSTER" \
    --dry-run=client -o yaml | KC apply -f -

  log "Dask operator (bundled chart)"
  helm --kubeconfig "$KUBECONFIG_DEV" upgrade --install dask-operator \
    zarf/charts/dask-kubernetes-operator-2024.1.0.tgz \
    --namespace dask-operator --create-namespace \
    --values zarf/manifests/dask-operator-values.yaml \
    --wait --timeout 5m
  KC wait --for=condition=Established crd/daskclusters.kubernetes.dask.org --timeout=60s

  log "DaskCluster (image ${DASK_IMAGE}, ${WORKERS} workers)"
  sed -e "s|__IMAGE__|${DASK_IMAGE}|g" \
      -e "s|__S3_ENDPOINT__|${S3_ENDPOINT_CLUSTER}|g" \
      -e "s|__AWS_REGION__|${AWS_REGION}|g" \
      -e "s|__AWS_KEY__|${AWS_KEY}|g" \
      -e "s|__AWS_SECRET__|${AWS_SECRET}|g" \
      -e "s|__WORKERS__|${WORKERS}|g" \
      tilt/k3d/dask-cluster.yaml | KC apply -f -
  log "deploy applied (app Deployments are owned by Tilt — run 'tilt')"
}

# ---------------------------------------------------------------- seed
# Seeds the laptop's devenv MinIO directly (host endpoint). Writes the parquet
# shards + _active_dataset.json the app discovers. Ensures the bucket first.
cmd_seed() {
  log "ensuring bucket s3://${S3_BUCKET} on ${S3_ENDPOINT_HOST}"
  AWS_ACCESS_KEY_ID="$AWS_KEY" AWS_SECRET_ACCESS_KEY="$AWS_SECRET" \
  uv run python - "$S3_BUCKET" "$S3_ENDPOINT_HOST" "$AWS_REGION" <<'PY'
import sys, boto3
from botocore.client import Config
bucket, endpoint, region = sys.argv[1], sys.argv[2], sys.argv[3]
s3 = boto3.client("s3", endpoint_url=endpoint, region_name=region,
                  config=Config(signature_version="s3v4"))
try:
    s3.head_bucket(Bucket=bucket); print(f"bucket {bucket} exists")
except Exception:
    s3.create_bucket(Bucket=bucket); print(f"created bucket {bucket}")
PY
  log "seeding OTEL parquet (mode=${SEED_MODE}) into s3://${S3_BUCKET}"
  AWS_ACCESS_KEY_ID="$AWS_KEY" AWS_SECRET_ACCESS_KEY="$AWS_SECRET" \
  S3_ENDPOINT="$S3_ENDPOINT_HOST" AWS_REGION="$AWS_REGION" \
  uv run python zarf/scripts/generate-otel-data.py \
    --mode "$SEED_MODE" --bucket "$S3_BUCKET" --endpoint "$S3_ENDPOINT_HOST"
  log "seed complete"
}

# ---------------------------------------------------------------- tilt
cmd_tilt() {
  command -v tilt >/dev/null || die "tilt not found"
  _ensure_runtime
  log "tilt up (registry-less; k3d image import into ${CLUSTER})"
  log "  Panel:    http://localhost:15006/otel-navigator   (or NodePort 30506)"
  log "  Terminal: ws on :18765 (Tilt) / :30765 (NodePort)"
  TILT_K3D_IMPORT="$CLUSTER" \
  KUBECONFIG="$KUBECONFIG_DEV" \
    tilt up --context "k3d-${CLUSTER}"
}

# ---------------------------------------------------------------- up (all)
cmd_up() {
  cmd_preflight
  cmd_cluster
  cmd_image
  cmd_deploy
  cmd_seed
  cmd_tilt
}

# ---------------------------------------------------------------- status
cmd_status() {
  command -v k3d >/dev/null && k3d cluster list 2>/dev/null | grep -E "NAME|${CLUSTER}" || true
  [ -f "$KUBECONFIG_DEV" ] || { warn "no dev kubeconfig yet — run 'just dev-cluster'"; return 0; }
  echo "--- dask ---";      KC -n dask get pods 2>/dev/null || true
  echo "--- panel-viz ---"; KC -n panel-viz get pods,svc 2>/dev/null || true
}

# ---------------------------------------------------------------- down
cmd_down() {
  _ensure_runtime
  log "deleting cluster $CLUSTER"
  k3d cluster delete "$CLUSTER" 2>/dev/null || true
  rm -f "$KUBECONFIG_DEV"
  log "torn down (devenv MinIO + its data are left intact)"
}

case "${1:-}" in
  preflight) cmd_preflight ;;
  cluster)   cmd_cluster ;;
  image)     cmd_image ;;
  deploy)    cmd_deploy ;;
  seed)      cmd_seed ;;
  tilt)      cmd_tilt ;;
  up)        cmd_up ;;
  status)    cmd_status ;;
  down)      cmd_down ;;
  *) echo "usage: dev-k3d.sh {preflight|cluster|image|deploy|seed|tilt|up|status|down}" >&2; exit 2 ;;
esac
