#!/usr/bin/env bash
# Shared lab environment helpers for cyberphy devenv / zarf:local / hub.
# Source from devenv tasks:  source scripts/lab_env.sh
#
# Exports (on success):
#   KUBECONFIG, CYBERPHY_K8S_TARGET (and CYBERSEC_K8S_TARGET alias)
#   CYBERPHY_NODE_IP, CYBERPHY_ZARF_PKG
#   RUSTFS / S3 convenience vars if unset

# shellcheck disable=SC2034

_lab_env_sourced=1

# Prefer rke2 kubectl when on PATH for RKE2 nodes
_lab_kubectl() {
  if [ -x /var/lib/rancher/rke2/bin/kubectl ]; then
    /var/lib/rancher/rke2/bin/kubectl "$@"
  else
    kubectl "$@"
  fi
}

# True if kubeconfig file is readable and can talk to an API server
_kubeconfig_works() {
  local kc="$1"
  [ -n "$kc" ] && [ -f "$kc" ] && [ -r "$kc" ] || return 1
  # Reject empty / truncated configs
  [ "$(wc -c <"$kc" 2>/dev/null || echo 0)" -gt 100 ] || return 1
  KUBECONFIG="$kc" _lab_kubectl cluster-info --request-timeout=3s >/dev/null 2>&1
}

# Resolve KUBECONFIG: explicit → working rke2 → k3d devenv state → default if works
resolve_kubeconfig() {
  local candidates=()

  if [ -n "${KUBECONFIG:-}" ]; then
    candidates+=("$KUBECONFIG")
  fi
  candidates+=(
    "$HOME/.kube/rke2.yaml"
    "/etc/rancher/rke2/rke2.yaml"
    "${DEVENV_STATE:-$PWD/.devenv/state}/kubeconfig"
    "$HOME/.kube/config"
  )

  local c
  for c in "${candidates[@]}"; do
    if _kubeconfig_works "$c"; then
      export KUBECONFIG="$c"
      return 0
    fi
  done

  # Last resort: first readable file (may still be broken — caller checks)
  for c in "${candidates[@]}"; do
    if [ -f "$c" ] && [ -r "$c" ]; then
      export KUBECONFIG="$c"
      return 0
    fi
  done
  return 1
}

# Detect target: rke2 | k3d | none  (also sets CYBERSEC_K8S_TARGET for back-compat)
detect_k8s_target() {
  if [ -n "${CYBERPHY_K8S_TARGET:-}" ]; then
    export CYBERSEC_K8S_TARGET="$CYBERPHY_K8S_TARGET"
    echo "$CYBERPHY_K8S_TARGET"
    return 0
  fi
  if [ -n "${CYBERSEC_K8S_TARGET:-}" ] && [ "$CYBERSEC_K8S_TARGET" != "none" ]; then
    export CYBERPHY_K8S_TARGET="$CYBERSEC_K8S_TARGET"
    echo "$CYBERSEC_K8S_TARGET"
    return 0
  fi

  local target="none"

  # Host RKE2 service is strongest signal
  if systemctl is-active --quiet rke2-server 2>/dev/null \
    || systemctl is-active --quiet rke2-server.service 2>/dev/null; then
    target="rke2"
  elif [ -n "${KUBECONFIG:-}" ] && [ -f "${KUBECONFIG:-}" ]; then
    if grep -qE 'rke2|rancher' "$KUBECONFIG" 2>/dev/null; then
      target="rke2"
    elif grep -qE 'k3d|k3s' "$KUBECONFIG" 2>/dev/null; then
      target="k3d"
    fi
  fi

  export CYBERPHY_K8S_TARGET="$target"
  export CYBERSEC_K8S_TARGET="$target"
  echo "$target"
}

# Node InternalIP for host-path S3 from pods
detect_node_ip() {
  local ip=""
  if [ -n "${KUBECONFIG:-}" ]; then
    ip=$(KUBECONFIG="$KUBECONFIG" _lab_kubectl get nodes \
      -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null \
      | awk '{print $1}')
  fi
  if [ -z "$ip" ]; then
    # Fallback: first non-loopback IPv4
    ip=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1); exit}}')
  fi
  export CYBERPHY_NODE_IP="${ip:-127.0.0.1}"
  echo "$CYBERPHY_NODE_IP"
}

# RustFS / local S3 health (prefers /health)
rustfs_up() {
  local base="${1:-http://127.0.0.1:${LOCAL_S3_PORT:-9010}}"
  curl -sf --max-time 3 "$base/health" >/dev/null 2>&1 \
    || curl -sf --max-time 3 "$base/minio/health/live" >/dev/null 2>&1
}

# Prefer highest version: mirror/1.6.5 → other mirrors → zarf/
find_zarf_package() {
  local root="${DEVENV_ROOT:-$PWD}"
  local pkg=""
  # Explicit override
  if [ -n "${CYBERPHY_ZARF_PKG:-}" ] && [ -f "$CYBERPHY_ZARF_PKG" ]; then
    echo "$CYBERPHY_ZARF_PKG"
    return 0
  fi
  # Sort by version field in path (…-amd64-X.Y.Z.tar.zst), highest last
  pkg=$(
    {
      ls "$root"/build/cyberphy-release-mirror/*/zarf-package-cybersec-dask-amd64-*.tar.zst 2>/dev/null
      ls "$root"/zarf/zarf-package-cybersec-dask-amd64-*.tar.zst 2>/dev/null
    } | sort -t- -k6 -V | tail -1
  )
  if [ -n "$pkg" ] && [ -f "$pkg" ]; then
    export CYBERPHY_ZARF_PKG="$pkg"
    echo "$pkg"
    return 0
  fi
  return 1
}

# Default S3 env for local lab if unset
ensure_local_s3_env() {
  export S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:${LOCAL_S3_PORT:-9010}}"
  export S3_BUCKET="${S3_BUCKET:-cyberphy}"
  export S3_REGION="${S3_REGION:-us-east-1}"
  export RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-${MINIO_ACCESS_KEY:-admin}}"
  export RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-${MINIO_SECRET_KEY:-admin}}"
  export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$RUSTFS_ACCESS_KEY}"
  export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$RUSTFS_SECRET_KEY}"
  export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-$RUSTFS_ACCESS_KEY}"
  export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-$RUSTFS_SECRET_KEY}"
}

# NodePort probes (return 0 if HTTP looks alive)
nodeport_up() {
  local port="$1"
  local path="${2:-/}"
  local host="${3:-127.0.0.1}"
  local code
  code=$(curl -sS -m 2 -o /dev/null -w "%{http_code}" "http://${host}:${port}${path}" 2>/dev/null || echo 000)
  case "$code" in
    200|201|301|302|303|307|401|403) return 0 ;;
    *) return 1 ;;
  esac
}

# Print compact lab + k8s status (for lab:status task)
lab_status_report() {
  ensure_local_s3_env
  echo "=== Cyberphy lab status ==="
  echo ""

  # Plane A
  echo "Plane A — devenv lab"
  rustfs_up && echo "  RustFS     :9010 OK" || echo "  RustFS     :9010 DOWN"
  curl -sf --max-time 2 http://127.0.0.1:8182/q/health/ready >/dev/null 2>&1 \
    && echo "  Polaris    :8182 OK" || echo "  Polaris    :8182 DOWN"
  curl -sf --max-time 2 http://127.0.0.1:5050/ >/dev/null 2>&1 \
    && echo "  Hub UI     :5050 OK" || echo "  Hub UI     :5050 DOWN"
  curl -sf --max-time 2 http://127.0.0.1:8450/nifi/ >/dev/null 2>&1 \
    && echo "  NiFi       :8450 OK" || echo "  NiFi       :8450 DOWN"
  curl -sf --max-time 2 http://127.0.0.1:8081/ >/dev/null 2>&1 \
    && echo "  Flink      :8081 OK" || echo "  Flink      :8081 DOWN"
  echo "  Catalog    : ${POLARIS_CATALOG_NAME:-cyberphy}"
  echo "  Warehouse  : ${POLARIS_WAREHOUSE:-s3://cyberphy/iceberg/warehouse}"
  echo "  Data dir   : ${RUSTFS_DATA_DIR:-/raid/build/cyberphy/data/}"
  echo ""

  # Plane B
  echo "Plane B — local K8s / Zarf"
  if systemctl is-active --quiet rke2-server 2>/dev/null; then
    echo "  rke2-server: active"
  else
    echo "  rke2-server: inactive"
  fi

  if resolve_kubeconfig; then
    echo "  KUBECONFIG : $KUBECONFIG"
    local tgt
    tgt=$(detect_k8s_target)
    echo "  target     : $tgt"
    if KUBECONFIG="$KUBECONFIG" _lab_kubectl get nodes --no-headers 2>/dev/null | head -5 | sed 's/^/    /'; then
      :
    else
      echo "    (cluster not reachable)"
    fi
    detect_node_ip >/dev/null
    echo "  node IP    : ${CYBERPHY_NODE_IP:-unknown}"
  else
    echo "  KUBECONFIG : (none usable)"
    detect_k8s_target >/dev/null
    echo "  target     : ${CYBERPHY_K8S_TARGET:-none}"
  fi

  echo ""
  echo "  NodePorts (hub links):"
  nodeport_up 30080 /hub/login && echo "    Jupyter  :30080 OK" || echo "    Jupyter  :30080 DOWN"
  nodeport_up 30087 /health && echo "    Dask     :30087 OK" || echo "    Dask     :30087 DOWN"
  nodeport_up 30506 / && echo "    Panel    :30506 OK" || echo "    Panel    :30506 DOWN"

  if [ -n "${KUBECONFIG:-}" ] && KUBECONFIG="$KUBECONFIG" _lab_kubectl get ns zarf >/dev/null 2>&1; then
    echo ""
    echo "  Zarf namespaces:"
    for ns in zarf dask dask-operator jupyterhub panel-viz; do
      if KUBECONFIG="$KUBECONFIG" _lab_kubectl get ns "$ns" >/dev/null 2>&1; then
        local ready
        ready=$(KUBECONFIG="$KUBECONFIG" _lab_kubectl get pods -n "$ns" --no-headers 2>/dev/null \
          | awk '$3=="Running"||$2~/\// {c++} END{print c+0}')
        echo "    $ns: pods~$ready"
      fi
    done
  fi

  echo ""
  if pkg=$(find_zarf_package 2>/dev/null); then
    echo "  Package    : $pkg"
  else
    echo "  Package    : (none found — build/cyberphy-release-mirror or zarf/)"
  fi
  echo ""
}
