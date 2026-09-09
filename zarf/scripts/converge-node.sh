#!/usr/bin/env bash
# Node-local convergence entrypoint — the PRIMARY way to deploy a cybersec-dask
# release into an existing, air-gapped RKE2. No AWS, no tofu, no bastion: run it ON
# the cluster's control-plane node after transporting the release package + this
# repo's zarf/ tree (or just the staged engine: converge/ + manifests/ +
# artifacts.manifest.json beside this script). It resolves kubectl / zarf / the
# package locally and drives `python3 -m converge` to the deployment's target state.
#
# Usage (on the air-gap node, as root — RKE2's kubeconfig is root-only):
#   sudo zarf/scripts/converge-node.sh [verify|apply|dry-run|teardown] [package.tar.zst]
#
#   verify    (default) read-only target oracle — reports drift, changes nothing
#   apply               remediate to a fixpoint (Layer-B only; NEVER deletes a
#                       transported image — that guard is structural in the engine)
#   dry-run             show what apply WOULD do
#   teardown            clean-slate the Layer-B app stack (registry +PV + node
#                       images CONSERVED, so a following apply redeploys fast)
#
# DEFAULT modality = RESILIENT air-gap: the registry binds a claimRef hostPath PV, so
# NO default StorageClass / local-path-provisioner / bootstrap images are needed. This
# is the only modality public releases target. To opt a resourced multi-node cluster
# back into dynamic provisioning, export CONVERGE_DYNAMIC_PROVISIONING=1.
#
# S3 credentials (only for a from-scratch deploy that (re)creates the in-cluster S3
# secret) — either:
#   - export S3_ENDPOINT S3_BUCKET S3_REGION S3_ACCESS_KEY S3_SECRET_KEY
#     S3_SESSION_TOKEN  (this script stages them to a tmpfs creds file + shreds it), or
#   - point CONVERGE_CREDS_FILE at a KEY=VALUE file you manage (not shredded here).
# Secrets are staged to a tmpfs creds file; the engine delivers them via a 0600
# ZARF_CONFIG ([package.deploy.set]) — bare ZARF_VAR_* env does NOT template in
# zarf v0.70.1. Never put secrets on argv / the process table.
#
# Env tunables: S3_BUCKET (required for a first deploy), worker sizing
#   DASK_WORKER_REPLICAS (default 4 — multi-core / air-gap baseline; set 1 for
#     tiny smoke hosts; engine capacity-caps by RAM headroom + Pending)
#   DASK_WORKER_NTHREADS / DASK_WORKER_CPU / DASK_WORKER_MEMORY (optional;
#     package defaults 2 / 2 / 6Gi — engine 0.5.0 applies surgically to live CR)
#   Aliases: DASK_WORKER_MEM_LIMIT → MEMORY, DASK_WORKER_MEM_REQUEST → requests
# KUBECONFIG, CONVERGE_DYNAMIC_PROVISIONING=1, CONVERGE_NO_REGISTRY_PVC=1.
set -euo pipefail

MODE="${1:-verify}"
case "$MODE" in
  verify)   MODE_FLAG="--verify" ;;
  apply)    MODE_FLAG="--apply" ;;
  dry-run)  MODE_FLAG="--dry-run" ;;
  teardown) MODE_FLAG="--teardown" ;;
  *) echo "usage: $0 [verify|apply|dry-run|teardown] [package.tar.zst]" >&2; exit 2 ;;
esac
PKG_ARG="${2:-}"

# --- locate the engine (works from the repo AND from a flat staged dir) -------
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ZARF_DIR=""
for cand in "$SELF" "$SELF/.." "$SELF/../zarf" "$SELF/zarf"; do
  if [ -f "$cand/converge/__init__.py" ]; then ZARF_DIR="$(cd "$cand" && pwd)"; break; fi
done
if [ -z "$ZARF_DIR" ]; then
  echo "❌ cannot find the converge engine (converge/__init__.py) near $SELF" >&2
  exit 2
fi
MANIFESTS_DIR="$ZARF_DIR/manifests"
MANIFEST_JSON="$ZARF_DIR/artifacts.manifest.json"

# --- resolve kubeconfig / kubectl / zarf -------------------------------------
KUBECONFIG="${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}"
if [ ! -r "$KUBECONFIG" ] && [ "$(id -u)" != 0 ]; then
  echo "❌ $KUBECONFIG not readable — run as root (sudo) on the control-plane node." >&2
  exit 2
fi
# Prefer explicit ZARF_BIN (must match the package build version — format skew is
# unrecoverable air-gapped). Then PATH `zarf`, then well-known install paths.
# Do NOT prefer a stale /usr/local/bin/zarf over a correct PATH entry.
if [ -z "${ZARF_BIN:-}" ] || [ ! -x "$ZARF_BIN" ]; then
  ZARF_BIN=""
  if command -v zarf >/dev/null 2>&1; then
    ZARF_BIN="$(command -v zarf)"
  else
    for c in /usr/local/bin/zarf /var/lib/rancher/rke2/bin/zarf; do
      if [ -x "$c" ]; then ZARF_BIN="$c"; break; fi
    done
  fi
fi
# Resolve to absolute path so sudo/chdir cannot lose it.
[ -n "$ZARF_BIN" ] && ZARF_BIN="$(readlink -f "$ZARF_BIN" 2>/dev/null || echo "$ZARF_BIN")"
echo "   zarf: ${ZARF_BIN:-<none>} ($("$ZARF_BIN" version 2>/dev/null | head -1 || echo '?'))"
KUBECTL_CMD=""
for c in /var/lib/rancher/rke2/bin/kubectl /usr/local/bin/kubectl kubectl; do
  if [ -x "$c" ] || command -v "$c" >/dev/null 2>&1; then
    KUBECTL_CMD="$c --kubeconfig $KUBECONFIG"; break
  fi
done
if [ -z "$KUBECTL_CMD" ]; then
  if [ -n "$ZARF_BIN" ]; then
    KUBECTL_CMD="$ZARF_BIN tools kubectl --kubeconfig $KUBECONFIG"
  else
    echo "❌ no kubectl or zarf binary found on PATH." >&2; exit 2
  fi
fi

# --- locate the transported deploy package (optional for verify / kubectl-only) -
# Paths are NEVER site- or mount-specific (no /mnt/… layouts, remote homes, etc.).
# Portable sources only — operator chooses where to run and what path to pass:
#   1) explicit argv2 (absolute or relative path this uid can read)
#   2) same basename next to this script / in CWD / in /var/tmp
#   3) any package next to this script / in CWD / in /var/tmp
# Do not invent packages from other trees.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_pkg_abs() {
  local p="$1" d
  [ -n "$p" ] && [ -f "$p" ] && [ -r "$p" ] || return 1
  d="$(cd "$(dirname "$p")" && pwd)" || return 1
  printf '%s\n' "$d/$(basename "$p")"
}
_pkg_in_dir() {
  local dir="$1" c abs
  shopt -s nullglob
  for c in "$dir"/zarf-package-cybersec-dask-amd64-*.tar.zst; do
    abs="$(_pkg_abs "$c")" || continue
    printf '%s\n' "$abs"
    shopt -u nullglob
    return 0
  done
  shopt -u nullglob
  return 1
}

if [ -n "$PKG_ARG" ]; then
  if RESOLVED="$(_pkg_abs "$PKG_ARG")"; then
    PKG_ARG="$RESOLVED"
  else
    if [ ! -e "$PKG_ARG" ]; then
      echo "   ⚠ package path does not exist as $(id -un): ${PKG_ARG}"
    else
      echo "   ⚠ package path not readable as $(id -un): ${PKG_ARG}"
    fi
    ls -la "$PKG_ARG" 2>&1 | sed 's/^/     /' || true
    base="$(basename "$PKG_ARG")"
    if RESOLVED="$(_pkg_abs "$SCRIPT_DIR/$base")" || \
       RESOLVED="$(_pkg_abs "./$base")" || \
       RESOLVED="$(_pkg_abs "/var/tmp/$base")"; then
      echo "   → using co-located package (same basename): $RESOLVED"
      PKG_ARG="$RESOLVED"
    else
      echo "   → pass a path this process can read, or place the package next to"
      echo "     converge-node.sh / in CWD / in /var/tmp"
      PKG_ARG=""
    fi
  fi
fi
if [ -z "$PKG_ARG" ]; then
  if RESOLVED="$(_pkg_in_dir "$SCRIPT_DIR")" || \
     RESOLVED="$(_pkg_in_dir ".")" || \
     RESOLVED="$(_pkg_in_dir "/var/tmp")"; then
    PKG_ARG="$RESOLVED"
    echo "   package (beside script / CWD /var/tmp): $PKG_ARG"
  else
    PKG_ARG=""
  fi
fi

# `zarf init` needs the zarf-INIT package (registry/agent/injector images) IN the
# closed world. It has NO --init-package flag and only looks in the CWD or next to the
# zarf binary, so we discover it beside the deploy package and run the engine FROM that
# dir — that's how `zarf init` finds it air-gapped. The init package is LAYER A:
# transport zarf-init-<arch>-<zarfver>.tar.zst alongside the deploy package.
RUN_DIR="$PWD"
if [ -n "$PKG_ARG" ] && [ -f "$PKG_ARG" ]; then
  PKG_DIR="$(dirname "$PKG_ARG")"
  INIT_PKG="$(ls -t "$PKG_DIR"/zarf-init-*-*.tar.zst /var/tmp/zarf-init-*-*.tar.zst 2>/dev/null | head -1 || true)"
  if [ -n "$INIT_PKG" ]; then
    RUN_DIR="$(cd "$(dirname "$INIT_PKG")" && pwd)"
    echo "   zarf-init: $(basename "$INIT_PKG") (engine runs from $RUN_DIR so zarf init finds it)"
  else
    echo "   ⚠ NO zarf-init-*.tar.zst beside the deploy package — 'zarf init' will FAIL air-gapped."
    echo "     Transport it into $PKG_DIR (Layer A: registry/agent/injector images)."
  fi
fi

# --- S3 creds → tmpfs creds file (off argv); the engine forwards as ZARF_VAR_* -
CREDS_FILE="${CONVERGE_CREDS_FILE:-}"
OWN_CREDS=""   # set only if WE created it (a caller-provided file is the caller's to remove)
# Fallback: converge-aws.sh stages creds at this fixed tmpfs path and exports
# CONVERGE_CREDS_FILE — but that env can be dropped crossing `sudo -n env`, which
# silently strips S3 vars from the deploy (empty S3_BUCKET → "s3:" bucket errors).
# Pick the file up by its known path so a lost env var can't lose the creds.
if [ -z "$CREDS_FILE" ] && [ -f /dev/shm/.converge-creds ]; then
  CREDS_FILE="/dev/shm/.converge-creds"
fi
if [ -z "$CREDS_FILE" ]; then
  CREDS_LINES=""
  for v in S3_ENDPOINT S3_BUCKET S3_REGION S3_ACCESS_KEY S3_SECRET_KEY S3_SESSION_TOKEN; do
    [ -n "${!v:-}" ] && CREDS_LINES+="$v=${!v}"$'\n'
  done
  if [ -n "$CREDS_LINES" ]; then
    if [ -d /dev/shm ]; then CREDS_FILE="/dev/shm/.converge-creds.$$"; else CREDS_FILE="/tmp/.converge-creds.$$"; fi
    ( umask 077; printf '%s' "$CREDS_LINES" > "$CREDS_FILE" )
    OWN_CREDS="$CREDS_FILE"
    echo "   creds: $(printf '%s' "$CREDS_LINES" | grep -c .) S3 var(s) staged to tmpfs (off argv)"
  fi
fi
cleanup() { [ -n "$OWN_CREDS" ] && { shred -u "$OWN_CREDS" 2>/dev/null || rm -f "$OWN_CREDS"; }; return 0; }
trap cleanup EXIT

# --- assemble the engine argv (single array → safe under `set -u` on old bash) -
ARGS=(--kubectl "$KUBECTL_CMD")
[ -f "$MANIFEST_JSON" ] && ARGS+=(--manifest "$MANIFEST_JSON")
[ -d "$MANIFESTS_DIR" ] && ARGS+=(--manifests-dir "$MANIFESTS_DIR")
# Always pass zarf when we have one — remediations need it even if package discovery
# failed (clearer errors). Package is required for component deploy remediations.
[ -n "$ZARF_BIN" ] && ARGS+=(--zarf "$ZARF_BIN")
if [ -n "$PKG_ARG" ] && [ -f "$PKG_ARG" ] && [ -r "$PKG_ARG" ]; then
  ARGS+=(--package "$PKG_ARG")
  echo "   package: $PKG_ARG"
else
  echo "   package: <none> — component-deploy / image-push remediations will MANUAL"
  echo "            pass a path this process can read as argv2, or place the package"
  echo "            next to converge-node.sh / in CWD / in /var/tmp, e.g.:"
  echo "              $0 apply /path/you/chose/zarf-package-cybersec-dask-amd64-1.6.6.tar.zst"
fi
[ -n "$CREDS_FILE" ] && ARGS+=(--creds-file "$CREDS_FILE")
# Multi-core / air-gap baseline 4; set DASK_WORKER_REPLICAS=1 for tiny smoke hosts
ARGS+=(--set "DASK_WORKER_REPLICAS=${DASK_WORKER_REPLICAS:-4}")
# Optional sizing (must keep CPU limit >= nthreads). Engine 0.5.0 patches the live
# DaskCluster CR + recycles workers — no zarf re-push for scale/size alone.
[[ -n "${DASK_WORKER_NTHREADS:-}" ]] && ARGS+=(--set "DASK_WORKER_NTHREADS=${DASK_WORKER_NTHREADS}")
[[ -n "${DASK_WORKER_CPU:-}" ]] && ARGS+=(--set "DASK_WORKER_CPU=${DASK_WORKER_CPU}")
[[ -n "${DASK_WORKER_MEMORY:-}" ]] && ARGS+=(--set "DASK_WORKER_MEMORY=${DASK_WORKER_MEMORY}")
# v1.6.5 doc aliases (engine folds → DASK_WORKER_MEMORY / requests.memory)
[[ -n "${DASK_WORKER_MEM_LIMIT:-}" ]] && ARGS+=(--set "DASK_WORKER_MEM_LIMIT=${DASK_WORKER_MEM_LIMIT}")
[[ -n "${DASK_WORKER_MEM_REQUEST:-}" ]] && ARGS+=(--set "DASK_WORKER_MEM_REQUEST=${DASK_WORKER_MEM_REQUEST}")
# Optional terminal-WS override (NodePort/tunnel access; empty = auto-detect / ingress /ws)
[ -n "${PTY_PROXY_WS:-}" ] && ARGS+=(--set "PTY_PROXY_WS=${PTY_PROXY_WS}")
# Optional explicit ingress class; engine auto-detects nginx on RKE2 when unset
[ -n "${INGRESS_CLASS:-}" ] && ARGS+=(--set "INGRESS_CLASS=${INGRESS_CLASS}")
case "${CONVERGE_DYNAMIC_PROVISIONING:-}" in 1|true|yes) ARGS+=(--enable-dynamic-provisioning) ;; esac
case "${CONVERGE_NO_REGISTRY_PVC:-}" in 1|true|yes) ARGS+=(--no-registry-pvc) ;; esac
ARGS+=("$MODE_FLAG")

echo "🎯 converge ($MODE)   engine=$ZARF_DIR   kubectl=${KUBECTL_CMD%% *}"

# --- prepare the registry hostPath (resilient claimRef PV) -------------------
# The claimRef registry PV is a hostPath; kubelet creates it ROOT-OWNED, but the zarf
# registry container runs NON-root and fsGroup does NOT chown hostPath volumes — so the
# registry can't write its storage ("mkdir /var/lib/registry/docker: permission denied")
# and every image push 500s. Make it writable BEFORE the engine runs zarf init. The path
# matches REGISTRY_PV_YAML in zarf/converge/catalog.py; harmless in dynamic mode (unused).
if [ "$(id -u)" = 0 ]; then
  mkdir -p /var/lib/zarf-registry && chmod 0777 /var/lib/zarf-registry \
    && echo "   registry hostPath /var/lib/zarf-registry prepared (writable — registry runs non-root)"
fi

# --- run the engine ----------------------------------------------------------
# Run FROM $RUN_DIR (the dir holding the zarf-init package) so `zarf init` finds it
# air-gapped. Every engine path (PYTHONPATH, --manifest(s), --package, --creds-file) is
# absolute, so the cd is safe. `python3 -u` + PYTHONUNBUFFERED keep the progress log live
# through `tee` (block-buffering hides it otherwise); PYTHONDONTWRITEBYTECODE avoids a
# root-owned __pycache__ that would block the next restage.
set +e
cd "$RUN_DIR"
env PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH="$ZARF_DIR" KUBECONFIG="$KUBECONFIG" \
  python3 -u -m converge "${ARGS[@]}"
RC=$?
set -e

# After verify/apply: if the staged tree has verify-s3-datapath.sh, surface a clear
# human report (catalog T5.s3-datapath also gates converge). Skip on teardown.
S3_SCRIPT=""
for cand in "$ZARF_DIR/scripts/verify-s3-datapath.sh" "$SELF/verify-s3-datapath.sh"; do
  [ -f "$cand" ] && S3_SCRIPT="$cand" && break
done
if [ -n "$S3_SCRIPT" ] && [ "$MODE" != "teardown" ]; then
  echo ""
  echo "── S3 datapath (configured bucket + marker + span parquet) ──"
  set +e
  bash "$S3_SCRIPT" || true
  set -e
fi

exit "$RC"
