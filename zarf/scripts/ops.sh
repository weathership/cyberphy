#!/usr/bin/env bash
# Build / package / redeploy ops for the cybersec-dask image.
#
# Bakes in this session's manual redeploy steps:
#   - a CONTENT-DERIVED image tag (BASE-<hash of image inputs>), so every image
#     change is a NEW tag -> the converge drift detect / set-image rolls it;
#   - a dual-tag + LOCAL-REGISTRY push, so `zarf package create` finds the fresh
#     image via the registry regardless of a stale podman DOCKER_HOST socket;
#   - the closure/size gate;
#   - `redeploy` (FAST): push only the ~MB app layer to the live registry NodePort
#     (base layers dedup) + `kubectl set image`, rolling in minutes, no 1.3G transport;
#   - `redeploy-full`: transport the package + drift-aware converge apply (air-gap path).
#
# Usage:  ops.sh {tag|image|package|redeploy|redeploy-full}
# Driven by `just image|package|redeploy`. Env overrides: IMAGE_BASE_VER, LOCAL_REGISTRY,
# AWS_ACCOUNT, BASTION_SG, SSH_KEY, LOCAL_FWD_PORT.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

IMG="cybersec-dask"
BASE_VER="${IMAGE_BASE_VER:-2025.2.0}"          # the ghcr.io/dask/dask base it builds on
LOCAL_REG="${LOCAL_REGISTRY:-localhost:5555}"
DOCKERFILE="zarf/images/Dockerfile.cybersec-dask"

# ---------------------------------------------------------------- content tag
# Files whose CONTENT defines the image — NOT the tag-bearing manifests (that would
# make the hash self-referential). Mirrors what the Dockerfile COPYs/installs.
_image_inputs() {
  local f
  for f in "$DOCKERFILE" zarf/images/requirements-airgap.txt \
           zarf/images/requirements-agent.txt zarf/images/otel-navigator.py \
           zarf/images/data-view.py zarf/images/data_view_lib.py \
           zarf/scripts/generate-vpc-flow.py zarf/scripts/generate_hdf5.py \
           zarf/images/loader.js; do
    [ -f "$f" ] && echo "$f"
  done
  # App code + standalone SDK baked into the image (not tag-bearing manifests).
  find cybersec config packages/hdf5_iceberg zarf/images/sample-notebooks -type f \
    -not -path '*/__pycache__/*' -not -name '*.pyc' \
    -not -path '*/.pytest_cache/*' -not -name '*.egg-info' 2>/dev/null
}
content_tag() {
  local h
  h=$(_image_inputs | sort -u | xargs sha256sum 2>/dev/null | sha256sum | cut -c1-10)
  echo "${BASE_VER}-${h}"
}
# The source files that carry the image tag (kept in lockstep with the build).
_tag_files() {
  printf '%s\n' zarf/zarf.yaml zarf/artifacts.manifest.json \
    zarf/manifests/engine.yaml zarf/manifests/panel-viz.yaml \
    zarf/manifests/dask-cluster.yaml zarf/manifests/jupyterhub-values.yaml \
    zarf/manifests/vpc-flow-generator.yaml
}

# CLOSURE: every image REFERENCED by a k8s manifest must be DECLARED in
# artifacts.manifest.json — a ref the package doesn't carry can never pull in
# the closed world. (Field 2026-07-30: vpc-flow-generator.yaml escaped the
# lockstep with a stale ':2025.2.0-notebook' tag — 550 ImagePullBackOffs on a
# fresh air-gap node; masked on upgraded nodes by conserved-registry leftovers.
# Fix landed as 7d44eb09 on matrix-050; absorb before any 1.6.8 package cut.)
check_manifest_image_closure() {
  local refs declared missing=0 r t
  declared="$(grep -oE "${IMG}:[A-Za-z0-9._-]+" zarf/artifacts.manifest.json | sort -u)"
  refs="$(grep -rhoE "image: *[a-z0-9.:/-]*${IMG}:[A-Za-z0-9._-]+" zarf/manifests/*.yaml \
          | grep -oE "${IMG}:[A-Za-z0-9._-]+" | sort -u)"
  for r in $refs; do
    t="${r##*:}"
    case "$t" in \#\#\#*|\**) continue ;; esac   # zarf-templated tags resolve at deploy
    if ! printf '%s\n' "$declared" | grep -qx "$r"; then
      echo "  ✗ CLOSURE: zarf/manifests references ${r} but artifacts.manifest.json does not declare it" >&2
      missing=1
    fi
  done
  return $missing
}

current_tag() { grep -hoE "${IMG}:[A-Za-z0-9._-]+" zarf/zarf.yaml | head -1 | cut -d: -f2-; }
bump_tag() {  # idempotent — rewrites the tag in the source manifests only if it changed
  local new="$1" old f
  old="$(current_tag)"
  if [ "$old" = "$new" ]; then echo "  image tag already ${new} (no manifest change)"; return 0; fi
  for f in $(_tag_files); do
    # Full image refs: cybersec-dask:<old>
    sed -i "s|${IMG}:${old}|${IMG}:${new}|g" "$f"
    # Helm values style: tag: "2025.2.0-notebook" (jupyterhub-values.yaml)
    sed -i "s|tag: \"${old}\"|tag: \"${new}\"|g" "$f"
  done
  echo "  bumped image tag ${old} -> ${new}"
}

_builder() { command -v podman >/dev/null 2>&1 && echo podman || echo docker; }
_registry_up() { curl -sf "http://${LOCAL_REG}/v2/" >/dev/null 2>&1; }

# ---------------------------------------------------------------- build/package
do_image() {
  local tag; tag="$(content_tag)"
  echo "[image] content tag = ${IMG}:${tag}"
  bump_tag "$tag"
  local B; B="$(_builder)"
  echo "[image] $B build (linux/amd64, dual-tag)..."
  $B build --platform linux/amd64 \
    -t "${IMG}:${tag}" -t "${LOCAL_REG}/${IMG}:${tag}" \
    -f "$DOCKERFILE" --build-arg BASE_IMAGE="ghcr.io/dask/dask:${BASE_VER}" .
  if _registry_up; then
    echo "[image] push ${LOCAL_REG}/${IMG}:${tag} (so zarf package create finds it)..."
    $B push --tls-verify=false "${LOCAL_REG}/${IMG}:${tag}"
  else
    echo "[image] WARN: local registry ${LOCAL_REG} unreachable — skipped push;" \
         "zarf package create will fall back to the daemon (may need DOCKER_HOST fixed)."
  fi
  echo "[image] done: ${IMG}:${tag}"
}
do_package() {
  # ConfigMap is packaged as a file — must re-embed before create or JH ships stale notebooks.
  echo "[package] embed + verify sample notebooks (HDF5 + sidecars for in-situ JH seed)..."
  python3 zarf/scripts/verify-sample-notebooks.py
  echo "[package] manifest image-closure check (referenced vs declared)..."
  check_manifest_image_closure || {
    echo "[package] ERROR: manifest references undeclared image(s) — fix before packaging" >&2
    exit 1
  }
  echo "[package] zarf package create (image tag $(current_tag))..."
  ( cd zarf && zarf package create --confirm )
  local pkg; pkg="$(ls -t zarf/zarf-package-${IMG}-amd64-*.tar.zst 2>/dev/null | head -1)"
  [ -n "$pkg" ] || { echo "[package] ERROR: no package produced"; exit 1; }
  echo "[package] closure gate on $(basename "$pkg")..."
  python3 zarf/scripts/check-closure.py "$pkg"
}

# ---------------------------------------------------------------- live connection
SSH_KEY="${SSH_KEY:-$HOME/.ssh/cybersec-dask.pem}"
SG="${BASTION_SG:-sg-06ec172a5360ee1c0}"
SSH_O=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=25
       -o ServerAliveInterval=15 -o ServerAliveCountMax=10 -o GSSAPIAuthentication=no)
BASTION=""; CP=""; BUCKET=""; REGION=""; MYIP=""; PROXY=""
KC="sudo -n /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml"
cp_ssh() { ssh -i "$SSH_KEY" -o ProxyCommand="$PROXY" "${SSH_O[@]}" ec2-user@"$CP" "$@"; }
_revoke_sg() { [ -n "$MYIP" ] && aws ec2 revoke-security-group-ingress --group-id "$SG" \
  --protocol tcp --port 22 --cidr "$MYIP" >/dev/null 2>&1 && echo "[sg] revoked $MYIP"; }
_connect() {
  pushd infra/aws/tofu >/dev/null
  BASTION="$(tofu output -raw bastion_public_ip 2>/dev/null || true)"
  CP="$(tofu output -json control_plane_private_ips 2>/dev/null | jq -r '.[0] // empty' || true)"
  BUCKET="$(tofu output -raw s3_bucket_name 2>/dev/null || true)"
  REGION="$(tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"' 2>/dev/null || echo us-east-1)"
  popd >/dev/null
  [ -n "$BASTION" ] && [ -n "$CP" ] || { echo "[connect] cluster not provisioned (no tofu coords)"; exit 1; }
  local want="${AWS_ACCOUNT:-050330818249}" acct
  acct="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
  [ "$acct" = "$want" ] || { echo "[connect] WRONG ACCOUNT $acct (want $want) — abort"; exit 1; }
  MYIP="$(curl -s --max-time 10 https://checkip.amazonaws.com)/32"
  trap _revoke_sg EXIT
  aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 \
    --cidr "$MYIP" >/dev/null 2>&1 && echo "[connect][sg] authorized $MYIP"
  sleep 8
  PROXY="ssh -i $SSH_KEY -W %h:%p ${SSH_O[*]} ec2-user@$BASTION"
  echo "[connect] account=$acct bastion=$BASTION cp=$CP bucket=$BUCKET region=$REGION"
}
_verify_live() {
  cp_ssh "$KC -n panel-viz get pods -o wide | grep -E 'navigator-engine|otel-navigator';
    echo -n '  image: '; $KC -n panel-viz get pod -l app=otel-navigator -o jsonpath='{.items[0].spec.containers[0].image}'; echo;
    P=\$($KC -n panel-viz get pod -l app=otel-navigator -o jsonpath='{.items[0].metadata.name}');
    echo -n '  otel-navigator.py lines: '; $KC -n panel-viz exec \$P -c otel-navigator -- wc -l /app/otel-navigator.py 2>/dev/null"
}

# ---------------------------------------------------------------- redeploy (FAST, WIP)
# EXPERIMENTAL — pushes only the app-layer delta to the live registry NodePort via an
# SSH tunnel, then set-image to the direct registry ref (the kubelet pulls it with node
# creds, bypassing the agent rewrite). The IDEA works; the macOS OBSTACLE is that
# `podman push` runs from the podman VM, so `localhost:<fwd>` is the VM's localhost, not
# the host's tunnel -> the push hangs. FIXES (TODO #30): push to
# `host.containers.internal:<fwd>` (VM->host gateway); OR host-side skopeo
# (`skopeo copy docker-archive:<podman-save> docker://localhost:<fwd>/...`); OR build the
# image on the bastion/CP (AWS-side) and push to the registry directly (no laptop tunnel).
do_redeploy_fast() {
  local tag; tag="$(current_tag)"
  echo "[redeploy] ${IMG}:${tag} — image-delta push + set-image roll (fast)"
  _connect
  local NP PASS
  NP="$(cp_ssh "$KC -n zarf get svc zarf-docker-registry -o jsonpath='{.spec.ports[0].nodePort}'" || true)"
  PASS="$(cp_ssh 'sudo -n zarf tools get-creds registry' 2>/dev/null | tr -d '[:space:]')"
  [ -n "$NP" ] && [ -n "$PASS" ] || { echo "[redeploy] no registry NodePort/creds — try redeploy-full"; exit 1; }
  echo "[redeploy] live registry NodePort=$NP"
  local LP="${LOCAL_FWD_PORT:-5999}" B; B="$(_builder)"
  ssh -i "$SSH_KEY" -o ProxyCommand="$PROXY" "${SSH_O[@]}" -L "$LP:127.0.0.1:$NP" -N -f ec2-user@"$CP"
  sleep 2
  echo "$PASS" | $B login --tls-verify=false -u zarf-push --password-stdin "localhost:$LP" >/dev/null
  echo "[redeploy] pushing ${IMG}:${tag} (base layers dedup -> only the app delta moves)..."
  $B push --tls-verify=false "${IMG}:${tag}" "localhost:$LP/${IMG}:${tag}"
  pkill -f "ssh.*-L $LP:127.0.0.1:$NP" 2>/dev/null || true
  local REF="127.0.0.1:$NP/${IMG}:${tag}"
  echo "[redeploy] kubectl set image -> $REF"
  cp_ssh "$KC -n panel-viz set image deploy/navigator-engine navigator-engine=$REF"
  cp_ssh "$KC -n panel-viz set image deploy/otel-navigator otel-navigator=$REF pty-proxy=$REF"
  cp_ssh "$KC -n panel-viz rollout status deploy/navigator-engine --timeout=150s; $KC -n panel-viz rollout status deploy/otel-navigator --timeout=200s"
  echo "[redeploy] verify:"; _verify_live
}

# ---------------------------------------------------------------- redeploy (FULL)
# Transport the package + drift-aware converge apply (the air-gap-clean path).
do_redeploy_full() {
  local tag; tag="$(current_tag)"
  echo "[redeploy-full] ${IMG}:${tag} — package transport + converge apply"
  _connect
  local PKG; PKG="$(ls -t zarf/zarf-package-${IMG}-amd64-*.tar.zst 2>/dev/null | head -1)"
  [ -n "$PKG" ] || { echo "[redeploy-full] no package — run: just package"; exit 1; }
  local PN; PN="$(basename "$PKG")"
  scp -i "$SSH_KEY" "${SSH_O[@]}" "$SSH_KEY" "ec2-user@$BASTION:/home/ec2-user/.ssh/$(basename "$SSH_KEY")" >/dev/null
  ssh -i "$SSH_KEY" "${SSH_O[@]}" ec2-user@"$BASTION" "chmod 600 ~/.ssh/$(basename "$SSH_KEY")"
  local lmd5 rmd5
  lmd5="$( (md5 -q "$PKG" 2>/dev/null || md5sum "$PKG" | cut -d' ' -f1) )"
  rmd5="$(cp_ssh "md5sum /var/tmp/$PN 2>/dev/null | cut -d' ' -f1" || true)"
  if [ -n "$rmd5" ] && [ "$lmd5" = "$rmd5" ]; then
    echo "[redeploy-full] package already on CP (md5 match) — skip transport"
  else
    echo "[redeploy-full] transporting $(du -h "$PKG"|cut -f1) laptop->bastion->CP (slow hop)..."
    scp -i "$SSH_KEY" "${SSH_O[@]}" "$PKG" "ec2-user@$BASTION:/var/tmp/$PN"
    ssh -i "$SSH_KEY" "${SSH_O[@]}" ec2-user@"$BASTION" \
      "scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i ~/.ssh/$(basename "$SSH_KEY") /var/tmp/$PN ec2-user@$CP:/var/tmp/$PN"
  fi
  export S3_ENDPOINT="" S3_BUCKET="$BUCKET" S3_REGION="$REGION"
  bash zarf/scripts/converge-aws.sh apply
  echo "[redeploy-full] verify:"; _verify_live
}

case "${1:-}" in
  tag)            content_tag ;;
  image)          do_image ;;
  package)        do_package ;;
  redeploy)       do_redeploy_full ;;     # reliable converge path (default)
  redeploy-fast)  do_redeploy_fast ;;     # EXPERIMENTAL — podman-VM tunnel obstacle (see header)
  *) echo "usage: ops.sh {tag|image|package|redeploy|redeploy-fast}" >&2; exit 2 ;;
esac
