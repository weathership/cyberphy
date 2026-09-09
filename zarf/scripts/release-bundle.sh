#!/usr/bin/env bash
# Assemble the COMPLETE air-gap convergent-deploy release bundle into build/release/.
#
# Produces a self-contained set an operator transports into a closed world and runs
# top-to-bottom per zarf/AIRGAP-CONVERGE-RUNBOOK.md:
#   • cybersec-converge-<VER>.tar.gz   the convergence engine (FSM) — exactly what
#                                      converge-node.sh resolves at runtime + the runbook
#   • zarf-init-amd64-<ZARF_VER>.tar.zst  Layer-A init package (registry/agent/injector)
#   • zarf                             the v<ZARF_VER> Linux binary
#   • AIRGAP-CONVERGE-RUNBOOK.md       the operator procedure
#   • AIRGAP-CHEATSHEET.md            one-page walkthrough (checksummed)
#   • AIRGAP-DISCOVERY.md             read-only pre-flight audit (optional companion)
#   • AIRGAP-REMEDIATION-COMMANDS.md  full manual mirror of engine remediations
#   • SHA256SUMS                       over all of the above AND the 1.3 GB deploy package
#                                      (referenced in place at zarf/, never copied)
#
# The zarf-init package + binary are fetched once from Zarf upstream (cached) and re-hosted,
# so the release is a single source of truth. Driven by `just release-bundle`.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

VER="${RELEASE_VERSION:-1.6.7}"
ZARF_VER="${ZARF_VERSION:-v0.70.1}"
PKG="zarf/zarf-package-cybersec-dask-amd64-${VER}.tar.zst"
OUT="build/release"
ENGINE_TGZ="$OUT/cybersec-converge-${VER}.tar.gz"
INIT_PKG="$OUT/zarf-init-amd64-${ZARF_VER}.tar.zst"
ZARF_BIN="$OUT/zarf"

[ -f "$PKG" ] || { echo "❌ deploy package missing: $PKG  (build it: just package)" >&2; exit 1; }
mkdir -p "$OUT"

# 1. Engine tarball — the exact tree converge-node.sh resolves: itself + converge/ +
#    manifests/local-path-provisioner.yaml + artifacts.manifest.json (+ air-gap docs).
#    Tarred so `tar xzf … -C <dir>` lays the files directly under <dir> (no wrapper dir).
echo "→ building $(basename "$ENGINE_TGZ")"
STAGE="$(mktemp -d)"; trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/manifests" "$STAGE/scripts"
cp -R zarf/converge "$STAGE/"
cp zarf/scripts/converge-node.sh "$STAGE/"
# S3 datapath probe: catalog resolves scripts/ next to converge/; converge-node.sh
# also looks at $SELF/verify-s3-datapath.sh (stage root). Ship both paths.
cp zarf/scripts/verify-s3-datapath.sh "$STAGE/scripts/"
cp zarf/scripts/verify-s3-datapath.sh "$STAGE/"
cp zarf/artifacts.manifest.json "$STAGE/"
cp zarf/manifests/local-path-provisioner.yaml "$STAGE/manifests/"
for doc in AIRGAP-CONVERGE-RUNBOOK.md AIRGAP-CHEATSHEET.md \
           AIRGAP-DISCOVERY.md AIRGAP-REMEDIATION-COMMANDS.md; do
  [ -f "zarf/$doc" ] && cp "zarf/$doc" "$STAGE/"
done
find "$STAGE" -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true
find "$STAGE" -name '*.pyc' -delete 2>/dev/null || true
tar czf "$ENGINE_TGZ" -C "$STAGE" .

# 2. zarf-init package + binary (Layer A), fetched from upstream + re-hosted. Cached.
fetch() {
  local url="$1" dst="$2"
  [ -s "$dst" ] && { echo "  cached  $(basename "$dst")"; return; }
  echo "  fetch   $(basename "$dst")"
  curl -fsSL --retry 3 -o "$dst" "$url" || { echo "❌ fetch failed: $url" >&2; exit 1; }
}
fetch "https://github.com/zarf-dev/zarf/releases/download/${ZARF_VER}/zarf-init-amd64-${ZARF_VER}.tar.zst" "$INIT_PKG"
fetch "https://github.com/zarf-dev/zarf/releases/download/${ZARF_VER}/zarf_${ZARF_VER}_Linux_amd64" "$ZARF_BIN"
chmod +x "$ZARF_BIN"

# 3. Operator docs standalone on the release page (also inside the engine tgz).
DOCS=()
for doc in AIRGAP-CONVERGE-RUNBOOK.md AIRGAP-CHEATSHEET.md \
           AIRGAP-DISCOVERY.md AIRGAP-REMEDIATION-COMMANDS.md; do
  if [ -f "zarf/$doc" ]; then
    cp "zarf/$doc" "$OUT/$doc"
    DOCS+=("$OUT/$doc")
  fi
done

# 4. SHA256SUMS over every asset (basenames; deploy package referenced in place).
echo "→ SHA256SUMS"
: > "$OUT/SHA256SUMS"
for f in "$ENGINE_TGZ" "$INIT_PKG" "$ZARF_BIN" "${DOCS[@]}"; do
  ( cd "$(dirname "$f")" && sha256sum "$(basename "$f")" ) >> "$OUT/SHA256SUMS"
done
( cd "$(dirname "$PKG")" && sha256sum "$(basename "$PKG")" ) >> "$OUT/SHA256SUMS"

echo
echo "✅ release bundle → $OUT/"
ls -lh "$OUT"
echo "   deploy package (referenced in place): $PKG ($(du -h "$PKG" | cut -f1))"
echo
echo "── SHA256SUMS ──"; cat "$OUT/SHA256SUMS"
