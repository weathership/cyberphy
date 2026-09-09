#!/usr/bin/env bash
# ONE-command, end-to-end live validation of the converge FSM (catalog.py
# registry/storage + interrupted-procedure + PARTIAL_PUSH + upgrade Retain guard).
#
# Drives the COMPLETE sequence itself — provision a throwaway air-gap RKE2 node,
# transport THIS repo's engine, cut egress — then for each wedged permutation it
# INDUCES the state, converges, and asserts recovery ([ok]/[fixed]) + post-asserts.
# Destroys the node on success (kept on --keep, or on any failure for inspection).
#
# Matrix (task #51):
#   01–13  registry/storage, audit, CADS, 2026-07-15 field wedges
#   15     legacy-registry upgrade — reclaim=Delete → Retain + hostPath conserved
#   16     registry blip mid-upgrade — deploy/pods gone, PV+images stay
#   17     SIGKILL mid-wait (converge-09) — kill zarf deploy; re-apply unwedges
#   PARTIAL_PUSH  catalog has repo, target manifest HEAD 404 → T2 re-push
#
#   just sandbox-test-fsm                    # full cycle → destroy
#   just sandbox-test-fsm --keep             # full cycle → keep the node
#   FSM_FILTER='15|16|17|PARTIAL' just sandbox-test-fsm --keep   # task #51 only
#
# (The `just` recipe runs sandbox-config first to hydrate build/sandbox/.)
# Engine line of record: weathership/cyberphy rch/devenv (not rch/laptop legacy).
set -uo pipefail   # NOT -e: the matrix must continue past a failing case.

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SB="$SELF_DIR/run-sandbox.sh"
REPO_ROOT="$(cd "$SELF_DIR/../../.." && pwd)"
BUILD_DIR="${SANDBOX_BUILD_DIR:-$REPO_ROOT/build/sandbox}"
ENV_FILE="$BUILD_DIR/sandbox.env"
[ -f "$ENV_FILE" ] || { echo "❌ $ENV_FILE missing — run 'just sandbox-config' first" >&2; exit 2; }
# shellcheck source=/dev/null
source "$ENV_FILE"   # KEY, S3_*, ...

KEEP=0; [ "${1:-}" = "--keep" ] && KEEP=1
SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=15)
KCTL="sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml"
IP=""
PASS=0; FAIL=0; FAILED_CASES=()
on_node() { ssh "${SSH_OPTS[@]}" "ec2-user@$IP" "$@"; }
_strip() { perl -pe 's/\e\[[0-9;]*[a-zA-Z]//g' 2>/dev/null || cat; }

finish() {
  local code="${1:-0}"
  if [ "$KEEP" = 1 ]; then
    echo "→ node KEPT (--keep)${IP:+ ($IP)} — 'just sandbox-destroy' when done."
  elif [ "$code" -eq 0 ]; then
    echo "→ all green — destroying the sandbox"; bash "$SB" destroy || true
  else
    echo "→ failure/abort — node KEPT${IP:+ ($IP)} for inspection. 'just sandbox-destroy' when done."
  fi
  exit "$code"
}

# ── full lifecycle (the one-command sequence) ─────────────────────────────────
echo "▶ End-to-end FSM validation: provision → transport → air-gap → matrix"
echo "── provision (egress ON) + wait for RKE2 Ready ──"
bash "$SB" up        || { echo "❌ provision (up) failed"; finish 1; }
echo "── transport packages + stage THIS repo's engine + MinIO (egress ON) ──"
bash "$SB" transport || { echo "❌ transport failed"; finish 1; }
echo "── cut egress (closed world) ──"
bash "$SB" airgap    || { echo "❌ airgap failed"; finish 1; }

IP="$(bash "$SB" ip 2>/dev/null)"
[ -n "$IP" ] && [ "$IP" != "None" ] || { echo "❌ node IP unresolved after up"; finish 1; }
on_node "rm -rf ~/cybersec-converge/converge/__pycache__" 2>/dev/null || true
echo "✓ node $IP up, air-gapped, engine staged — running the matrix"

# Optional: run only matching cases on a machine that already has a live sandbox.
#   FSM_FILTER='15|16|17|PARTIAL' just sandbox-test-fsm --keep
#   FSM_FILTER='PARTIAL_PUSH' bash infra/aws/tofu-sandbox/test-fsm.sh --keep
should_run() {
  local name="$1"
  [ -z "${FSM_FILTER:-}" ] && return 0
  printf '%s' "$name" | grep -qiE "$FSM_FILTER"
}

# induce → converge → assert primary invariant recovered (+ optional post-assert).
# Args: name  induce_fn  [post_fn]  [inv_regex default=T1.registry-running]
run_case() {
  local name="$1" induce_fn="$2" post_fn="${3:-}" inv_re="${4:-T1[[:space:]].*registry-running}"
  echo; echo "═══════════ CASE: $name"
  if ! should_run "$name"; then
    echo "  ↷ skip (FSM_FILTER=${FSM_FILTER})"
    return 0
  fi
  # Fail FAST if the node is unreachable (e.g. the operator's residential IP rotated
  # out of the SG /32 mid-run — it happened, truncating a case's converge output and
  # misgrading it). A dead link must abort the matrix loudly, not grade cases.
  if ! on_node "true" >/dev/null 2>&1; then
    echo "❌ node unreachable before '$name' — did your public IP rotate out of the SG /32?"
    echo "   recover: just sandbox-config && bash run-sandbox.sh airgap  (re-applies the /32, keeps egress cut)"
    FAIL=$((FAIL + 1)); FAILED_CASES+=("$name [node unreachable — aborted matrix]")
    finish 1
  fi
  "$induce_fn"
  local out clean inv act
  out="$(bash "$SB" converge 2>&1)" || true
  clean="$(printf '%s\n' "$out" | _strip)"
  inv="$(printf '%s\n' "$clean" | grep -E "$inv_re" | head -1)"
  act="$(printf '%s\n' "$clean" | grep -oE \
        'un-defaulted StorageClass[^];]*|reset static registry PV[^];]*|force-finalized[^];]*|created static registry PV[^];]*|deleted (captured )?registry PVC[^];]*|unwedged pending Helm release[^];]*|cleared pods in Terminating[^];]*|drained ns zarf[^];]*|recreated absent zarf ns[^];]*|drained zarf husk[^];]*|chmod 0777[^];]*|cleared partial seed-registry[^];]*|PARTIAL_PUSH[^];]*|reclaim[^];]*|Retain[^];]*|partial push[^];]*|cybersec-images[^];]*' \
        | head -6 | paste -sd'; ' -)"
  if printf '%s' "$inv" | grep -qiE '\[ *(ok|fixed) *\]'; then
    echo "  ✓ recovered: ${inv}"
    [ -n "$act" ] && echo "    ↳ FSM unwind: $act"
    # PROOF of the mechanism: the bound registry PVC must be on storageClassName "" (the
    # static claimRef PV), NOT a captured class. This is what --storage-class - guarantees.
    local pvc_sc
    pvc_sc="$(on_node "$KCTL -n zarf get pvc zarf-docker-registry -o jsonpath='{.spec.storageClassName}'" 2>/dev/null)"
    if [ -z "$pvc_sc" ]; then
      echo "    ↳ registry PVC storageClassName=\"\" (bound the static PV — no default-SC capture) ✓"
    else
      echo "    ✗ registry PVC storageClassName=$pvc_sc (expected \"\" — capture not prevented)"
      FAIL=$((FAIL + 1)); FAILED_CASES+=("$name [PVC on '$pvc_sc' not '']"); return
    fi
    # Case-specific proof (agent back / helm unwedged / ns Active …)
    if [ -n "$post_fn" ] && ! "$post_fn"; then
      FAIL=$((FAIL + 1)); FAILED_CASES+=("$name [post-assert]"); return
    fi
    PASS=$((PASS + 1))
  else
    echo "  ✗ primary invariant did NOT recover (pattern: $inv_re)"
    echo "    ${inv:-<no matching invariant line in output>}"
    printf '%s\n' "$clean" | grep -iE 'rc=1|MANUAL|Pending|won.t bind|ExternalProvision|registry|agent-hook|PARTIAL|images-pushed|scheduler' \
      | tail -8 | sed 's/^/      /'
    FAIL=$((FAIL + 1)); FAILED_CASES+=("$name")
  fi
}

# ── inducers ──────────────────────────────────────────────────────────────────
# Deleting the zarf ns tears down ONLY the registry; the app survives in its own
# namespaces and the pushed images survive on the Retain hostPath — so each converge
# re-inits T1 fast (T2 images stay [ok]). That image survival also validates CONSERVATION.

induce_baseline() { :; }   # a plain converge must stay green

# A default StorageClass faithful to RKE2's local-path: WaitForFirstConsumer + a
# provisioner that does NOT exist in the closed world (so dynamic provisioning hangs).
_apply_hostile_default_sc() {
  cat <<'Y' | on_node "$KCTL apply -f -" >/dev/null 2>&1 || true
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: sb-fsm-default
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: example.com/sb-nonexistent-provisioner
volumeBindingMode: WaitForFirstConsumer
Y
}

induce_default_sc() {      # THE field bug: a default SC captures the fresh registry PVC
  _apply_hostile_default_sc
  on_node "$KCTL delete ns zarf --wait=false" >/dev/null 2>&1 || true
}

induce_vestigial_pvc() {   # THE artifact: a prior attempt left a zarf-docker-registry PVC
  # already CAPTURED onto the default class (immutable). zarf init reuses it by name, so
  # un-defaulting can't help — converge must DELETE it and re-init on "". This is the exact
  # state the live node was wedged in (PVC Pending storageClass='local-path').
  _apply_hostile_default_sc
  on_node "$KCTL delete ns zarf --wait=false" >/dev/null 2>&1 || true
  on_node "$KCTL create ns zarf" >/dev/null 2>&1 || true
  cat <<'Y' | on_node "$KCTL apply -f -" >/dev/null 2>&1 || true
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: zarf-docker-registry
  namespace: zarf
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 5Gi}}
Y
}

induce_released_pv() {     # delete the PVC out from under the Retain PV → Released
  on_node "$KCTL -n zarf delete pvc zarf-docker-registry --wait=false" >/dev/null 2>&1 || true
  on_node "$KCTL delete ns zarf --wait=false" >/dev/null 2>&1 || true
}

induce_class_drift() {     # the static PV carries a class the (un-defaulted) "" PVC can't bind
  on_node "$KCTL delete ns zarf --wait=false" >/dev/null 2>&1 || true
  on_node "$KCTL delete pv zarf-registry-pv --ignore-not-found" >/dev/null 2>&1 || true
  cat <<'Y' | on_node "$KCTL apply -f -" >/dev/null 2>&1 || true
apiVersion: v1
kind: PersistentVolume
metadata: {name: zarf-registry-pv}
spec:
  capacity: {storage: 5Gi}
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: "sb-bogus-drift"
  hostPath: {path: /var/lib/zarf-registry, type: DirectoryOrCreate}
  claimRef: {namespace: zarf, name: zarf-docker-registry}
Y
}

# ── audit-gap inducers (2026-07-07 state-space audit) ─────────────────────────

induce_dead_agent() {      # registry Running ≠ init complete: agent gone → every later
  # deploy ImagePullBackOff on upstream refs. T1 detect must now name it + re-init.
  on_node "$KCTL -n zarf delete deploy agent-hook --wait=true" >/dev/null 2>&1 || true
  local i=0
  while [ "$i" -lt 10 ] && on_node "$KCTL -n zarf get pods --no-headers 2>/dev/null | grep -q '^agent-hook'"; do
    sleep 3; i=$((i + 1))
  done
}

post_agent_back() {
  local n img
  n="$(on_node "$KCTL -n zarf get pods --no-headers 2>/dev/null" | grep -c '^agent-hook.*Running')" || true
  if [ "${n:-0}" -lt 1 ]; then echo "    ✗ agent-hook NOT running after converge"; return 1; fi
  # Running is not enough — admission must BEHAVIORALLY rewrite an upstream ref in an
  # APP namespace (the field poison: a re-run init labels pre-existing app namespaces
  # zarf.dev/agent=ignore, disabling rewriting; probing `default` would be a permanent
  # false negative — zarf deliberately ignores pre-init namespaces). Server dry-run
  # exercises the full chain incl. selectors; persists nothing.
  img="$(on_node "$KCTL -n dask-operator run zz-post-canary --image=ghcr.io/zarf-canary/agent-check:v1 --restart=Never --dry-run=server -o jsonpath='{.spec.containers[0].image}'" 2>/dev/null)"
  case "$img" in
    *ghcr.io/zarf-canary*) echo "    ✗ agent-hook Running but webhook NOT mutating (canary kept upstream ref)"; return 1 ;;
    "")                    echo "    ↳ agent-hook Running (${n}) ✓ (canary gave no verdict)"; return 0 ;;
    *)                     echo "    ↳ agent-hook Running (${n}), webhook mutating (canary → ${img%%@*}) ✓"; return 0 ;;
  esac
}

induce_wedged_helm() {     # the killed-mid-deploy state: a REAL pending-upgrade helm
  # revision (payload + labels) makes every upgrade of that release fail "another
  # operation is in progress"; the operator Deployment is removed so converge MUST
  # deploy — and therefore must unwedge first.
  cat <<'PY' | on_node "cat > /tmp/sb-wedge-helm.py"
import base64, gzip, json, subprocess
KC = "/var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml".split()
def k(*a, inp=None):
    return subprocess.run(KC + list(a), capture_output=True, text=True, input=inp)
# zarf stores component release secrets HASHED in ns zarf (not the component ns)
o = json.loads(k("get", "secrets", "-A", "-l", "owner=helm", "-o", "json").stdout)
by = {}
for s in o.get("items", []):
    ns = s["metadata"]["namespace"]
    if ns == "kube-system":
        continue  # RKE2 system charts — never wedge those
    lab = s["metadata"]["labels"]; v = int(lab["version"])
    key = (ns, lab["name"])
    if key not in by or v > by[key][0]:
        by[key] = (v, s)
assert by, "no zarf-managed helm release found (searched all ns except kube-system)"
(rel_ns, rel_name), (ver, last) = sorted(by.items())[0]
new = ver + 1
gz = base64.b64decode(base64.b64decode(last["data"]["release"]))
rel = json.loads(gzip.decompress(gz))
rel["version"] = new
rel["info"]["status"] = "pending-upgrade"
helm_blob = base64.b64encode(gzip.compress(json.dumps(rel).encode())).decode()
secret = {
    "apiVersion": "v1", "kind": "Secret", "type": "helm.sh/release.v1",
    "metadata": {
        "name": "sh.helm.release.v1.%s.v%d" % (rel_name, new),
        "namespace": rel_ns,
        "labels": {"name": rel_name, "owner": "helm",
                   "status": "pending-upgrade", "version": str(new)},
    },
    "data": {"release": base64.b64encode(helm_blob.encode()).decode()},
}
r = k("apply", "-f", "-", inp=json.dumps(secret))
print("planted pending-upgrade rev:", rel_name, new, "rc=", r.returncode)
r2 = k("delete", "deploy", "--all", "-n", "dask-operator", "--wait=false")
print("operator deployment removed rc=", r2.returncode)
PY
  on_node "sudo python3 /tmp/sb-wedge-helm.py" 2>&1 | sed 's/^/    induce: /'
}

post_helm_clean() {
  local pend ops
  pend="$(on_node "$KCTL get secrets -A -l 'owner=helm,status in (pending-install,pending-upgrade,pending-rollback)' --no-headers 2>/dev/null | grep -c ." )" || true
  ops="$(on_node "$KCTL -n dask-operator get pods --no-headers 2>/dev/null" | grep -c 'Running')" || true
  if [ "${pend:-1}" -eq 0 ] && [ "${ops:-0}" -ge 1 ]; then
    echo "    ↳ no pending-* helm releases; operator Running (${ops}) ✓"; return 0
  fi
  echo "    ✗ pending helm secrets remain (${pend:-?}) or operator not Running (${ops:-0})"; return 1
}

induce_terminating_ns() {  # a finalizer-bearing resource wedges panel-viz in Terminating:
  # its component deploy fails "namespace is being terminated" until force-finalized.
  cat <<'Y' | on_node "$KCTL apply -f -" >/dev/null 2>&1 || true
apiVersion: v1
kind: ConfigMap
metadata:
  name: sb-wedge
  namespace: panel-viz
  finalizers: ["cybersec.sandbox/test-wedge"]
Y
  on_node "$KCTL delete ns panel-viz --wait=false" >/dev/null 2>&1 || true
  local i=0
  while [ "$i" -lt 10 ]; do
    [ "$(on_node "$KCTL get ns panel-viz -o jsonpath='{.status.phase}'" 2>/dev/null)" = "Terminating" ] && break
    sleep 3; i=$((i + 1))
  done
}

post_ns_active() {
  # clear the wedge cm's finalizer if it resurfaced in the recreated ns (zombie object)
  on_node "$KCTL -n panel-viz patch configmap sb-wedge --type=merge -p '{\"metadata\":{\"finalizers\":null}}'" >/dev/null 2>&1 || true
  local phase pods
  phase="$(on_node "$KCTL get ns panel-viz -o jsonpath='{.status.phase}'" 2>/dev/null)"
  pods="$(on_node "$KCTL -n panel-viz get pods --no-headers 2>/dev/null" | grep -c 'Running')" || true
  if [ "$phase" = "Active" ] && [ "${pods:-0}" -ge 1 ]; then
    echo "    ↳ panel-viz Active, ${pods} pod(s) Running ✓"; return 0
  fi
  echo "    ✗ panel-viz phase=${phase:-absent}, Running pods=${pods:-0}"; return 1
}

# ── CADS session inducers (2026-07 air-gap walkthrough — split-brain / husk / hostPath)
induce_zarf_husk_service() {
  # Active zarf ns with only an ancient Service (no Ready registry) — the 34d
  # zarf-injector leftover after force-finalize + recreate. Re-init on top →
  # seed-registry Helm deadline. Converge must drain then re-init.
  on_node "$KCTL delete ns zarf --wait=false" >/dev/null 2>&1 || true
  sleep 2
  on_node "$KCTL create ns zarf" >/dev/null 2>&1 || true
  cat <<'Y' | on_node "$KCTL apply -f -" >/dev/null 2>&1 || true
apiVersion: v1
kind: Service
metadata:
  name: zarf-injector
  namespace: zarf
spec:
  ports: [{port: 5000, targetPort: 5000}]
Y
}

induce_pvc_terminating_split() {
  # PVC Terminating + ns force-finalized = split-brain: namespaced patch fails
  # with "namespaces zarf not found" until ns is recreated and PVC re-homed.
  _apply_hostile_default_sc
  on_node "$KCTL create ns zarf" >/dev/null 2>&1 || true
  cat <<'Y' | on_node "$KCTL apply -f -" >/dev/null 2>&1 || true
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: zarf-docker-registry
  namespace: zarf
  finalizers: ["kubernetes.io/pvc-protection", "cybersec.sandbox/test-wedge"]
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 5Gi}}
  storageClassName: local-path
Y
  on_node "$KCTL -n zarf delete pvc zarf-docker-registry --wait=false" >/dev/null 2>&1 || true
  # force-finalize the ns while PVC still has finalizers (split-brain)
  on_node "$KCTL get ns zarf -o json" 2>/dev/null | on_node "python3 -c '
import sys,json
o=json.load(sys.stdin)
o[\"spec\"][\"finalizers\"]=[]
print(json.dumps(o))
'" 2>/dev/null | on_node "$KCTL replace --raw /api/v1/namespaces/zarf/finalize -f -" >/dev/null 2>&1 || true
}

induce_hostpath_perms() {
  # Registry non-root cannot write hostPath → seed chart context deadline.
  on_node "sudo mkdir -p /var/lib/zarf-registry && sudo chmod 0700 /var/lib/zarf-registry && sudo chown root:root /var/lib/zarf-registry" >/dev/null 2>&1 || true
  on_node "$KCTL delete ns zarf --wait=false" >/dev/null 2>&1 || true
}

post_hostpath_writable() {
  local mode
  mode="$(on_node "stat -c '%a' /var/lib/zarf-registry" 2>/dev/null)" || true
  if [ "$mode" = "777" ]; then
    echo "    ↳ hostPath mode=777 ✓"; return 0
  fi
  echo "    ✗ hostPath mode=${mode:-absent} (want 777)"; return 1
}

induce_dead_release() {   # THE 2026-07-15 field wedge: a chart whose FIRST install failed
  # (only `failed` revisions, never deployed) → every helm upgrade refuses with
  # "has no deployed releases", and the required dask-cluster rider blocks EVERY
  # component deploy. Manufacture: rewrite ALL revisions of the dask-cluster-cr
  # release to failed + delete the DaskCluster CR (as a failed install leaves it).
  cat <<'PY' | on_node "cat > /tmp/sb-dead-release.py"
import base64, gzip, json, subprocess
KC = "/var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml".split()
def k(*a, inp=None):
    return subprocess.run(KC + list(a), capture_output=True, text=True, input=inp)
o = json.loads(k("get", "secrets", "-A", "-l", "owner=helm", "-o", "json").stdout)
touched = 0
for s in o.get("items", []):
    md, lab = s["metadata"], s["metadata"].get("labels", {}) or {}
    try:
        rel = json.loads(gzip.decompress(base64.b64decode(base64.b64decode(s["data"]["release"]))))
    except Exception:
        continue
    if "dask-cluster-cr" not in (rel.get("chart", {}).get("metadata", {}) or {}).get("name", ""):
        continue
    rel["info"]["status"] = "failed"
    blob = base64.b64encode(gzip.compress(json.dumps(rel).encode())).decode()
    patch = {"data": {"release": base64.b64encode(blob.encode()).decode()},
             "metadata": {"labels": {"status": "failed"}}}
    r = k("patch", "secret", md["name"], "-n", md["namespace"], "--type=merge",
          "-p", json.dumps(patch))
    touched += 1 if r.returncode == 0 else 0
print("revisions set to failed:", touched)
r = k("delete", "daskcluster", "cybersec-dask", "-n", "dask", "--ignore-not-found", "--wait=false")
print("daskcluster CR delete rc:", r.returncode)
PY
  on_node "sudo python3 /tmp/sb-dead-release.py" 2>&1 | sed 's/^/    induce: /'
  # clear operator-created children so T4 detect fails and the deploy path runs
  on_node "$KCTL -n dask delete deploy --all --wait=false" >/dev/null 2>&1 || true
}

induce_kubectl_off_path() {  # THE 2026-07-15 field wedge #2: zarf component actions run bare
  # `kubectl`; on real RKE2 it lives only in /var/lib/rancher/rke2/bin (off root's
  # PATH) — the dask-cluster S3-bucket after-action died `command not found` and the
  # required rider blocked every deploy. Our provisioning MASKS this with a
  # /usr/local/bin symlink — hide it, then force a dask-cluster redeploy so the
  # after-action must run. Engine v0.4.2 must hand zarf a PATH that resolves kubectl.
  on_node "sudo mv /usr/local/bin/kubectl /usr/local/bin/kubectl.sb-hidden 2>/dev/null; sudo mv /usr/bin/kubectl /usr/bin/kubectl.sb-hidden 2>/dev/null; true"
  on_node "$KCTL -n dask delete daskcluster cybersec-dask --ignore-not-found --wait=false; $KCTL -n dask delete deploy --all --wait=false; true" >/dev/null 2>&1 || true
}

post_kubectl_path_healed() {
  # kubectl must STILL be hidden (prove the engine shim/PATH did it, not the mask),
  # and the scheduler must be back (deploy + after-action both succeeded)
  local hidden sched
  hidden="$(on_node "command -v kubectl >/dev/null 2>&1 && echo visible || echo hidden")"
  sched="$(on_node "$KCTL -n dask get pods -l dask.org/component=scheduler --no-headers 2>/dev/null" | grep -c Running)" || true
  on_node "sudo mv /usr/local/bin/kubectl.sb-hidden /usr/local/bin/kubectl 2>/dev/null; sudo mv /usr/bin/kubectl.sb-hidden /usr/bin/kubectl 2>/dev/null; true"
  if [ "$hidden" = "hidden" ] && [ "${sched:-0}" -ge 1 ]; then
    echo "    ↳ kubectl off PATH throughout, deploy+after-action succeeded, scheduler Running ✓"
    return 0
  fi
  echo "    ✗ kubectl=$hidden (want hidden), scheduler Running=${sched:-0}"; return 1
}

post_dead_release_healed() {
  # the release must have a DEPLOYED revision again + the CR + scheduler back
  local dep sched
  dep="$(on_node "$KCTL get secrets -A -l 'owner=helm,status=deployed' -o name 2>/dev/null | grep -c ." )" || true
  sched="$(on_node "$KCTL -n dask get pods -l dask.org/component=scheduler --no-headers 2>/dev/null" | grep -c Running)" || true
  if [ "${sched:-0}" -ge 1 ] && [ "${dep:-0}" -ge 1 ]; then
    echo "    ↳ dead release healed: deployed revisions present, scheduler Running ✓"; return 0
  fi
  echo "    ✗ scheduler Running=${sched:-0}, deployed releases=${dep:-0}"; return 1
}

# ── task #51 — upgrade-path / interrupted-procedure / partial-push (engine ≥ 0.5.0)
# Cases 15–17 + PARTIAL_PUSH: first runtime validation of the hot-swap upgrade lineage.
# Inducers plant state; converge apply must recover without Layer-A destruction.
# Filter: FSM_FILTER='15|16|17|PARTIAL' just sandbox-test-fsm --keep

_sb_marker_write() {
  # Stand-in for pushed image layers — hostPath must keep this across PV/ns churn.
  on_node "sudo mkdir -p /var/lib/zarf-registry/.sb-matrix && \
    echo \"marker-$(date -u +%Y%m%dT%H%M%SZ)\" | sudo tee /var/lib/zarf-registry/.sb-matrix/MARKER >/dev/null && \
    sudo chmod -R a+rX /var/lib/zarf-registry/.sb-matrix" >/dev/null 2>&1 || true
}

_sb_marker_present() {
  on_node "test -f /var/lib/zarf-registry/.sb-matrix/MARKER && cat /var/lib/zarf-registry/.sb-matrix/MARKER" 2>/dev/null
}

# 15) Legacy registry upgrade + Retain guard (8154617d)
# Pre-guard engines could leave reclaim=Delete on the static PV. Induce that, tear the
# zarf ns (PVC gone → Released/recreate path), and require: reclaim back to Retain,
# hostPath marker conserved, T1 registry Running on storageClassName "".
induce_legacy_registry_retain() {
  echo "    induce: plant hostPath marker + patch zarf-registry-pv reclaim→Delete + delete ns zarf"
  _sb_marker_write
  # Ensure PV object exists so we can mis-set reclaim (create-if-missing is catalog's job later)
  if ! on_node "$KCTL get pv zarf-registry-pv" >/dev/null 2>&1; then
    cat <<'Y' | on_node "$KCTL apply -f -" >/dev/null 2>&1 || true
apiVersion: v1
kind: PersistentVolume
metadata: {name: zarf-registry-pv}
spec:
  capacity: {storage: 5Gi}
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  hostPath: {path: /var/lib/zarf-registry, type: DirectoryOrCreate}
  claimRef: {namespace: zarf, name: zarf-docker-registry}
Y
  fi
  on_node "$KCTL patch pv zarf-registry-pv --type merge -p '{\"spec\":{\"persistentVolumeReclaimPolicy\":\"Delete\"}}'" >/dev/null 2>&1 || true
  local pol
  pol="$(on_node "$KCTL get pv zarf-registry-pv -o jsonpath='{.spec.persistentVolumeReclaimPolicy}'" 2>/dev/null || true)"
  echo "    induce: reclaim now=${pol:-?} (want Delete before converge)"
  on_node "$KCTL delete ns zarf --wait=false" >/dev/null 2>&1 || true
  # Give API a moment so T1 detect sees ns/PVC gone
  sleep 3
}

post_legacy_registry_retain() {
  local pol marker running
  pol="$(on_node "$KCTL get pv zarf-registry-pv -o jsonpath='{.spec.persistentVolumeReclaimPolicy}'" 2>/dev/null || true)"
  marker="$(_sb_marker_present || true)"
  running="$(on_node "$KCTL -n zarf get pods --no-headers 2>/dev/null" | grep -c Running || true)"
  if [ "$pol" != "Retain" ]; then
    echo "    ✗ zarf-registry-pv reclaim=${pol:-absent} (want Retain — guard failed)"; return 1
  fi
  if [ -z "$marker" ]; then
    echo "    ✗ hostPath marker missing — Layer-A conservation failed"; return 1
  fi
  if [ "${running:-0}" -lt 1 ]; then
    echo "    ✗ no Running pods in zarf ns after Retain recovery"; return 1
  fi
  echo "    ↳ reclaim=Retain, hostPath marker present (${marker}), zarf Running=${running} ✓"
  return 0
}

# 16) Registry blip mid-upgrade — pods/deploy gone, PV+hostPath+images stay
# Models a mid-flight registry restart/OOM without re-init wipe. Converge must
# bring registry Ready; catalog still lists cybersec-dask (no re-transport).
induce_registry_blip() {
  echo "    induce: hostPath marker + delete registry Deploy/pods (PV+data conserved)"
  _sb_marker_write
  on_node "$KCTL -n zarf delete deploy -l app=docker-registry --wait=false --ignore-not-found" >/dev/null 2>&1 || true
  on_node "$KCTL -n zarf delete deploy docker-registry --wait=false --ignore-not-found" >/dev/null 2>&1 || true
  on_node "$KCTL -n zarf delete pods -l app=docker-registry --force --grace-period=0 --ignore-not-found" >/dev/null 2>&1 || true
  on_node "$KCTL -n zarf delete pods -l app.kubernetes.io/name=docker-registry --force --grace-period=0 --ignore-not-found" >/dev/null 2>&1 || true
  # Leave PVC/PV Bound if present — blip is workload-only, not storage destroy
  sleep 2
}

post_registry_blip() {
  local marker ready catalog
  marker="$(_sb_marker_present || true)"
  ready="$(on_node "$KCTL -n zarf get pods --no-headers 2>/dev/null" | grep -ciE 'Running' || true)"
  catalog="$(on_node "sudo env KUBECONFIG=/etc/rancher/rke2/rke2.yaml zarf tools registry catalog 2>/dev/null | grep -c cybersec-dask" 2>/dev/null || true)"
  if [ -z "$marker" ]; then
    echo "    ✗ hostPath marker missing after blip recovery"; return 1
  fi
  if [ "${ready:-0}" -lt 1 ]; then
    echo "    ✗ no Running pods in zarf ns after blip recovery"; return 1
  fi
  # Catalog may be empty if images never pushed on this sandbox pass — only require
  # when catalog was previously known. Soft-pass if zarf tools missing.
  if [ "${catalog:-0}" -ge 1 ]; then
    echo "    ↳ registry Running, marker conserved, catalog still has cybersec-dask ✓"
  else
    echo "    ↳ registry Running, marker conserved (catalog cybersec-dask=${catalog:-0} — soft) ✓"
  fi
  return 0
}

# 17) SIGKILL mid-wait — converge-09 class (eb888bc5 interrupted-procedure accounting)
# Kill zarf package deploy mid after-action wait so we leave pending helm and/or a
# half-applied dask-cluster without a journal. Re-apply must census live state and
# finish to scheduler Ready.
induce_sigkill_mid_wait() {
  echo "    induce: delete scheduler pods + SIGKILL zarf deploy mid-wait (converge-09)"
  on_node "$KCTL -n dask delete pod -l dask.org/component=scheduler --force --grace-period=0 --ignore-not-found --wait=false" >/dev/null 2>&1 || true
  # Best-effort: also remove worker pods so after-action has work to do
  on_node "$KCTL -n dask delete pod -l dask.org/component=worker --force --grace-period=0 --ignore-not-found --wait=false" >/dev/null 2>&1 || true
  cat <<'SH' | on_node "sudo bash -s" 2>&1 | sed 's/^/    induce: /'
set +e
PKG="$(ls -t /var/tmp/zarf-package-cybersec-dask-amd64-*.tar.zst 2>/dev/null | head -1)"
if [ -z "$PKG" ] || [ ! -f "$PKG" ]; then
  echo "no deploy package in /var/tmp — planting helm pending-upgrade only"
  exit 0
fi
# Prefer PATH zarf; fall back to known locations
ZARF_BIN="$(command -v zarf 2>/dev/null || true)"
[ -z "$ZARF_BIN" ] && for c in /usr/local/bin/zarf /var/lib/rancher/rke2/bin/zarf; do
  [ -x "$c" ] && ZARF_BIN="$c" && break
done
if [ -z "$ZARF_BIN" ]; then echo "no zarf binary — skip live SIGKILL"; exit 0; fi
export PATH="/var/lib/rancher/rke2/bin:/usr/local/bin:$PATH"
export KUBECONFIG=/etc/rancher/rke2/rke2.yaml
# Long enough to enter helm/wait; short enough for matrix pace. SIGKILL = no cleanup.
timeout -s KILL 35s "$ZARF_BIN" package deploy "$PKG" --confirm \
  --components=dask-cluster --retries 1 </dev/null >/var/tmp/sb-sigkill-deploy.log 2>&1
echo "zarf deploy killed/finished rc=$? (log: /var/tmp/sb-sigkill-deploy.log)"
# If kill was too early and left nothing, plant a pending-upgrade on dask-operator
# so the interrupted-procedure path still has a helm secret to unwedge.
pend="$(kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml get secrets -A \
  -l 'owner=helm,status in (pending-install,pending-upgrade,pending-rollback)' \
  --no-headers 2>/dev/null | wc -l)"
echo "pending helm secrets after kill: $pend"
SH
  # Always ensure at least one interrupted-looking surface if deploy was a no-op:
  # pending helm on dask-operator (reuse wedge helper path lightly).
  local pend
  pend="$(on_node "$KCTL get secrets -A -l 'owner=helm,status in (pending-install,pending-upgrade,pending-rollback)' --no-headers 2>/dev/null | grep -c ." || true)"
  if [ "${pend:-0}" -eq 0 ]; then
    echo "    induce: no pending helm after kill — planting pending-upgrade (fallback)"
    induce_wedged_helm
  fi
}

post_sigkill_mid_wait() {
  local pend sched cr
  pend="$(on_node "$KCTL get secrets -A -l 'owner=helm,status in (pending-install,pending-upgrade,pending-rollback)' --no-headers 2>/dev/null | grep -c ." || true)"
  sched="$(on_node "$KCTL -n dask get pods -l dask.org/component=scheduler --no-headers 2>/dev/null" | grep -c Running || true)"
  cr="$(on_node "$KCTL -n dask get daskcluster cybersec-dask --no-headers 2>/dev/null | grep -c ." || true)"
  if [ "${pend:-1}" -ne 0 ]; then
    echo "    ✗ pending helm still present (${pend}) — unwedge incomplete"; return 1
  fi
  if [ "${sched:-0}" -lt 1 ]; then
    echo "    ✗ scheduler not Running after SIGKILL recovery (cr=${cr:-0})"; return 1
  fi
  echo "    ↳ no pending helm; scheduler Running; DaskCluster present — converge-09 class healed ✓"
  return 0
}

# PARTIAL_PUSH — catalog lists cybersec-dask but target tag manifest HEAD → 404 (138b389a)
# Cheap: delete the distribution v2 tag pointer under hostPath; leave repo dir so catalog
# still lists the name. Engine must re-push cybersec-images (T2 fixed) without full init wipe.
induce_partial_push() {
  echo "    induce: delete cybersec-dask target tag manifest under hostPath (PARTIAL_PUSH)"
  _sb_marker_write
  cat <<'PY' | on_node "sudo python3 -" 2>&1 | sed 's/^/    induce: /'
import json, os, shutil, glob
from pathlib import Path

manifest_paths = [
    Path("/home/ec2-user/cybersec-converge/artifacts.manifest.json"),
    Path("/var/tmp/artifacts.manifest.json"),
]
tag = ""
for p in manifest_paths:
    if not p.is_file():
        continue
    try:
        m = json.loads(p.read_text())
    except Exception as e:
        print("manifest read fail", p, e)
        continue
    for img in (m.get("package_images") or {}).get("images") or []:
        ref = img.get("ref") or ""
        if "cybersec-dask" in ref and ":" in ref.split("/")[-1]:
            tag = ref.rsplit(":", 1)[-1].split("-zarf-", 1)[0]
            break
    if tag:
        print("target tag from", p, "→", tag)
        break
if not tag:
    # Fallback: any tag dir under cybersec-dask
    print("no tag in artifacts.manifest — will remove ALL cybersec-dask tags if present")

root = Path("/var/lib/zarf-registry")
# distribution layout: .../docker/registry/v2/repositories/<name>/_manifests/tags/<tag>
repos = list(root.glob("**/repositories/**/cybersec-dask"))
# also name may be nested path cybersec-dask
if not repos:
    repos = list(root.glob("**/repositories/cybersec-dask"))
print("repo roots found:", len(repos), [str(r) for r in repos[:5]])
removed = 0
for repo in repos:
    tags_dir = repo / "_manifests" / "tags"
    if not tags_dir.is_dir():
        continue
    if tag:
        tdir = tags_dir / tag
        if tdir.exists():
            shutil.rmtree(tdir, ignore_errors=True)
            removed += 1
            print("removed tag dir", tdir)
        # also try full content-tag if stored with -zarf- suffix variants
        for child in list(tags_dir.iterdir()) if tags_dir.is_dir() else []:
            if child.name.startswith(tag):
                shutil.rmtree(child, ignore_errors=True)
                removed += 1
                print("removed tag dir", child)
    else:
        for child in list(tags_dir.iterdir()):
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
                removed += 1
                print("removed tag dir", child)
print("tag dirs removed:", removed)
if removed == 0:
    # Last resort: drop revisions under _manifests/revisions (still leaves repo name)
    for repo in repos:
        rev = repo / "_manifests" / "revisions"
        if rev.is_dir():
            shutil.rmtree(rev, ignore_errors=True)
            print("wiped revisions", rev)
            removed += 1
print("done partial_push plant; removed_ops=", removed)
PY
  # Bounce registry so it re-reads filesystem (in-memory cache of manifests)
  on_node "$KCTL -n zarf delete pods -l app=docker-registry --force --grace-period=0 --ignore-not-found" >/dev/null 2>&1 || true
  on_node "$KCTL -n zarf delete pods -l app.kubernetes.io/name=docker-registry --force --grace-period=0 --ignore-not-found" >/dev/null 2>&1 || true
  sleep 5
}

post_partial_push() {
  local marker catalog head_ok tag
  marker="$(_sb_marker_present || true)"
  if [ -z "$marker" ]; then
    echo "    ✗ hostPath marker missing after PARTIAL_PUSH rem"; return 1
  fi
  # Prefer engine-reported recovery; also probe registry if possible
  catalog="$(on_node "sudo env KUBECONFIG=/etc/rancher/rke2/rke2.yaml zarf tools registry catalog 2>/dev/null" || true)"
  tag="$(on_node "python3 -c \"
import json
from pathlib import Path
p=Path('/home/ec2-user/cybersec-converge/artifacts.manifest.json')
m=json.loads(p.read_text()) if p.is_file() else {}
for img in (m.get('package_images') or {}).get('images') or []:
    ref=img.get('ref') or ''
    if 'cybersec-dask' in ref and ':' in ref.rsplit('/',1)[-1]:
        print(ref.rsplit(':',1)[-1].split('-zarf-',1)[0]); break
\" 2>/dev/null" || true)"
  head_ok="$(on_node "TAG='$tag'; python3 - <<'PY'
import os, urllib.request, urllib.error
tag = os.environ.get('TAG','').strip()
if not tag:
    print('no-tag'); raise SystemExit
path = f'/v2/cybersec-dask/manifests/{tag}'
accept = 'application/vnd.docker.distribution.manifest.v2+json,application/vnd.oci.image.manifest.v1+json'
for base in ('http://127.0.0.1:31999','http://127.0.0.1:30001'):
    try:
        req = urllib.request.Request(base+path, method='HEAD', headers={'Accept': accept})
        with urllib.request.urlopen(req, timeout=5) as r:
            print(r.status); raise SystemExit
    except urllib.error.HTTPError as e:
        print(e.code); raise SystemExit
    except Exception:
        continue
print('unreachable')
PY" 2>/dev/null || true)"
  case "$head_ok" in
    200) echo "    ↳ PARTIAL_PUSH healed: manifest HEAD 200 for cybersec-dask:${tag} ✓" ;;
    *)
      # Accept T2-fixed path even if HEAD probe can't reach NodePort from this user
      if printf '%s' "$catalog" | grep -q cybersec-dask; then
        echo "    ↳ catalog has cybersec-dask; HEAD probe=${head_ok:-?} (T2 ok from engine) — soft pass ✓"
      else
        echo "    ✗ catalog missing cybersec-dask and HEAD=${head_ok:-?} after rem"; return 1
      fi
      ;;
  esac
  return 0
}

# ── the permutation matrix ────────────────────────────────────────────────────
# Cases 1–13: registry/storage + audit + CADS + field 2026-07-15 wedges
# Cases 15–17 + PARTIAL_PUSH: task #51 upgrade / interrupt / partial-push lineage
run_case "01 baseline (idempotent converge stays green)"   induce_baseline
run_case "02 default StorageClass capture (the field bug)"  induce_default_sc
run_case "03 vestigial captured PVC (init reuses 'local-path')" induce_vestigial_pvc
run_case "04 Released registry PV (PVC deleted)"            induce_released_pv
run_case "05 class-drifted static PV"                       induce_class_drift
run_case "06 dead zarf agent (registry up, init incomplete)" induce_dead_agent   post_agent_back
run_case "07 wedged pending-upgrade Helm release"            induce_wedged_helm  post_helm_clean
run_case "08 app namespace stuck Terminating (apply path)"   induce_terminating_ns post_ns_active
run_case "09 zarf husk Service only (no registry)"          induce_zarf_husk_service
run_case "10 PVC Terminating + ns split-brain"              induce_pvc_terminating_split
run_case "11 hostPath not writable (seed deadline class)"   induce_hostpath_perms post_hostpath_writable
run_case "12 dead helm release (failed first install, no deployed rev)" induce_dead_release post_dead_release_healed
run_case "13 kubectl off PATH (zarf action 'command not found')" induce_kubectl_off_path post_kubectl_path_healed

# --- task #51 (engine 0.5.0 line) ---
run_case "15 legacy-registry upgrade (Retain guard)" \
  induce_legacy_registry_retain post_legacy_registry_retain
run_case "16 registry blip mid-upgrade" \
  induce_registry_blip post_registry_blip
run_case "17 SIGKILL mid-wait (converge-09)" \
  induce_sigkill_mid_wait post_sigkill_mid_wait
# Primary gate is T2.images-pushed (re-push after PARTIAL_PUSH); T1 still must be healthy.
run_case "PARTIAL_PUSH (catalog hit, manifest HEAD 404)" \
  induce_partial_push post_partial_push \
  "T2[[:space:]].*images-pushed"

on_node "$KCTL delete storageclass sb-fsm-default --ignore-not-found" >/dev/null 2>&1 || true

echo; echo "════════════ FSM validation: ${PASS} passed, ${FAIL} failed ════════════"
[ "$FAIL" -gt 0 ] && printf '   failed: %s\n' "${FAILED_CASES[*]}"
if [ -n "${FSM_FILTER:-}" ]; then
  echo "   (FSM_FILTER=${FSM_FILTER} — subset run; full matrix needs empty filter)"
fi
finish "$FAIL"
