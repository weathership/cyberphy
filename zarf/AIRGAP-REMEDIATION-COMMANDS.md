# Air-Gap K8s/RKE2 Remediation Commands — Manual Mirror of `converge-node.sh`

**Purpose.** Copy-paste-ready `kubectl`/`zarf` commands for **every** eventuality the convergence
engine handles, so you can drive the deployment to target **by hand** — when `zarf`/the package
isn't on the host, when you want surgical control, or to understand exactly what the engine does.
Each block is the manual equivalent of one remediation in `zarf/converge/catalog.py`.

> **Engine emits these in situ.** On `MANUAL` / `FAILED` / dry-run `WOULD_FIX`, converge prints an
> **IN SITU** section with the same DISCOVER + FIX recipes (SSOT: `zarf/converge/manual.py`),
> including `zarf package deploy --components=…` for every app tier. This markdown file is the
> human-readable mirror; prefer the live report when you are already on the node after a run.

> **The supported path is still the engine.** It's idempotent, ordered, and structurally cannot
> delete a transported (Layer-A) artifact. Reach for these manual commands deliberately, not by
> default. Pair with [AIRGAP-DISCOVERY.md](AIRGAP-DISCOVERY.md) (confirm state first) and
> [AIRGAP-CONVERGE-RUNBOOK.md](AIRGAP-CONVERGE-RUNBOOK.md) (the narrative).

---

## 0. Setup + the golden path

Run as **root on the control-plane node**. Define the helpers (persist for your session):

```bash
export KUBECONFIG=/etc/rancher/rke2/rke2.yaml
export PATH="$PATH:/var/lib/rancher/rke2/bin"   # zarf component actions run bare `kubectl` —
                                                # off root's PATH on RKE2, deploys die
                                                # `kubectl: command not found` without this
kc()  { /var/lib/rancher/rke2/bin/kubectl --kubeconfig "$KUBECONFIG" "$@" 2>/dev/null \
        || zarf tools kubectl --kubeconfig "$KUBECONFIG" "$@"; }
cri() { /var/lib/rancher/rke2/bin/crictl --runtime-endpoint unix:///run/k3s/containerd/containerd.sock "$@"; }
PKG=$(ls -t /var/tmp/zarf-package-cybersec-dask-amd64-*.tar.zst 2>/dev/null | head -1); echo "PKG=$PKG"
```

**The one command that does all of the below, in order, to a fixpoint:**

```bash
umask 077; cat > /dev/shm/s3-creds <<'EOF'
S3_ENDPOINT=<endpoint>
S3_BUCKET=<bucket>
S3_REGION=<region>
S3_ACCESS_KEY=<key>
S3_SECRET_KEY=<secret>
EOF
sudo env CONVERGE_CREDS_FILE=/dev/shm/s3-creds bash ~/cybersec-converge/converge-node.sh apply
shred -u /dev/shm/s3-creds
```

The sections below are what that unwinds, tier by tier. **Conservation rule (applies throughout):
NEVER `crictl rmi`/`ctr images rm`/prune a Layer-A image, and NEVER delete the registry hostPath
data** — the `Retain` PV conserves pushed images so the registry rebinds with no re-push.

---

## T0 — Node

### T0.api — Kubernetes API unreachable
```bash
systemctl status rke2-server --no-pager | head -20
journalctl -u rke2-server -n 50 --no-pager
systemctl restart rke2-server        # last resort; RKE2 supervises kubelet/containerd/etcd
```

### T0.node-ready — node cordoned (`uncordon`)
```bash
kc get nodes
kc uncordon <node-name>              # engine: _rem_node_ready
```

### T0.no-disk-pressure — disk-pressure taint (**Layer A — free disk SAFELY**)
Remove only safe-to-delete bytes. **Never** `crictl rmi --prune` (evicts Layer-A images that can't
be re-fetched air-gapped).
```bash
df -h /var/lib/rancher /var/tmp
# Safe to remove: a redundant copy of the package tarball, journald logs, Failed/Evicted pods.
journalctl --vacuum-size=200M
kc get pods -A --field-selector=status.phase=Failed -o name | xargs -r -n1 kc delete
# The taint clears on its own once usage drops below the eviction threshold.
```

**Apply the conservation kubelet policy on an EXISTING node** (one we didn't provision — RKE2
defaults run image GC at **85%** disk, which silently deletes Layer-A images, and evict at 10–15%).
This is the literal block our ansible templates render; append it to the node's RKE2 config and
restart (restarting `rke2-server` does NOT disrupt running pods — containerd keeps them):
```bash
grep -q 'image-gc-high-threshold' /etc/rancher/rke2/config.yaml 2>/dev/null || cat >> /etc/rancher/rke2/config.yaml <<'EOF'
kubelet-arg:
  - "eviction-hard=imagefs.available<2%,nodefs.available<2%,nodefs.inodesFree<2%,memory.available<100Mi"
  - "eviction-minimum-reclaim=imagefs.available=1%,nodefs.available=1%"
  - "image-gc-high-threshold=100"
  - "image-gc-low-threshold=99"
EOF
systemctl restart rke2-server
```
> If the file already has a `kubelet-arg:` list from the operator, MERGE these four entries into it
> instead of appending a duplicate key (YAML duplicate keys — last one wins, silently dropping theirs).

### Layer-A image missing on the node (CLOSURE; only checked in dynamic-provisioning mode)
Re-import from the package OCI layout or RKE2's bundled-images dir — **never pull**:
```bash
cri images | grep -Ei 'local-path|busybox'        # confirm which is absent
# Option A — RKE2 auto-imports tarballs placed here on (re)start:
#   cp <image>.tar /var/lib/rancher/rke2/agent/images/ && systemctl restart rke2-server
# Option B — import directly into containerd's k8s.io namespace:
/var/lib/rancher/rke2/bin/ctr -a /run/k3s/containerd/containerd.sock -n k8s.io images import <image>.tar
```

---

## T0.5 — Storage

### Resilient default — claimRef registry PV (no StorageClass needed)
Prep the hostPath writable (the registry runs **non-root**; `fsGroup` does not chown hostPath — skip
this and every image push 500s with "permission denied"), then apply the static PV:
```bash
mkdir -p /var/lib/zarf-registry && chmod 0777 /var/lib/zarf-registry   # converge-node.sh does this

cat <<'YAML' | kc apply -f -            # engine: _rem_registry_pv / REGISTRY_PV_YAML (sc="")
apiVersion: v1
kind: PersistentVolume
metadata:
  name: zarf-registry-pv
spec:
  capacity:
    storage: 5Gi
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  hostPath:
    path: /var/lib/zarf-registry
    type: DirectoryOrCreate
  claimRef:
    namespace: zarf
    name: zarf-docker-registry
YAML
```

### Dynamic provisioning (opt-in only) — default StorageClass + provisioner
Only for a resourced multi-node cluster with a working provisioner (`CONVERGE_DYNAMIC_PROVISIONING=1`):
```bash
kc apply -f ~/cybersec-converge/manifests/local-path-provisioner.yaml   # bundled, node-preloaded image
kc patch sc local-path -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
kc -n local-path-storage get pods -l app=local-path-provisioner
```

---

## T1 — Zarf init / registry  ⚠ the wedge-prone tier

The engine's `_pre_init_cleanup` unwinds **every** registry/storage state that makes `zarf init`
loop at `rc=1`, then runs `zarf init --storage-class -`. Do the same by hand, **A→D in order**,
then init. All steps are idempotent and Layer-B (images conserved by the Retain PV).

### A. `zarf` namespace wedged `Terminating` → force-finalize
Clearing `spec.finalizers` via the `/finalize` subresource is the only thing that releases it:
```bash
kc get namespace zarf -o json \
  | python3 -c 'import sys,json;o=json.load(sys.stdin);o["spec"]["finalizers"]=[];print(json.dumps(o))' \
  | kc replace --raw /api/v1/namespaces/zarf/finalize -f -        # engine: _force_finalize_ns
```

### B. Un-default any cluster-default StorageClass (hygiene)
So a class-omitting PVC falls through to `""` instead of waiting on a dead provisioner:
```bash
for sc in $(kc get sc -o json | python3 -c 'import sys,json;[print(s["metadata"]["name"]) for s in json.load(sys.stdin)["items"] if (s["metadata"].get("annotations") or {}).get("storageclass.kubernetes.io/is-default-class")=="true"]'); do
  kc patch storageclass "$sc" -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'   # engine: _undefault_sc
done
```

### C. Delete the captured registry PVC (its `storageClassName` is IMMUTABLE)
The docker-registry chart omits `storageClassName` on an empty value, so the cluster-default SC
(RKE2 `local-path`) gets stamped on → `Pending` forever. `zarf init` reuses an existing PVC by
name, so the only fix is to remove it. **Skip if it's already `Bound` on `sc=""`.**
```bash
kc -n zarf get pvc zarf-docker-registry -o jsonpath='phase={.status.phase} sc={.spec.storageClassName}{"\n"}' 2>/dev/null
kc delete pvc zarf-docker-registry -n zarf --ignore-not-found --wait=false     # engine: _force_delete_pvc
# if it lingers Terminating on the pvc-protection finalizer:
kc -n zarf get pvc zarf-docker-registry >/dev/null 2>&1 \
  && kc patch pvc zarf-docker-registry -n zarf --type=merge -p '{"metadata":{"finalizers":null}}'
```

### D. Ensure the static `""` registry PV exists and is bindable
Create it if absent; **reset** it (delete the OBJECT + reapply — hostPath data conserved by Retain)
if it's `Released`/`Failed`, carries a stale `claimRef.uid`, or its class drifted off `""`. Never
adopt a captured class; the target is **always** `""`.
```bash
kc get pv zarf-registry-pv -o jsonpath='phase={.status.phase} sc={.spec.storageClassName}{"\n"}' 2>/dev/null
# reset a stale/mis-classed/Released PV (data survives — Retain):
kc delete pv zarf-registry-pv --ignore-not-found
# then re-apply the sc="" PV from the T0.5 block above.
```

### Run `zarf init` on explicit empty storageClass
The `-` sentinel renders `storageClassName: ""` explicitly → binds the static PV directly, no
provisioner, no default-SC race. Run from the dir holding the **zarf-init package** so it's found
air-gapped:
```bash
cd /var/tmp                          # holds zarf-init-<arch>-<ver>.tar.zst (Layer A)
zarf init --confirm --set=REGISTRY_PVC_SIZE=5Gi --storage-class=-     # engine: _rem_registry_running
```
**By-hand quick recovery** (the captured-PVC case, condensed):
```bash
kc -n zarf delete pvc zarf-docker-registry --ignore-not-found
cd /var/tmp && zarf init --confirm --storage-class -
```
Verify the registry came up and bound on `""`:
```bash
kc -n zarf get pods -l app=docker-registry
kc -n zarf get pvc zarf-docker-registry -o jsonpath='{.status.phase} sc={.spec.storageClassName}{"\n"}'   # want: Bound sc=""
```
> Registry running on `emptyDir` instead (no PV)? That's `--no-registry-pvc` mode — `zarf init`
> without `--storage-class`, and the PVC steps don't apply (storage is lost on pod restart).

### E. Agent missing / orphaned zarf webhook (registry up ≠ init complete)
The **agent-hook** pods rewrite image refs at admission. Registry `Running` but agent absent/dead ⇒
every subsequent deploy's pods `ImagePullBackOff` against `ghcr.io`/`quay.io` (refs never rewritten).
Re-running init is safe and idempotent — it redeploys the agent + webhook (**but see §E2 — a
re-init has a poisonous side effect you must undo immediately after**):
```bash
kc -n zarf get pods | grep agent-hook || { cd /var/tmp && zarf init --confirm --storage-class -; }
```
A **force-finalized** zarf ns leaves the cluster-scoped `MutatingWebhookConfiguration` behind
(webhooks are NOT namespaced). If deploys fail with admission-webhook errors while the zarf ns is
absent, remove the orphan (re-init recreates it properly):
```bash
kc get ns zarf >/dev/null 2>&1 || kc get mutatingwebhookconfiguration -o name 2>/dev/null | grep -i zarf | xargs -r kc delete
```

### E2. ⚠ Namespace poison — EVERY re-run of `zarf init` plants this (sandbox-proven)
`zarf init` labels every **pre-existing** namespace `zarf.dev/agent=ignore`. On a re-init that
includes OUR app namespaces — which **disables image rewriting** there: the agent skips them, so
any pod created afterward keeps its upstream ref (`ghcr.io/...`) and `ImagePullBackOff`s forever in
the closed world. Latent until pod churn (reboot/eviction/redeploy). **Strip after every re-init**
(the engine does this automatically; by hand):
```bash
for ns in dask dask-operator panel-viz jupyterhub; do
  kc label namespace "$ns" zarf.dev/agent- --overwrite 2>/dev/null || true    # engine: _strip_agent_ignore
done
# then clean up any pods ALREADY admitted with upstream refs (a no-diff helm upgrade will
# NEVER recreate them — delete them; their controllers re-admit through the active agent):
kc get pods -A | grep -E 'ImagePullBackOff|ErrImagePull' | awk '{print $1, $2}' | while read -r ns pod; do
  img=$(kc -n "$ns" get pod "$pod" -o jsonpath='{.spec.containers[0].image}')
  case "$img" in 127.0.0.1:*) ;; *) kc -n "$ns" delete pod "$pod" --wait=false ;; esac   # engine: _unwedge_unmutated_pods
done
# verify with the app-namespace canary (discovery §6b): the dry-run ref must come back REWRITTEN.
```

### E3. Stale/broken Dask children (operator only creates on CR CREATION)
The dask operator (kopf) builds the scheduler/worker Deployments **only on the DaskCluster's
creation event** — it neither propagates CR env changes to existing children nor recreates a
deleted child (both sandbox-proven; deleting a drifted Deployment STRANDS the cluster). So fix at
the **CR level**: delete the DaskCluster and redeploy the `dask-cluster` component — the re-apply
is a fresh CREATE and the operator builds everything with the CR's (correct) env:
```bash
kc -n dask delete daskcluster cybersec-dask --ignore-not-found --wait=false   # engine: _unwedge_broken_dask_cluster
kc -n dask get daskcluster cybersec-dask >/dev/null 2>&1 \
  && kc -n dask patch daskcluster cybersec-dask --type=merge -p '{"metadata":{"finalizers":null}}'  # kopf finalizer
# then redeploy (required component — any deploy carries it; use the env setup from above):
zarf package deploy "$PKG" --confirm --components=dask-cluster --retries 10 "${SETV[@]}"
kc -n dask get pods    # scheduler + workers return with the CR's env
```

### F. Wedged Helm release — "another operation (install/upgrade/rollback) is in progress"
A converge/zarf killed mid-deploy leaves the release `pending-install`/`pending-upgrade`; every
retry then fails with this exact error. Recovery: delete the **latest pending** release secret
(this reverts helm's view to the previous deployed revision), then redeploy that component:
```bash
kc get secrets -A -l 'owner=helm,status in (pending-install,pending-upgrade,pending-rollback)' \
   -o custom-columns='NS:.metadata.namespace,NAME:.metadata.name,RELEASE:.metadata.labels.name,VER:.metadata.labels.version,STATUS:.metadata.labels.status'
# for each wedged row (highest VER of that release):
kc -n <NS> delete secret <NAME>
# then redeploy the affected component (T2–T6 table below) — helm proceeds cleanly.
```

### F2. DEAD Helm release — "has no deployed releases"
Different from §F: here the release's **first install failed**, so its history holds only
`failed` revisions — there is no previous deployed revision to fall back to, and every later
`helm upgrade` refuses with `unable to install chart … has no deployed releases`, forever.
Because required components ride every `zarf package deploy`, ONE dead release (field case:
`dask-cluster-cr`) blocks EVERY component deploy — scheduler/jupyterhub/sample-notebooks all
fail on the same chart hash. The engine auto-recovers on the next `converge --apply`; manual
equivalent is zarf's own recommendation — remove the failing **component**, so the next deploy
INSTALLS instead of upgrading:
```bash
# identify: latest revision failed AND no deployed/superseded revision anywhere in history
kc get secrets -A -l owner=helm \
   -o custom-columns='NS:.metadata.namespace,RELEASE:.metadata.labels.name,VER:.metadata.labels.version,STATUS:.metadata.labels.status'
# the failing component is named in the deploy error: unable to deploy component "<name>"
zarf package remove "$PKG" --confirm --components=<name>
# then redeploy that component (T2–T6 table below; S3 env FIRST) — fresh install succeeds.
```

### G. App namespace stuck `Terminating` during an APPLY
A leftover Terminating `dask`/`panel-viz`/`jupyterhub` ns makes its component deploy fail with
"namespace is being terminated". Clear pods FIRST (force-finalizing a ns with live pods orphans
them), then finalize, then redeploy:
```bash
NS=<the-stuck-ns>
kc delete pods --all -n "$NS" --force --grace-period=0 --wait=false
kc get namespace "$NS" -o json | python3 -c 'import sys,json;o=json.load(sys.stdin);o["spec"]["finalizers"]=[];print(json.dumps(o))' \
  | kc replace --raw "/api/v1/namespaces/$NS/finalize" -f -
```

### H. Corrupt registry storage (Bound `""` PVC but registry crashloops / pulls 500)
E.g. disk-full mid-push corrupted blobs. Registry **content is Layer-B** (rebuilt from the package
by re-push), so wiping it is legal — this is the ONE case where removing `/var/lib/zarf-registry`
data is correct. Scripted recovery:
```bash
kc -n zarf scale deploy zarf-docker-registry --replicas=0 2>/dev/null || true
kc -n zarf delete pvc zarf-docker-registry --ignore-not-found --wait=false
kc -n zarf get pvc zarf-docker-registry >/dev/null 2>&1 && kc patch pvc zarf-docker-registry -n zarf --type=merge -p '{"metadata":{"finalizers":null}}'
kc delete pv zarf-registry-pv --ignore-not-found
rm -rf /var/lib/zarf-registry && mkdir -p /var/lib/zarf-registry && chmod 0777 /var/lib/zarf-registry
# re-apply the sc="" PV (T0.5 block), then:
cd /var/tmp && zarf init --confirm --storage-class -
zarf package deploy "$PKG" --confirm --components=cybersec-images --retries 10   # re-push (Layer A intact)
```

---

## T2–T6 — Component deploys (S3 env FIRST, for EVERY deploy)

**Required-components semantics (important):** in Zarf, `required: true` components deploy on
**every** `zarf package deploy`, regardless of `--components`. In this package `cybersec-images`,
`dask-operator`, and `dask-cluster` are all required — so *every* deploy below also (re)pushes the
images and (re)deploys the Dask stack (idempotent Helm upgrades; don't be surprised by the extra
work). Consequence: **`dask-cluster` templates the S3 vars into the scheduler/worker env on every
deploy** — a deploy run with empty S3 vars silently strips the workers' S3 access (no IMDS exists
air-gapped). So set up the S3 environment **before ANY deploy**, not just the app tiers.

**Credential discipline (matches the engine exactly):** **non-secret** config (`S3_BUCKET`,
`S3_ENDPOINT`, `S3_REGION`) goes on `--set-variables`; S3 **secrets** ride a **`ZARF_CONFIG`
tmpfs file** (`[package.deploy.set]`) — zarf's first-class config path. Never put a secret on the
command line — **and never rely on bare `ZARF_VAR_*` env for package variables: it does NOT reach
zarf v0.70.1's templating** (field-proven twice: a configMap rendered `S3_BUCKET=""` on the live
deploy, and the sandbox DaskCluster rendered empty AWS creds, silently de-credentialing every
worker). `S3_BUCKET` is **required** for `panel-viz`/`navigator-engine` — a blank bucket renders
`OTEL_DATA_PATH=s3:///` and bricks the app, so the engine refuses it; you should too.

```bash
# Load values off the process table (e.g. from your tmpfs creds-file):
set -a; . /dev/shm/s3-creds; set +a
# SECRETS -> a 0600 tmpfs ZARF_CONFIG file (the mechanism that actually reaches templating):
umask 077; cat > /dev/shm/zarf-secrets.toml <<EOF
[package.deploy.set]
S3_ACCESS_KEY = "$S3_ACCESS_KEY"
S3_SECRET_KEY = "$S3_SECRET_KEY"
EOF
export ZARF_CONFIG=/dev/shm/zarf-secrets.toml
# NON-SECRETS -> --set-variables:
SETV=(--set-variables=S3_BUCKET="$S3_BUCKET" --set-variables=S3_ENDPOINT="$S3_ENDPOINT" --set-variables=S3_REGION="$S3_REGION")
# ... run the deploys (table below) ... then clean up:
#   shred -u /dev/shm/zarf-secrets.toml; unset ZARF_CONFIG
```

### T2 — push app images to the internal registry
```bash
zarf package deploy "$PKG" --confirm --components=cybersec-images --retries 10 "${SETV[@]}"   # engine: _rem_images_pushed
zarf tools registry catalog | grep cybersec-dask        # confirm pushed
```

### T3–T6 — per-invariant deploys (all after the env setup above)

| Tier / invariant | Manual command |
|---|---|
| **T3** `dask-operator` | `zarf package deploy "$PKG" --confirm --components=dask-operator --retries 10 "${SETV[@]}"` |
| **T4** `scheduler` (dask-cluster) | `zarf package deploy "$PKG" --confirm --components=dask-cluster --retries 10 "${SETV[@]}"` |
| **T5** `otel-navigator` | `zarf package deploy "$PKG" --confirm --components=cybersec-images,panel-viz --retries 10 "${SETV[@]}"` |
| **T5** `navigator-engine` | `zarf package deploy "$PKG" --confirm --components=cybersec-images,navigator-engine --retries 10 "${SETV[@]}"` |
| **T5** `jupyterhub` | `zarf package deploy "$PKG" --confirm --components=jupyterhub,sample-notebooks --retries 10 "${SETV[@]}"`  (field unblock; engine co-deploys notebooks) |
| **T5** `sample-notebooks` | `zarf package deploy "$PKG" --confirm --components=sample-notebooks --retries 10 "${SETV[@]}"` |
| **T6** `ingress` | `zarf package deploy "$PKG" --confirm --components=ingress --retries 10 "${SETV[@]}"` |

(`panel-viz`/`navigator-engine` name `cybersec-images` explicitly so a tag-drift redeploy pushes the
new image before the rollout; the push is idempotent either way since the component is required.)

Clean up the exported secrets when done: `unset ZARF_VAR_S3_ACCESS_KEY ZARF_VAR_S3_SECRET_KEY ZARF_VAR_S3_SESSION_TOKEN`.

---

## T4.workers-capacity — workers oversubscribed / sizing drift (engine ≥ 0.5.0)
Strands `otel-navigator` when workers oversubscribe node memory. Engine does **surgical**
remediation (no zarf re-push):

1. Fold aliases: `DASK_WORKER_MEM_LIMIT` → `DASK_WORKER_MEMORY`, optional `MEM_REQUEST`
2. Target replicas = `min(DASK_WORKER_REPLICAS, floor((total_alloc_Gi − 8) / worker_Gi))`, then
   shrink further if workers are still `Pending`
3. Merge-patch live `DaskCluster/cybersec-dask` for **replicas + nthreads + cpu + memory**
4. Bounce worker pods when template fields change (operator often skips rolls)
5. Reap excess/orphaned worker Deployments (least-ready first)

Canonical vars (package + engine): `DASK_WORKER_REPLICAS` / `NTHREADS` / `CPU` / `MEMORY`.
```bash
export DASK_WORKER_REPLICAS=${DASK_WORKER_REPLICAS:-4}
export DASK_WORKER_NTHREADS=${DASK_WORKER_NTHREADS:-2}
export DASK_WORKER_CPU=${DASK_WORKER_CPU:-2}
export DASK_WORKER_MEMORY=${DASK_WORKER_MEMORY:-6Gi}
# Prefer: converge apply with the env above (engine: _rem_workers_capacity)
# Manual replica-only cap:
kc patch daskcluster cybersec-dask -n dask --type merge -p "{\"spec\":{\"worker\":{\"replicas\":$DASK_WORKER_REPLICAS}}}"
# After template edits, force pickup:
kc -n dask delete pod -l dask.org/component=worker --force --grace-period=0 --wait=false
# Reap excess worker Deployments, Pending/least-ready first:
kc -n dask get deploy -l dask.org/component=worker --sort-by=.status.readyReplicas -o name \
  | head -n -$DASK_WORKER_REPLICAS | xargs -r -n1 kc -n dask delete --wait=false
kc -n dask get pods -l dask.org/component=worker -o wide
```

## T5.otel-navigator / panel stack — full remediation matrix

The engine (`T5.otel-navigator`, `T5.navigator-engine`) walks this matrix on
`converge --apply`. Use the same order by hand when driving install in situ.

| Failure mode | DISCOVER | FIX |
|---|---|---|
| **Namespace / deploy missing** | `kc get ns panel-viz; kc -n panel-viz get deploy` | `zarf package deploy "$PKG" --confirm --components=cybersec-images,panel-viz --retries 10 "${SETV[@]}"` |
| **Blank `S3_BUCKET` / `OTEL_DATA_PATH=s3:///`** (pod may still be Ready — TCP probe) | `kc -n panel-viz get cm otel-navigator-config -o jsonpath='…'` | Redeploy **with** S3 env (SETV + ZARF_CONFIG). Never redeploy with empty bucket. |
| **ImagePullBackOff** | `kc -n panel-viz describe pod -l app=otel-navigator \| sed -n '/Events:/,$p'` | `--components=cybersec-images,panel-viz` (push then roll). If upstream refs: strip `zarf.dev/agent=ignore` on ns (§E). |
| **Image tag drift** | compare running tag vs package | same deploy line (images + panel-viz) |
| **Pending / unschedulable (4Gi)** | `kc get pods -A --field-selector=status.phase=Pending`; worker Pending count | Cap workers (T4.workers-capacity) **then** redeploy/recycle panel |
| **CrashLoopBackOff / OOMKilled** | `kc -n panel-viz logs deploy/otel-navigator -c otel-navigator --tail=80` | redeploy; then `kc -n panel-viz delete pod -l app=otel-navigator --force --grace-period=0` |
| **1/2 containers** (panel vs pty-proxy) | ready counts on containerStatuses | recycle pods; if pty-proxy can't reach engine → deploy `navigator-engine` |
| **Dead Helm release** | helm secrets `failed` only; error `has no deployed releases` | `zarf package remove "$PKG" --confirm --components=panel-viz` then redeploy |
| **navigator-engine down** | `kc -n panel-viz get pods,ep -l app=navigator-engine` | `--components=cybersec-images,navigator-engine` (if shared CM blank, panel-viz first) |
| **UI up, terminal WS dead** | ingress paths; NodePort 30765 | `--components=ingress`; optional `PTY_PROXY_WS=ws://<node>:30765` |
| **vpc-flow-generator broken** | `kc -n panel-viz get deploy vpc-flow-generator` | best-effort only — does **not** block panel-viz component (wait removed) |

```bash
# Primary in-situ unblock (same as engine _rem_otel_navigator):
set -a; . /dev/shm/s3-creds; set +a   # S3_* required
# … SETV + ZARF_CONFIG as in T2–T6 preamble …
zarf package deploy "$PKG" --confirm --components=cybersec-images,panel-viz --retries 10 "${SETV[@]}"
# Full stack heal (navigator + terminal + routes):
zarf package deploy "$PKG" --confirm \
  --components=cybersec-images,panel-viz,navigator-engine,ingress --retries 10 "${SETV[@]}"
# Capacity strand:
#   see T4.workers-capacity, then:
kc -n panel-viz delete pod -l app=otel-navigator --force --grace-period=0 --wait=false
# Confirm:
kc -n panel-viz get pods -o wide     # otel-navigator 2/2, engine 1/1
kc -n panel-viz get cm otel-navigator-config \
  -o jsonpath='S3_BUCKET={.data.S3_BUCKET}{"\n"}OTEL_DATA_PATH={.data.OTEL_DATA_PATH}{"\n"}'
curl -sS -o /dev/null -w '%{http_code}\n' --connect-timeout 3 http://127.0.0.1:30506/otel-navigator
```

### T5.s3-datapath — configured S3 + generated spans readable

Pods can be Ready while the data path is dead. **SSOT script** (no secrets on argv;
reads ConfigMap + exec into app/scheduler):

```bash
bash zarf/scripts/verify-s3-datapath.sh          # human report; exit 0/1
bash zarf/scripts/verify-s3-datapath.sh --json   # machine-readable
bash zarf/scripts/verify-s3-datapath.sh --allow-empty   # auth only (pre-seed)
```

| Failure | Meaning | Fix |
|---|---|---|
| blank ConfigMap bucket | panel-viz rendered empty S3 | redeploy with SETV (T5.otel-navigator) |
| akid_len=0 | secret never templated | redeploy panel-viz + dask-cluster with ZARF_CONFIG secrets |
| cannot list bucket | endpoint/network/creds | fix S3_ENDPOINT / keys; path-style used when endpoint set |
| marker missing | no `_active_dataset.json` | run `OTEL_Data_Generator.ipynb` or `generate-otel-data.py` |
| parquet_count=0 | marker points at empty prefix | re-generate spans under `{dataset}/spans/` |
| sample unreadable | list OK, GET fails | bucket policy / path-style / gateway |

`converge --verify` includes **T5.s3-datapath** (depends on otel-navigator + scheduler).
`converge-node.sh` also prints the script report after every verify/apply.

---



## Teardown — clean-slate the Layer-B app stack (registry/SC + images CONSERVED)
The manual inverse of `reconcile()`; mirrors `engine.teardown`. A subsequent deploy redeploys fast
from the still-present registry.
```bash
# 1. neutralize Dask CR finalizers so the (possibly absent) operator can't deadlock the delete
#    (each `get` may error if the CRD itself is absent — that's fine, the loop skips it)
for kind in daskclusters daskworkergroups daskautoscalers daskjobs; do
  kc get "$kind" -A -o json 2>/dev/null | python3 -c '
import sys,json
try: items=json.load(sys.stdin).get("items",[])
except Exception: items=[]
for it in items:
  m=it["metadata"]; print(m.get("namespace","-"), m["name"])
' 2>/dev/null | while read -r ns name; do
    [ "$name" = "" ] && continue
    if [ "$ns" = "-" ]; then kc patch "$kind" "$name" --type merge -p '{"metadata":{"finalizers":null}}'
    else kc patch "$kind" "$name" -n "$ns" --type merge -p '{"metadata":{"finalizers":null}}'; fi
  done
done
# 2. delete the app-stack namespaces (platform tier conserved)
for ns in dask dask-operator jupyterhub panel-viz; do kc delete namespace "$ns" --wait=false --ignore-not-found; done
# 3. settle: force-remove lingering pods FIRST (force-finalizing a ns with live pods orphans them),
#    then force-finalize a ns still stuck Terminating (last resort):
for i in 1 2 3 4 5 6; do
  stuck=$(for ns in dask dask-operator jupyterhub panel-viz; do kc get ns "$ns" >/dev/null 2>&1 && echo "$ns"; done)
  [ -z "$stuck" ] && break
  for ns in $stuck; do kc delete pods --all -n "$ns" --force --grace-period=0 --wait=false 2>/dev/null; done
  [ "$i" -ge 3 ] && for ns in $stuck; do
    kc get namespace "$ns" -o json | python3 -c 'import sys,json;o=json.load(sys.stdin);o["spec"]["finalizers"]=[];print(json.dumps(o))' \
      | kc replace --raw "/api/v1/namespaces/$ns/finalize" -f -; done
  sleep 15
done
kc get ns | grep -E 'dask|jupyterhub|panel-viz' || echo "CLEAN SLATE — app stack removed"
```

---

## Nuclear — `zarf destroy` (only for corruption BELOW Layer B)
The engine self-heals every storage/registry wedge, so a plain re-`apply` is almost always right.
Reserve this for damage it structurally won't touch (etcd/kubelet/CNI failure, a corrupt control
plane). It removes the registry too — you'll re-init and re-push afterward.
```bash
zarf destroy --confirm        # then: re-run zarf init + converge apply (or the per-tier commands)
```

---

## S3 secret hygiene — recreating the in-cluster credentials by hand
If you must (re)create the credentials secret without a full component redeploy (values via stdin,
never argv):
```bash
kc -n panel-viz create secret generic otel-navigator-credentials \
  --from-literal=AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY" \
  --from-literal=AWS_SESSION_TOKEN="${S3_SESSION_TOKEN:-}" \
  --from-literal=S3_ENDPOINT="$S3_ENDPOINT" \
  --dry-run=client -o yaml | kc apply -f -
kc -n panel-viz rollout restart deploy/otel-navigator deploy/navigator-engine
```

---

## Conservation guardrails (do NOT cross)
- **Never** `crictl rmi` / `ctr images rm` / `crictl rmi --prune` a Layer-A image — it can't be re-fetched air-gapped.
- **Never** delete the registry hostPath data `/var/lib/zarf-registry` as routine cleanup — the
  `Retain` PV conserves the pushed images so the registry rebinds with no re-push. The **single
  exception** is §T1-H (provably corrupt registry storage): registry *content* is Layer-B and is
  rebuilt by re-pushing from the still-present package — never wipe it for any other reason.
- **Never** change the static registry PV's reclaim policy off `Retain`.
- Deleting the registry **PVC** or **PV object** (T1-C/D) is fine — the hostPath **data** survives and the registry rebinds with no re-push.
- The engine enforces all of the above structurally; by hand, you are the guard.

---

## Exit oracle
After any manual remediation, confirm with the read-only engine pass (or `git`-tracked discovery):
```bash
sudo bash ~/cybersec-converge/converge-node.sh verify     # every invariant [ok] = at target; exit 0
```
`1` = not converged (read the per-tier diagnosis); `2` = CLOSURE violation (a Layer-A artifact is
missing — re-transport it; the engine won't pull).
