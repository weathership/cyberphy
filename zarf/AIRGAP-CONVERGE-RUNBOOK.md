# Air-Gap Convergent Deploy — Operator Runbook (v1.6.7)

**Audience.** You have a single-node, air-gapped Kubernetes (RKE2) and need to stand up the
cybersec-dask / OTEL Navigator stack with **no internet**. The node may already carry a **partial
or stuck Zarf procedure** — a half-finished `zarf init`, a registry up but the agent missing,
pods in `ImagePullBackOff` against `ghcr.io`, a namespace wedged `Terminating`, app namespaces
silently poisoned by a prior re-init — or an **interfering storage config** (a default
StorageClass whose provisioner can't run air-gapped). The linear `zarf init && zarf package
deploy` path does not recover from these states; the convergence engine does.

**How to read this document.**
- **Part I** — the procedure. Step-by-step, assuming the automation produces the intended result
  (it is validated 8/8 against induced wedge states on real air-gapped RKE2 before every release).
- **Part II** — the manual equivalents: how to replicate, by hand, each procedure the engine
  automates — for when you want surgical control or the engine isn't available.
- **Part III** — in-situ investigation: the methods for diagnosing and remediating **unforeseen**
  error modes — the discipline and the technique catalog that found every root cause we now
  auto-heal.

Companion references: [AIRGAP-DISCOVERY.md](AIRGAP-DISCOVERY.md) (the read-only pre-flight
audit) and [AIRGAP-REMEDIATION-COMMANDS.md](AIRGAP-REMEDIATION-COMMANDS.md) (the full manual
mirror of every engine remediation, tier by tier).

---

# Part I — The procedure

## 1. What the automation is

A deterministic FSM — **detect → remediate → re-detect → fixpoint** — over a tiered invariant
catalog (node → storage → registry/agent → images → operator → scheduler → workloads → ingress).
**Idempotent** (`verify` is a clean no-op at target), **conservative** (no code path deletes a
transported Layer-A artifact — structural guard), and **resumptive** (already-good tiers detect
`[ok]`; only broken ones are remediated).

- **Layer A** — what you transport IN: deploy package, **zarf-init package**, zarf binary, engine
  bundle. Never re-fetched; the engine verifies, never pulls.
- **Layer B** — the deployment (K8s resources, registry content). Disposable; rebuilt from Layer A.

**Resilient by default:** the Zarf registry binds a claimRef hostPath PV — bare RKE2 with no
StorageClass is the baseline; an interfering default SC is defeated deterministically
(`--storage-class -`).

## 2. Prerequisites (on the node)

- An existing single-node RKE2 — `kubectl get nodes` shows it `Ready`.
- `python3` (stdlib only), root (RKE2's kubeconfig is root-only).
- The release assets transported into the closed world.

## 3. Transport

Verify integrity (`sha256sum -c SHA256SUMS`), then place each asset:

| Asset | Destination |
|-------|-------------|
| `zarf-package-cybersec-dask-amd64-1.6.7.tar.zst` | `/var/tmp/` — **keep only ONE version there** (discovery is newest-by-mtime) |
| `zarf-init-amd64-v0.70.1.tar.zst` *(Layer A — the piece partial procedures most often lack)* | `/var/tmp/` (beside the deploy package) |
| `zarf` *(v0.70.1 binary)* | `/usr/local/bin/zarf` (`chmod +x`) |
| `cybersec-converge-1.6.7.tar.gz` *(the engine)* | unpack anywhere writable |

```bash
sha256sum -c SHA256SUMS
install -m0755 zarf /usr/local/bin/zarf
mv zarf-package-cybersec-dask-amd64-1.6.7.tar.zst zarf-init-amd64-v0.70.1.tar.zst /var/tmp/
mkdir -p ~/cybersec-converge && tar xzf cybersec-converge-1.6.7.tar.gz -C ~/cybersec-converge
```

## 4. Converge — one command

S3 is **provided** in your environment. Keep secrets off the process table via a tmpfs
creds-file; the engine forwards non-secrets on `--set-variables` and secrets via a `ZARF_CONFIG`
file it manages itself (never `argv`, never bare env — see Part II §7 for why):

```bash
umask 077; cat > /dev/shm/s3-creds <<'EOF'
S3_ENDPOINT=<provided-endpoint>
S3_BUCKET=<given-bucket-name>
S3_REGION=<region>
S3_ACCESS_KEY=<key>
S3_SECRET_KEY=<secret>
EOF

sudo env CONVERGE_CREDS_FILE=/dev/shm/s3-creds bash ~/cybersec-converge/converge-node.sh apply
shred -u /dev/shm/s3-creds
```

> **`S3_BUCKET` is required** — the engine refuses to deploy the app components with a blank
> bucket (it would render `OTEL_DATA_PATH=s3:///` and brick the app) and says so.

Watch the tiers climb to `✔ CONVERGED`. If your session may drop (remote/flaky link), run it
detached and follow the log — a dead SSH must never kill a converge:

```bash
sudo setsid bash -c 'env CONVERGE_CREDS_FILE=/dev/shm/s3-creds \
  bash ~/cybersec-converge/converge-node.sh apply; echo $? > /var/tmp/converge.rc' \
  </dev/null >> /var/tmp/converge.log 2>&1 &
tail -f /var/tmp/converge.log          # reconnect + re-tail as needed; rc in /var/tmp/converge.rc
```

## 5. Verify (always — after every apply)

```bash
sudo bash ~/cybersec-converge/converge-node.sh verify     # every invariant [ok] = at target; exit 0
```

The apply loop never re-checks an invariant that passed earlier in the same run, so `verify` is
the authoritative post-state. Exit codes: `0` converged · `1` not converged (read the per-tier
diagnosis — it names the unmet condition, never a bare `rc=1`) · `2` CLOSURE violation (a Layer-A
artifact is missing; re-transport — the engine will not pull).

## 6. Upgrade an existing deployment (pre-v1.6.5 → this package)

**Audience.** The cluster already runs an older `cybersec-dask` stack (any release **before**
this package). The closed world has **no internet** and **no** laptop-side tooling — only the
assets in this release kit and `converge-node.sh`. Package and image wire names stay
**`cybersec-dask`** (no rename mid-flight).

### 6.0 REQUIRED preflight — registry lineage, conservation backstop, disk, scaling

Run these BEFORE anything else in this section. They are cheap and reversible,
and they convert the worst known upgrade failure modes from unrecoverable to
recoverable.

**(a) Registry lineage probe.** The safe upgrade floor is set by the release
that ran `zarf init` on this node (upgrades never re-init), not the package
currently deployed:

```bash
kubectl -n zarf get pvc zarf-registry -o jsonpath='{.spec.storageClassName}'; echo
```

- Output **empty** → claimRef lineage (v1.6.1+ init). Continue with §6.
- Output **non-empty** (e.g. `local-path`) → LEGACY registry on a dynamically
  provisioned PV with `reclaimPolicy: Delete` — deleting that PVC deletes every
  transported image. Do NOT run `apply` until (b) is in place, and prefer the
  clean path: `converge-node.sh teardown` → re-init under this kit → `apply`.

**(b) Conservation backstop — run unconditionally (harmless on claimRef).**

```bash
PV=$(kubectl -n zarf get pvc zarf-registry -o jsonpath='{.spec.volumeName}')
kubectl patch pv "$PV" -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
```

With `Retain`, even a mistaken registry PVC deletion leaves the image data on
disk instead of erasing it.

**(c) Disk gate (imagefs).** The upgrade adds new layers BESIDE old ones in
both the registry hostPath and containerd. Require free space ≥ 3× the deploy
package size (this cut: **≥ 4 GiB**) before apply:

```bash
df -h /var/lib/rancher /var/lib/zarf-registry
```

kubelet image GC starts at **85% imagefs usage — before any disk-pressure
taint the engine can observe** — and GC-evicted images are unrecoverable
air-gapped once the kit tarballs are gone. Free space safely if needed (never
`crictl rmi`, never prune). Keep BOTH kit tarballs staged in `/var/tmp` until
`verify` is clean — and retain them afterwards as restore media.

**(d) Preserve worker scaling.** This kit templates `DASK_WORKER_REPLICAS`
(default **4**) on every apply. If the node was scaled up (e.g. 32 sized
workers via the v1.6.5 engine), export the CURRENT values before apply or the
cluster silently resets to 4 template-sized workers:

```bash
export DASK_WORKER_REPLICAS=32 DASK_WORKER_NTHREADS=2 \
       DASK_WORKER_CPU=2 DASK_WORKER_MEMORY=28Gi
```

The v1.6.5 docs' `DASK_WORKER_MEM_REQUEST` / `DASK_WORKER_MEM_LIMIT` names are
**not** package template vars — use the names above for zarf. Engine **0.5.0+**
folds those aliases (`MEM_LIMIT`→`MEMORY`, `MEM_REQUEST`→requests.memory) and
applies **surgical** worker sizing on a live `DaskCluster` (patch CR + bounce
workers; no image re-push).

**Validation scope.** This upgrade path is procedure-reviewed and its engine
mechanism (image-tag drift → redeploy) is code-verified. Task **#51** matrix
inducers (cases **15–17** + **PARTIAL_PUSH** in `infra/aws/tofu-sandbox/test-fsm.sh`)
are landed; flip this sentence only after that subset is green on a closed-world
sandbox (`FSM_FILTER='15|16|17|PARTIAL' just sandbox-test-fsm --keep`). Until
then it is **not** yet runtime-validated. The preflight above is mandatory, not
optional.

### What the engine does on upgrade

Converge is readiness-based **and** compares the running `cybersec-dask` image tag to the
**target tag in this kit’s** `artifacts.manifest.json` (shipped inside `cybersec-converge-*.tar.gz`).
A mismatch is **image drift** → `zarf package deploy` re-pushes layers from the new deploy
package and rolls panel / Dask / Jupyter / ingress as needed. Content-derived tags (this cut:
`2025.2.0-85f3d9ecf5`) defeat the stale `IfNotPresent` trap that a fixed tag would hit on
existing nodes.

**S3 data is preserved** (markers, parquet, CPHY HDF5 under operator prefixes). The package
does not wipe the registry hostPath; new layers are added beside old ones (plan disk headroom).

### Preconditions (closed world only)

| Check | Requirement |
|-------|-------------|
| Node Ready | `kubectl get nodes` |
| Disk | enough free space for **new image layers** (~deploy package size + headroom); never `crictl rmi --prune` |
| Deploy package | **exactly one** `zarf-package-cybersec-dask-amd64-*.tar.zst` in discovery paths (`/var/tmp` …) — remove every older tarball |
| Init package | `zarf-init-amd64-v0.70.1.tar.zst` beside the deploy package (Layer A) |
| `zarf` binary | **this kit’s** v0.70.x binary — format skew with a stale CLI is unrecoverable air-gapped |
| Engine | **this kit’s** `cybersec-converge-1.6.7.tar.gz` unpacked (old engine + new package ⇒ wrong target tag) |
| S3 | same endpoint/bucket/creds the live stack already uses (`S3_BUCKET` required) |

### Procedure

```bash
# 1. Integrity + replace Layer A (do not leave multiple deploy packages)
sha256sum -c SHA256SUMS
install -m0755 zarf /usr/local/bin/zarf
zarf version    # expect v0.70.x

rm -f /var/tmp/zarf-package-cybersec-dask-amd64-*.tar.zst
mv zarf-package-cybersec-dask-amd64-1.6.7.tar.zst \
   zarf-init-amd64-v0.70.1.tar.zst /var/tmp/

# Fresh engine tree (do not mix sources with a previous unpack)
rm -rf ~/cybersec-converge
mkdir -p ~/cybersec-converge
tar xzf cybersec-converge-1.6.7.tar.gz -C ~/cybersec-converge

# 2. Same S3 as the running deployment (secrets off argv)
umask 077; cat > /dev/shm/s3-creds <<'EOF'
S3_ENDPOINT=<existing>
S3_BUCKET=<existing>
S3_REGION=<existing>
S3_ACCESS_KEY=<existing>
S3_SECRET_KEY=<existing>
EOF

# Optional: NodePort terminal path (ingress /ws needs no extra var)
#   export PTY_PROXY_WS=ws://<node>:30765

# 3. Converge — image push + component roll; detached if the session may drop
sudo setsid bash -c 'env CONVERGE_CREDS_FILE=/dev/shm/s3-creds \
  bash ~/cybersec-converge/converge-node.sh apply; echo $? > /var/tmp/converge.rc' \
  </dev/null >> /var/tmp/converge.log 2>&1 &
tail -f /var/tmp/converge.log

# 4. Authoritative post-state
sudo bash ~/cybersec-converge/converge-node.sh verify
shred -u /dev/shm/s3-creds
```

Silence during image push / helm (10–20+ min) is normal. Two flat checks with no pod movement
⇒ re-read the engine diagnosis line (it names the unmet tier), then re-run `apply` once before
hand surgery.

### Post-upgrade checks

| Check | Good |
|-------|------|
| App image | panel / dask / jupyter singleuser image tag contains the kit content tag (`2025.2.0-85f3d9ecf5`, possibly with a `-zarf-…` rewrite suffix) |
| Panel UI | ingress `panel.<domain>/otel-navigator` or NodePort `:30506` |
| Terminal WS | ingress same-origin `/ws` → HTTP 101, or NodePort `:30765` with `PTY_PROXY_WS` set at apply |
| Data path | `verify` / **T5.s3-datapath** still sees `_active_dataset.json` + readable parquet (existing data) |
| Sample notebooks | ConfigMap updated; **writable** `/root/*.ipynb` may be stale until re-seed |

**Jupyter home PVC:** startup seeds from the RO ConfigMap, but an existing user server may keep
old home copies. After singleuser is on the new image, restart the user server **or**:

```bash
cp /root/sample-notebooks/HDF5_CPHY_Acquisition_Generator.ipynb /root/
cp /root/sample-notebooks/HDF5_Iceberg_Metadata_Provider.ipynb /root/
# plus OTEL_Data_Generator / Dask_S3_Validation as needed
```

### Compatibility (pre-v1.6.5 → 1.6.7)

| Concern | Compatible? |
|---------|-------------|
| Wire name `cybersec-dask` (package / image) | Yes — no rename |
| `converge apply` only (no `just`, no internet) | Yes — intended path |
| Content image tag change | Yes — forces pull on existing nodes |
| S3 / OTEL / operator data | Yes — preserved |
| CPHY prefix `datasets/hdf5/cphy/` | Additive — does not replace older keys |
| Ingress `/ws` terminal route | Yes if **ingress** is re-applied (this package’s apply does) |
| Old engine tarball + new deploy package | **No** — always unpack **this** `cybersec-converge-1.6.7` |
| Multiple deploy packages in `/var/tmp` | **No** — remove extras (mtime discovery) |
| Stale `zarf` CLI ≠ kit v0.70.x | **No** — install kit binary first |

**Upgrade floor:** §6 as written assumes a **v1.6.1-or-later-initialized**
registry (probe §6.0a: storageClassName empty). v1.5.0 / v1.6.0-initialized
nodes: apply §6.0b, then prefer `teardown` → re-init under this kit → `apply`.

### If verify is not clean

1. Re-read the diagnosis (tier + condition).
2. Re-run `converge-node.sh apply` once (multi-pass fixpoint).
3. Common upgrade-adjacent tiers: **T0.package-uniqueness**, **T0.layer-a-zarf-tools**,
   image drift / **T2**, panel LIVE STATE / **T5**, **T5.s3-datapath**.
4. Layer-B clean slate only: `converge-node.sh teardown` then `apply` (registry + node images
   **conserved**).

Do **not** `zarf destroy` or prune images for a normal version upgrade.

## 7. First data (closed world) — the OTEL_Data_Generator notebook

The package ships **no data** (by design — datasets are provided or generated on site). The app
resolves its dataset from `s3://$S3_BUCKET/_active_dataset.json` and **fails loud** (naming the
bucket) until that marker + parquet exist. If the provided bucket is empty, seed it **from inside
the cluster** — the generator notebook is transported with the package, and the pods already
carry the S3 endpoint + credentials:

1. Open **JupyterHub** — `http://jupyter.<your-ingress-domain>/` (or the hub's NodePort).
2. Open **`OTEL_Data_Generator.ipynb`** (in the sample-notebooks the deploy mounted).
3. Run all cells — it writes partitioned OTel parquet (`…/spans/date=…/hour=…/`) **and** the
   `_active_dataset.json` marker to the given bucket, using the pod's own env (no secrets typed).
4. Prove the data path end-to-end (readiness probes cannot see it). Prefer the staged script
   (also run automatically at the end of `converge-node.sh verify|apply`, and as catalog
   invariant **T5.s3-datapath**):
   ```bash
   # Reads panel-viz ConfigMap (configured location), execs into otel-navigator (or
   # dask scheduler), checks: auth, _active_dataset.json, span parquet readable.
   # Secrets never leave the pod / never appear on argv.
   sudo bash ~/cybersec-converge/scripts/verify-s3-datapath.sh
   sudo bash ~/cybersec-converge/scripts/verify-s3-datapath.sh --json   # CI-friendly
   # Auth only (empty bucket OK while seeding):
   #   bash …/verify-s3-datapath.sh --allow-empty
   ```
   Manual one-liner equivalent (AIRGAP-DISCOVERY.md §9) still works; the script is the SSOT.
5. Load/refresh the app — it discovers the dataset from the marker; nothing is hardcoded.

## 8. Access the app + the embedded terminal (`switch` and friends)

- **The app**: `http://panel.<your-ingress-domain>/otel-navigator` (ingress) or
  `http://<node>:30506/otel-navigator` (NodePort).
- **The embedded terminal** (the deterministic REPL: `status`, `chk`, **`switch`** — the
  engine-driven toggle between the explorer and the Tap-linked latency-spectrum view, all backed
  by distributed Dask): its WebSocket must reach the pty-proxy. Two supported paths:
  - **Ingress (default)**: the bundled ingress routes **`/ws` → pty-proxy** on the panel host —
    same-origin, no configuration.
  - **NodePort / tunnel**: set the explicit URL at deploy time —
    `PTY_PROXY_WS=ws://<node>:30765` in the environment of `converge-node.sh apply` (or
    `--set-variables PTY_PROXY_WS=…` on a manual deploy). Re-running apply with it set is safe
    and rolls only the app pod.
- Quick wire-check from any workstation that can reach the endpoint (expect `101`):
  ```bash
  curl -s -i -N --max-time 5 -H "Connection: Upgrade" -H "Upgrade: websocket" \
    -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" -H "Sec-WebSocket-Version: 13" \
    http://panel.<domain>/ws | head -1
  ```
- In the terminal, `switch` toggles the visualization; both panes rasterize via datashader over
  the Dask workers — viewport interactions fan out as distributed reads.

## 9. Other modes

- `converge-node.sh dry-run` — what *would* be remediated; changes nothing.
- `converge-node.sh teardown` — clean-slate the app stack (registry + PV + node images
  **CONSERVED** → fast redeploy).
- `CONVERGE_DYNAMIC_PROVISIONING=1` — opt into the default-StorageClass modality (resourced
  multi-node cluster with a working provisioner).

## 10. What it resolves autonomously

| Symptom | The engine's action |
|---------|---------------------|
| `zarf init`: "requires a zarf-init package" | runs from the directory holding the transported init package |
| image push `500 … permission denied` | preps the registry hostPath writable (registry runs non-root; `fsGroup` doesn't chown hostPath) |
| registry PVC `Pending storageClass=local-path` (default-SC capture; provisioner dead air-gapped) | inits with **`--storage-class -`** → PVC pinned to `""` → binds the static hostPath PV; un-defaults the SC as hygiene |
| vestigial captured registry PVC (immutable class; init reuses it by name) | deletes it (clearing `pvc-protection`) so init recreates it on `""` — the Retain PV conserves pushed images |
| registry PV stranded `Released`/`Failed`/class-drifted | resets the PV object to `""` — hostPath data conserved |
| `zarf` namespace wedged `Terminating` | force-finalizes it, then re-inits |
| registry up but **agent missing** (init incomplete) | T1 requires agent-hook Ready; re-init restores it |
| **app namespaces poisoned by a re-run init** (`zarf.dev/agent=ignore` → rewriting silently OFF → later pod churn `ImagePullBackOff`s on upstream refs) | strips the label pre-init, post-init, and pre-deploy; recycles already-poisoned pods; proves the agent **behaviorally** (canary dry-run in an app namespace) |
| helm "another operation (install/upgrade/rollback) is in progress" (killed prior deploy) | deletes the stuck `pending-*` revision before every deploy and before init |
| S3 secrets never rendered (bare `ZARF_VAR_*` env doesn't template in zarf v0.70.1) | secrets ride a 0600 tmpfs `ZARF_CONFIG` file, scrubbed after each deploy |
| Dask pods with stale/empty S3 env; a deleted scheduler that never returns (operator builds children **only on CR creation**) | deletes the DaskCluster CR (clearing kopf's finalizer) → the deploy re-creates it fresh |
| app namespace stuck `Terminating`, or Active but full of **husk workloads** (force-finalize resurrection) | drains contents + strips finalizers BEFORE finalizing; heals husk namespaces (Deployments with zero pods) with a clean delete + redeploy |
| workers oversubscribed (`Pending`) | caps worker replicas to schedulable capacity; reaps orphaned excess Deployments |
| re-run on an already-good cluster | fast no-op (every tier `[ok]`) |

---

# Part II — Manual equivalents (replicating the automation by hand)

Everything the engine does is plain `kubectl` + `zarf`. The full per-invariant mirror lives in
[AIRGAP-REMEDIATION-COMMANDS.md](AIRGAP-REMEDIATION-COMMANDS.md); this section is the condensed
operating sequence. Setup used throughout (root on the control-plane node):

```bash
export KUBECONFIG=/etc/rancher/rke2/rke2.yaml
kc() { /var/lib/rancher/rke2/bin/kubectl --kubeconfig "$KUBECONFIG" "$@"; }
PKG=$(ls -t /var/tmp/zarf-package-cybersec-dask-amd64-*.tar.zst | head -1)
```

### 1. Pre-flight (read-only)
Run the discovery audit — [AIRGAP-DISCOVERY.md](AIRGAP-DISCOVERY.md) — top to bottom. At minimum:
node Ready, packages staged, the **SC/PVC capture state**, the **namespace-poison check**
(`kc get ns --show-labels | grep 'zarf.dev/agent=ignore'` on app namespaces), helm `pending-*`
releases, and the S3 endpoint/creds you were given.

### 2. Registry storage (the resilient path)
```bash
mkdir -p /var/lib/zarf-registry && chmod 0777 /var/lib/zarf-registry
kc apply -f - <<'YAML'
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
YAML
```

### 3. Pre-init unwind (A→D, all idempotent)
```bash
# A. zarf ns wedged Terminating → force-finalize (the ONLY way out)
kc get ns zarf -o json | python3 -c 'import sys,json;o=json.load(sys.stdin);o["spec"]["finalizers"]=[];print(json.dumps(o))' \
  | kc replace --raw /api/v1/namespaces/zarf/finalize -f -   # only if Terminating
# B. un-default any default StorageClass (hygiene)
kc get sc -o name | xargs -r -n1 -I{} kc patch {} -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'
# C. delete a captured registry PVC (class ≠ "" is IMMUTABLE — removal is the only fix)
kc -n zarf delete pvc zarf-docker-registry --ignore-not-found --wait=false
# D. wedged helm releases from a killed prior run
kc get secrets -A -l 'owner=helm,status in (pending-install,pending-upgrade,pending-rollback)' \
  -o custom-columns='NS:.metadata.namespace,NAME:.metadata.name'   # delete each listed secret
```

### 4. Init on explicit empty storageClass
```bash
cd /var/tmp && zarf init --confirm --storage-class -      # "-" = storageClassName:"" → binds the static PV
kc -n zarf get pods                                        # registry 1/1 AND agent-hook 2/2
```

### 5. Strip the namespace poison — after EVERY init/re-init
```bash
for ns in dask dask-operator panel-viz jupyterhub; do
  kc label namespace "$ns" zarf.dev/agent- --overwrite 2>/dev/null || true
done
# behavioral proof — the canary must come back REWRITTEN (internal 127.0.0.1:31999/... ref):
kc -n dask-operator run zz --image=ghcr.io/zarf-canary/agent-check:v1 --restart=Never \
   --dry-run=server -o jsonpath='{.spec.containers[0].image}'; echo
# recycle any pod already admitted with an upstream ref (stuck ImagePull, image NOT 127.0.0.1:*)
```

### 6. Component deploys — secrets via ZARF_CONFIG, never bare env
Bare `ZARF_VAR_*` env does **not** reach zarf v0.70.1 templating (field-proven twice — it
silently renders empty values). Non-secrets go on `--set-variables`:
```bash
export PATH="$PATH:/var/lib/rancher/rke2/bin"   # actions run bare `kubectl` (field 2026-07-15:
                                                # off root's PATH → every deploy failed on the
                                                # dask-cluster S3-bucket after-action)
set -a; . /dev/shm/s3-creds; set +a
umask 077; cat > /dev/shm/zarf-secrets.toml <<EOF
[package.deploy.set]
S3_ACCESS_KEY = "$S3_ACCESS_KEY"
S3_SECRET_KEY = "$S3_SECRET_KEY"
EOF
export ZARF_CONFIG=/dev/shm/zarf-secrets.toml
SETV=(--set-variables=S3_BUCKET="$S3_BUCKET" --set-variables=S3_ENDPOINT="$S3_ENDPOINT" --set-variables=S3_REGION="$S3_REGION")

zarf package deploy "$PKG" --confirm --components=cybersec-images  --retries 10 "${SETV[@]}"
zarf package deploy "$PKG" --confirm --components=dask-operator    --retries 10 "${SETV[@]}"
zarf package deploy "$PKG" --confirm --components=dask-cluster     --retries 10 "${SETV[@]}"
zarf package deploy "$PKG" --confirm --components=cybersec-images,panel-viz        --retries 10 "${SETV[@]}"
zarf package deploy "$PKG" --confirm --components=cybersec-images,navigator-engine --retries 10 "${SETV[@]}"
zarf package deploy "$PKG" --confirm --components=jupyterhub,sample-notebooks,ingress --retries 10 "${SETV[@]}"
shred -u /dev/shm/zarf-secrets.toml; unset ZARF_CONFIG
```
> `required: true` components (`cybersec-images`, `dask-operator`, `dask-cluster`) ride EVERY
> deploy — so pass the S3 setup to all of them, and don't be surprised by re-pushes.

**If every deploy fails with `has no deployed releases`** (field-proven 2026-07-15): one chart's
*first* install failed, leaving a release with only `failed` revisions — helm upgrade then refuses
forever, and the required-component rider spreads the failure to every deploy. The engine
auto-recovers on the next `converge --apply`; manually, remove the failing component (named in the
error as `unable to deploy component "<name>"`) and redeploy — the fresh INSTALL succeeds:
```bash
zarf package remove "$PKG" --confirm --components=<name>
zarf package deploy "$PKG" --confirm --components=<name> --retries 10 "${SETV[@]}"
```

### 7. Dask repairs are CR-level
The operator builds children only on CR **creation** — env changes never propagate; a deleted
scheduler never returns. To repair stale creds or a stranded scheduler:
```bash
kc -n dask delete daskcluster cybersec-dask --ignore-not-found --wait=false
kc -n dask get daskcluster cybersec-dask >/dev/null 2>&1 \
  && kc -n dask patch daskcluster cybersec-dask --type=merge -p '{"metadata":{"finalizers":null}}'
zarf package deploy "$PKG" --confirm --components=dask-cluster --retries 10 "${SETV[@]}"
```

### 8. Namespace teardown — NEVER force-finalize with contents
Force-finalizing removes only the namespace **object**; its contents' etcd keys survive and
**resurrect as inert husks** when the ns is recreated (Deployments with zero pods that hang every
later deploy). Drain first, finalize last:
```bash
NS=<stuck-ns>
for kind in deployments replicasets statefulsets daemonsets services configmaps secrets pvc jobs; do
  kc delete "$kind" --all -n "$NS" --wait=false 2>/dev/null
done
kc delete pods --all -n "$NS" --force --grace-period=0 --wait=false
# only when the ns is EMPTY:
kc get ns "$NS" -o json | python3 -c 'import sys,json;o=json.load(sys.stdin);o["spec"]["finalizers"]=[];print(json.dumps(o))' \
  | kc replace --raw "/api/v1/namespaces/$NS/finalize" -f -
# husk recovery (Active ns, Deployments present, zero pods): kc delete ns "$NS" (normal delete now works) + redeploy
```

---

# Part III — In-situ investigation of unforeseen error modes

Everything above exists because something once failed in a way no procedure anticipated. When you
hit a state neither the engine nor Part II covers, this is the method. It found nine root causes;
it will find the tenth.

## 3.1 The discipline

1. **Observe before you deduce.** When a hypothesis needs a third assumption to survive, stop and
   fetch the decisive object instead. (The webhook "TLS skew" theory survived two contradictions;
   one `kubectl get ns --show-labels` ended it.)
2. **Distinguish slow from stuck — check visible progress every 5–15 minutes.** "Waiting for it
   to finish" cannot tell the difference. Progress = new log lines, pod state transitions, byte
   counts — not "the process is alive". Two flat checks ⇒ investigate.
3. **Never let a link own a long operation.** Run it detached with a node-side log + rc file
   (Part I §4); a dropped session then delays your *view*, never the *work*.
4. **Change one thing, re-run the oracle.** `converge-node.sh verify` (read-only) after every
   manual intervention — it grades all tiers and its diagnosis strings name unmet conditions.
5. **Respect the conservation guardrails while experimenting**: never `crictl rmi`/prune, never
   delete `/var/lib/zarf-registry` data (except the documented corrupt-registry recovery), prefer
   deleting *objects* whose data survives (PVC/PV objects, pods, CRs) over anything bearing state.
6. **Record the before/after.** The discovery doc's one-shot snapshot (§12) before and after any
   experiment turns "I think that fixed it" into a diff.

## 3.2 Technique catalog (all field-proven)

**Admission behavior without side effects — the canary server dry-run.** A `--dry-run=server`
create traverses the real admission chain (webhooks, selectors, mutation) and persists nothing:
```bash
kc -n <app-ns> run zz --image=ghcr.io/zarf-canary/agent-check:v1 --restart=Never \
   --dry-run=server -o jsonpath='{.spec.containers[0].image}'
```
Rewritten ref ⇒ the agent mutates that namespace; unchanged ⇒ bypass (check the namespace labels
against the webhook's `namespaceSelector` before blaming the agent — zarf *deliberately* ignores
pre-init namespaces, so `default` is a permanent false negative).

**Helm archaeology — what was ACTUALLY rendered, per revision.** Release state lives in secrets
(`-l owner=helm`; labels `name`/`version`/`status`). The payload is base64(base64(gzip(JSON))) and
contains the rendered manifest — decisive when you must know whether a rewrite/variable ever made
it into an applied revision:
```bash
kc get secrets -A -l owner=helm -o custom-columns='NS:.metadata.namespace,RELEASE:.metadata.labels.name,VER:.metadata.labels.version,STATUS:.metadata.labels.status'
kc -n <ns> get secret sh.helm.release.v1.<rel>.v<N> -o jsonpath='{.data.release}' \
  | base64 -d | base64 -d | gunzip | python3 -c 'import sys,json; print(json.load(sys.stdin)["manifest"])' | grep image:
```

**Controller-intent forensics — "who should have created this, and did it try?"** A workload
that "should exist" but doesn't: check the would-be creator's view. `ReplicaSet` desired vs
current **plus its events**: `FailedCreate` events = tried and failed (quota/SA/admission); **no
events at all** = the controller isn't even trying — suspect stale/ownerless objects (see next).
Same logic one level up (Deployment→RS) and for operators (does the CR's controller only act on
*creation* events? The dask operator does — kopf handlers, not reconciliation).

**The resurrection signature — objects older than their namespace.** After any force-finalize,
compare ages: `kc -n <ns> get all -o wide` objects older than `kc get ns <ns>` creation ⇒ etcd
leftovers re-exposed by recreation. They look healthy (`deleting=False`, no finalizers) but their
controllers are dead to them. Remedy: clean delete of the ns (works now) + redeploy.

**Rendered-value verification without echoing secrets — compare lengths.** When "the creds are
set" is in doubt, walk the chain and print only lengths: HOCON/creds-file → zarf variables →
rendered CR/ConfigMap (`jsonpath` the value into `wc -c`) → pod env (`kc exec … -- sh -c 'echo
${#AWS_ACCESS_KEY_ID}'`) → runtime behavior (an in-pod `s3fs`/`boto3` probe using the pod's own
env — discovery §9). The first hop where the length drops to 0 is your culprit.

**Event archaeology.** `kc get events -A --sort-by=.lastTimestamp | tail -30` and
`kc describe pod … | sed -n '/Events:/,$p'` — image-pull targets (upstream host vs `127.0.0.1`),
admission denials, scheduling reasons. The *hostname* in a pull error tells you whether the
problem is rewriting (upstream host in a closed world) or registry content (internal host, 404/500).

**Action/script failure inside zarf deploys.** Component actions run under `set -e`: an inner
command's non-zero aborts the action even if the script "handles" it afterward. The engine
surfaces the zarf error tail; to see an action's own prints, re-run the failing deploy manually
and read the full output, or exec the action's key command yourself (they're plain shell in
`zarf.yaml`).

**Timeout vs failure.** `rc=124` from the engine = its subprocess timeout (default 1800s/zarf
call) — the operation *hung*. Hangs point at waits (helm `--wait`, zarf healthchecks) on pods
that will never arrive: go look at what the wait is waiting FOR (`kc get pods -n <target-ns>`)
rather than re-running.

**Package/variable spot-checks.** `zstd -dc pkg.tar.zst | tar -xO zarf.yaml` reads the manifest
straight from a package; `zarf tools registry catalog` lists what's actually in the internal
registry; `kc -n zarf get secret zarf-state` existence marks a completed init.

## 3.3 Escalation — when it's genuinely below Layer B

If evidence points below the deployment — etcd/kubelet/CNI failure, a corrupt control plane
(API errors unrelated to workloads, node flapping NotReady with healthy hardware) — the engine
structurally won't touch it:

```bash
zarf destroy --confirm     # removes registry too; then re-init + converge apply
# or reprovision the node, re-transport, converge apply
```

Justify it with evidence first: `systemctl status rke2-server`, `journalctl -u rke2-server -n
100`, `kc get --raw /healthz`, `dmesg | tail` (OOM/disk). "Converge didn't fix it" is not, by
itself, evidence of a below-Layer-B fault — re-read its diagnosis line; it names what's unmet.

## 3.4 Feeding it back

An unforeseen mode you solved by hand is a candidate invariant. Capture: the **detect** (what
observable state distinguishes it), the **remediate** (the idempotent command sequence), and the
**inducer** (how to plant the state on a sandbox). That triple is exactly one catalog entry +
one matrix case — the mechanism by which this runbook's Part I table has grown from three rows
to fourteen.

---

## Appendix A — root cause: the registry-PVC StorageClass capture (fixed in v1.6.1)

`zarf init` exits `rc=1`; `kubectl -n zarf get pvc` shows `zarf-docker-registry Pending
storageClass=local-path`. The init package's `docker-registry` chart **omits** `storageClassName`
when its value is empty, so Kubernetes admission stamps the cluster-default SC — RKE2
`local-path`, `WaitForFirstConsumer`, provisioner absent air-gapped → the PVC waits forever and
init rolls back. The class is **immutable** and init **reuses the PVC by name**, so retries can't
fix it. Fix: delete the captured PVC + `zarf init --storage-class -` (the chart's sentinel for
*explicit empty*) → binds the static claimRef PV, no provisioner, no default-SC race.

## Appendix B — root causes fixed in v1.6.2 (the 2026-07 audit)

*(v1.6.3 adds no engine changes — it routes the terminal WebSocket: the `/ws` ingress rule and
the `PTY_PROXY_WS` deploy variable, plus the first-data procedure in Part I §6.)*

1. **Re-init namespace poison.** `zarf init` labels every *pre-existing* namespace
   `zarf.dev/agent=ignore` (correct on first init; a re-run poisons the app namespaces). The
   agent webhook excludes them → image rewriting silently OFF → the wedge stays latent until pod
   churn, then `ImagePullBackOff` on upstream refs. **Any node where init was ever re-run should
   be assumed poisoned.**
2. **`ZARF_VAR_*` env doesn't template** (zarf v0.70.1): package variables passed as bare env
   render as empty strings — proven by a configMap (`S3_BUCKET=""`) and a DaskCluster CR (empty
   AWS creds). Deploy-time variables must ride `--set-variables` (non-secret) or a `ZARF_CONFIG`
   file (`[package.deploy.set]`, secret).
3. **kopf children are creation-only.** The dask operator neither propagates CR changes to
   existing children nor recreates deleted ones; re-applying an unchanged CR is a server-side
   no-op. All repairs are CR-level (delete + fresh create).
4. **Force-finalize resurrection.** A namespace's contents outlive the namespace object in etcd;
   recreation re-exposes them as controller-dead husks. Drain before finalizing; treat
   "Deployments with zero pods" as the husk signature.
5. **Helm pending-\* wedge.** A killed deploy leaves the latest release revision `pending-*`;
   every subsequent operation fails "another operation is in progress" until that revision's
   secret is deleted.

## Appendix C — provenance / validation

The FSM is regression-tested end-to-end against a throwaway air-gapped RKE2 node —
`just sandbox-test-fsm` provisions, **induces** each wedged state, converges, asserts recovery,
destroys. The v1.6.3 matrix is **8 cases, 8 passed**: five registry/storage permutations
(default-SC capture faithful to RKE2 `local-path`; the vestigial captured PVC — the exact
live-node wedge; Released PV; class drift; idempotent baseline) plus three audit cases with
mechanism-level post-asserts — dead agent (agent back AND canary-rewriting), wedged
`pending-upgrade` release (zero pending AND operator recovered), Terminating namespace (Active
AND pods Running). Each storage case additionally asserts the registry PVC binds on
`storageClassName=""`. The five v1.6.3 fixes were also validated individually by driving a live
quadruple-wedged specimen to `✔ CONVERGED` with the engine alone.

### Task #51 matrix extension (engine ≥ 0.5.0 — *inducers landed; runtime pending*)

Harness: `infra/aws/tofu-sandbox/test-fsm.sh` (line of record: **cyberphy `rch/devenv`**).

| Case | Inducer | Primary assert | Post-assert |
|------|---------|----------------|-------------|
| **15** legacy-registry Retain | hostPath marker + PV `reclaim=Delete` + delete ns zarf | T1 ok/fixed | reclaim=`Retain`, marker present |
| **16** registry blip | delete registry Deploy/pods; PV+hostPath stay | T1 ok/fixed | Running + marker; catalog soft |
| **17** SIGKILL mid-wait | delete scheduler + `timeout -s KILL` zarf deploy (± pending helm) | T1 ok/fixed | no pending helm; scheduler Running |
| **PARTIAL_PUSH** | wipe target tag under `/var/lib/zarf-registry` (repo remains) | **T2** images-pushed | HEAD 200 or catalog restored |

```bash
# Full matrix (13 historical + 4 task #51)
just sandbox-test-fsm --keep

# Task #51 only (node already green / --keep from prior run)
FSM_FILTER='15|16|17|PARTIAL' just sandbox-test-fsm --keep
```

When all four are green on a closed-world node, Part I § upgrade may claim the **first
runtime-validated upgrade path** in this lineage (until then: procedure-reviewed only).

## Appendix D — anticipatory platform recovery (v1.6.4)

Encoded from a live multi-day air-gap recovery session. The engine's
`_pre_init_cleanup` / `_rem_registry_running` now treat these as first-class
states (detect → remediate → re-detect), not operator folklore:

| State | Detect cue | Remediation |
|-------|------------|-------------|
| hostPath not writable | mode ≠ 0777 / push 500s / seed deadline | `mkdir` + `chmod 0777 /var/lib/zarf-registry` |
| PVC Terminating + mounts | `deletionTimestamp` + pods using claim | delete controllers/pods holding mount → strip finalizers |
| Split-brain (ns gone, PVC listed) | `namespaces "zarf" not found` on patch | recreate ns → re-home husks → drain → delete PVC |
| Active husk (`zarf-injector` 34d) | services without Ready registry | full ns drain (svc/cm/secret/pvc) then re-init |
| Seed-registry Helm deadline | init rc=1/124 mid docker-registry | unwedge pending helm + clear partial workloads; retry init |
| Default SC capture | PVC `sc=local-path` Pending | delete PVC; `zarf init --storage-class -` |
| No StorageClass at all | `kc get sc` empty | **supported** resilient path — not an error |
| Layer-A tools missing | `zarf` rc=127 / no init tarball | T0.layer-a-zarf-tools MANUAL (transport) |
| hub-db local-path orphan | hub PVC Pending / Released hub-db-pv | T5: delete orphan PVC/PV + redeploy jupyterhub |

**Generalized principles (CADS):** conservation of Layer A; empty-class static PV
as the resilient attractor; drain-before-finalize; recreate-ns-before-namespaced-write;
diagnose strings that name the unmet condition.

Matrix: `just sandbox-test-fsm` — includes husk Service, PVC/ns split-brain, and
hostPath permission inducers in addition to the v1.6.2/1.6.3 eight.

## Appendix E — always-on discovery + vestige sweep (v0.3.0 / package 1.6.4+)

Every `converge-node.sh` mode runs **multi-layer discovery** before catalog work:

```
nodes → managed namespaces → controllers/pods → PVC→PV→SC chains →
helm pending secrets → VolumeAttachments → Dask CR finalizers
```

**Apply** additionally, **each reconcile pass**:

1. Print discovery (entry → relationship → root condition)
2. **Functional surface** check (registry, operator, scheduler, otel-navigator, hub)
3. **Vestige + partial-rollout sweep** (Layer-B only):
   - **Always:** pending / DEAD / INTERRUPTED helm secrets, agent poison labels,
     Terminating ns/PVCs, junk pods, husks, orphan app PVs, stuck VolumeAttachments,
     stuck Dask CR finalizers
   - **When surface not Ready across the board:** stalled Deployments
     (ProgressDeadlineExceeded / long unavailable), Failed Jobs, orphan zero
     ReplicaSets, partial DaskCluster (no scheduler child) notes — so every
     FSM/zarf intermediate is unwound before the next remediate
4. Re-detect **every** invariant (no sticky OK)
5. Remediate broken Layer-B tiers

Idempotent: a fully Ready surface makes deep partial-rollout unwind a no-op.

**Never disposed (Layer-A / foundational):** containerd images, `/var/lib/zarf-registry`
data, zarf binary + init/deploy packages on disk, RKE2 system namespaces. A Bound
registry PVC with Ready registry is left intact during sweep.

## Appendix F — anticipatory platform edges (engine v0.4.0)

| Edge | Invariant / behavior | Conservation |
|------|----------------------|--------------|
| Kubelet image-GC @85% | `T0.kubelet-gc` writes lenient `kubelet-arg` + restarts `rke2-server` (root) | Raises thresholds only; never prunes images |
| RKE2 system plane | `T0.system-plane` observes API/DNS/ingress; uncordons only | Never restarts etcd; coredns ImagePull → MANUAL Layer-A |
| Package mtime races | `T0.package-uniqueness` fails if multiple deploy tarballs | Never deletes packages (Layer-A MANUAL) |
| Ingress class (traefik on RKE2) | Auto-detect `nginx`; stamp on every deploy; T6 checks class + `/ws` | Redeploy ingress only |
| Hub SC-less | Package `sqlite-memory` + `storage: none`; T5 deletes any hub PVC/hub-db PV then redeploy | Hub DB ephemeral by design air-gap |
| Worker default | Deploy path forces `DASK_WORKER_REPLICAS=4` (+ nthreads/cpu/memory defaults) if unset | Engine 0.5.0 capacity-caps by RAM headroom; surgical CR sizing |

Discovery always prints SYSTEM/RKE2 PLANE, LAYER-A PACKAGES, and KUBELET GC POLICY lines.
