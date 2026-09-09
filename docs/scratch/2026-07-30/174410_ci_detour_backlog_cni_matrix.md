# CI detour backlog — hold until current matrix rerun completes

**Status: HELD** — do not land while the in-flight validation cycle is running
(avoid moving the engine/harness target mid-run).

Source: field-class findings from sandbox matrix detour (iptables/IPAM cascade +
harness observability). RKE2 tar + `iptables-nft` bootstrap already on line
(`c592221c`); engine partial-rollout + reclaim normalization already upstream.

---

## 1. Engine — CNI root-cause detection (keeper; rides next engine cut)

**Principle:** health framework = root causes, not symptoms.

Field class (not CI-only): RKE2 **tar** install or minimal OS **without iptables
userland** → portmap CNI errors after IP allocation → one IP leaked per kubelet
retry → node `/24` exhausts → every new pod fails with misleading
**"no IP addresses available"**; original cause has scrolled away.

| ID (proposed) | Detect | Rem | Layer |
|---------------|--------|-----|-------|
| **T0.cni-iptables** (name TBD) | Node-side: `command -v iptables` (or nft equivalent) **absent** | MANUAL only — hint names the package (`iptables-nft` / distro package); engine already runs node-side | B detect → MANUAL |
| **T0.cni-ipam-exhausted** (name TBD) | Pods failing with IPAM-exhaustion signature after prior CNI failure loop | Detect-only MANUAL: “leaked allocations from prior CNI failure — fix plugin (iptables), then restart canal/rke2-server to release the block.” **Never** auto-prune CNI/IPAM | B detect-only |

Do **not** auto-restart canal or rke2-server without operator intent (blast radius).

Can ride the same engine cut as reclaim normalization / post-rerun release.

---

## 2. Matrix harness — case isolation on failure (test-only)

**Problem:** Case 15 inducer arms `reclaim=Delete` and restore only happens via
successful convergence. On FAIL the landmine stays armed and can poison later
cases (e.g. case 17 T1 cascade).

**Fix:** Each inducer registers a **post-case cleanup** that always runs (PASS or
FAIL), same discipline as case 13 restoring kubectl. Cheap; prevents cross-case
confounds.

Land with case-14 backlog after rerun; no engine API change.

---

## 3. Matrix harness — stream case output (test-only)

**Problem:** `run_case` buffers each case’s converge stdout until completion →
log file sits static for long legitimate work → stall watchers false-alarm.

**Fix:** Tee converge output incrementally (per-case runner-side log file) so
liveness is observable and stall detection is meaningful.

Harness-only; land with #2 after rerun.

---

## Already baked (do not re-implement)

| Item | Where |
|------|--------|
| RKE2 `INSTALL_RKE2_METHOD=tar` + `iptables-nft` on bootstrap | sandbox `c592221c` / `main.tf` (sandbox-only; field installs from transported artifacts) |
| Partial-rollout unwind when surface not Ready | engine ≥ 0.5.1 |
| Registry PV reclaim=Retain normalization (case 15) | engine (matrix-050 → devenv) |
| Portable package paths (no site `/mnt` pollution) | engine ≥ 0.5.9 + `test_converge_portable_paths` |

---

## After current rerun is green/red

1. Implement **#1** in `zarf/converge` catalog (T0) + manual hints  
2. Implement **#2** + **#3** in `infra/aws/tofu-sandbox/test-fsm.sh`  
3. Engine cut + re-bundle only if #1 ships; harness-only otherwise  
