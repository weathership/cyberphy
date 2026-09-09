# Full FSM matrix on zarf-v1.6.7 (engine 0.5.16) — run record 2026-07-31

**Artifacts under test:** deploy package `2361e30e…` (post image-closure hotfix,
tag `736e1181`), engine 0.5.16, fresh m7i.2xlarge single-node RKE2 sandbox
(tar-method + iptables-nft bootstrap), egress cut before cases.

## Result: 12 passed / 5 failed

| Case | Verdict | Note |
|------|---------|------|
| 01 baseline (fresh install of 1.6.7 pkg) | ✓ | first runtime proof of the closure-hotfixed package |
| 02–05 storage tier (SC capture, vestigial PVC, Released PV, class drift) | ✓ | |
| 06 dead zarf agent | ✓ | canary soft |
| 07 wedged pending-upgrade helm | ✗ | inducer aborted (see harness bug 1) AND operator 0 Running at assert |
| 08 app ns Terminating | ✗ | panel-viz absent post-converge (starvation pattern) |
| 09 zarf husk Service | ✓ | |
| 10 PVC Terminating + split-brain | ✓ | |
| 11 hostPath perms | ✓ | |
| 12 dead helm release | ✗ | recovery deployed 11 releases but scheduler 0 Running at assert |
| 13 kubectl off PATH | ✗ | kubectl correctly hidden; scheduler 0 Running at assert |
| **15 legacy-registry Retain guard** | **✓** | induced reclaim=Delete detected ("want Retain — conservation"), normalized, marker conserved — the 2026-07-29 audit finding is now runtime-validated |
| **16 registry blip** | **✓ (soft)** | registry Running, marker conserved; post-assert catalog read 0 (probe bug, below) |
| **17 SIGKILL mid-wait (converge-09)** | **✓** | real `timeout -s KILL` on live deploy; 0.5.16 streamed/budgeted rems healed to scheduler Running, no pending helm |
| PARTIAL_PUSH | ✗ | assert failed on the same catalog probe bug + `HEAD=no-tag` extraction |

**The three critical cases (15/16/17) are green** — the §6 upgrade path of
v1.6.7 ships with runtime collateral for its claims, first in the lineage.

## Cross-cutting failure analysis (two reader issues, NOT one)

Every case logged `registry catalog missing cybersec-dask — push images first`
and paid a ~10 min re-push, starving workload-tier rebuilds (07/08/12/13)
within their per-case apply budgets. Post-run forensics on the kept node:

1. **Harness probes (confirmed, fixed in this commit):** the post-assert
   probes ran bare `sudo zarf tools registry catalog` — no KUBECONFIG under
   sudo → `unable to connect to the cluster` → parsed as 0 repos. Direct v2
   `/_catalog` is 401 (registry auth — correct). Fixed by
   `sudo env KUBECONFIG=… zarf tools registry catalog`.
2. **Engine feasibility reads (OPEN — needs reproduction):** the engine's
   `_registry_census` is already unknown-safe (`catalog_ok=None` on rc≠0;
   consumers check `is False`), so its "catalog missing" verdicts mean the
   read returned **rc=0 with cybersec-dask genuinely absent** in engine
   context (KUBECONFIG present). Candidate causes to distinguish on a repro
   node — NOT patched blind:
   - registry pod serving a different root than the conserved hostPath after
     the re-init/rebind sequence (emptyDir fallback window? rootDirectory?)
   - zarf state/htpasswd skew after ns-zarf drain + re-init (creds rotate,
     old registry container)
   - catalog output shape/pagination under the tunnel
   If confirmed as a real empty catalog, this is a CONSERVATION-visibility
   gap (data on disk, registry not serving it) — higher priority than a
   probe bug.

Also fixed in this commit: **case-07 inducer targeted ns `dask-operator`, but
zarf stores component helm release secrets HASHED in ns `zarf`** — the
assert `no helm release found` aborted the induction, meaning the
pending-upgrade *upgrade-path* variant had never actually been exercised by
any prior matrix. Inducer now searches all namespaces (minus kube-system)
and plants the pending revision in the release's own namespace.

## Follow-ups (task #53 equivalents)

- Reproduce + root-cause engine-context empty-catalog (item 2 above); then
  re-run `FSM_FILTER='07|08|12|13|PARTIAL'`.
- Consider routing the older workload rems (jupyterhub/dask redeploys)
  through the 0.5.13 streamed/budgeted deploy path — case 17 succeeded where
  07/08/12/13 starved, and that path difference is the strongest signal.
- PARTIAL_PUSH post-assert tag extraction (`HEAD=no-tag`).
- Case isolation: inducers restore induced state on FAIL (case 15's
  reclaim=Delete landmine had to be defused by the engine in a later case).
- Stream per-case converge output (runner buffers until case end — every
  stall alarm this week was this).
