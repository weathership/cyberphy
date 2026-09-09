# Task #51 matrix — carry-forward (sandbox machine)

**Status:** Inducers + post-asserts **landed** on cyberphy `rch/devenv`.  
**Not yet:** live green on a throwaway air-gap RKE2 (needs sandbox node access).

Engine line of record: **weathership/cyberphy `rch/devenv`** (not `rch/laptop` legacy).

## What was added

File: `infra/aws/tofu-sandbox/test-fsm.sh`

| Case | Name | Inducer | Gate |
|------|------|---------|------|
| 15 | legacy-registry upgrade (Retain guard) | `induce_legacy_registry_retain` | T1 + reclaim=Retain + hostPath marker |
| 16 | registry blip mid-upgrade | `induce_registry_blip` | T1 + Running + marker |
| 17 | SIGKILL mid-wait (converge-09) | `induce_sigkill_mid_wait` | T1 + no pending helm + scheduler |
| — | PARTIAL_PUSH | `induce_partial_push` | **T2** images-pushed + HEAD/catalog |

Also:
- `FSM_FILTER` — run a subset without re-running 01–13
- Case titles numbered `01`…`13`, `15`…`17` for greppable logs
- Runbook Appendix C documents the extension as *runtime pending*

## How to run on the sandbox machine

```bash
# 0) on cyberphy rch/devenv, current engine staged via transport
git fetch && git checkout rch/devenv && git pull

# 1) hydrate + full matrix (destroys on success unless --keep)
just sandbox-test-fsm --keep

# 2) or, if node already up/air-gapped/engine staged from a prior --keep:
FSM_FILTER='15|16|17|PARTIAL' just sandbox-test-fsm --keep

# 3) re-stage engine only (after pull) without full reprovision
bash infra/aws/tofu-sandbox/run-sandbox.sh transport   # while egress still available if needed
# engine is under ~/cybersec-converge on the node
```

### Interpreting results

- Log lines: `═══════════ CASE: 15 …` then `✓ recovered` / `✗`
- Summary: `FSM validation: N passed, M failed`
- On failure the node is **kept** for inspection (`just sandbox-destroy` when done)
- PARTIAL_PUSH needs a prior successful image push (baseline/T2) so the catalog has
  `cybersec-dask` — full matrix order is correct; filter-only on a virgin node may soft-pass

### After green

1. Paste the summary table into this note or a release PR
2. Flip runbook Part I § upgrade from “not runtime-validated” → first validated path
3. Cut the release that ships that claim (package + engine tarball)

## Related commits (already on rch/devenv)

| Commit | Role |
|--------|------|
| `8154617d` | Retain conservation / §6.0 preflight |
| `eb888bc5` | Interrupted-procedure accounting (converge-09) |
| `138b389a` | Registry v2 manifest HEAD / PARTIAL_PUSH SoR |
| `d5e91d7b` | Engine 0.5.0 surgical worker sizing |
| *(this)* | Matrix inducers 15–17 + PARTIAL_PUSH |
