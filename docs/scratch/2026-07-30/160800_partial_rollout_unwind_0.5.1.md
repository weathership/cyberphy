# Converge 0.5.1 — partial-rollout unwind when surface not Ready

## Directive

Every FSM- or zarf-started cluster change can become a **rollout wedge** if a
failure mode interrupts mid-flight. Converge stays **idempotent**, but must
still remediate **all intermediate Layer-B states** when the functional surface
is not Ready across the board — otherwise “converge apply” is not a valid
procedure from partial rollouts.

## Behavior

Each reconcile pass:

1. Discovery (API SoR)
2. `functional_surface()` — registry, dask-operator, scheduler, otel-navigator, hub
3. `sweep_vestiges()`:
   - **Always:** helm pending + DEAD + INTERRUPTED, poison labels, Terminating,
     junk pods, husks, orphan PVs, VA finalizers, stuck Dask CR deletes
   - **If surface partial:** stalled Deployments, Failed Jobs, orphan RS=0,
     partial DaskCluster notes
4. Full catalog re-detect + remediate

Clean slate (no managed ns) → `partial=False` (no deep unwind; first deploy creates state).  
Fully Ready surface → deep unwind skipped (idempotent no-op).

## Helm classification

| Class | Meaning | Unwind |
|-------|---------|--------|
| pending | latest pending-* | delete latest secret |
| dead | failed only, never deployed | delete all release secrets |
| interrupted | failed latest + prior deployed | delete latest failed only |
| ok | deployed/superseded | leave |

## Version

`__version__ = 0.5.1` — engine-only; re-bundle `cybersec-converge-*.tar.gz` for air-gap.
