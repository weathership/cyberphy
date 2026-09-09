"""Cyberphy Zarf (package cybersec-dask) deployment convergence engine.

A deterministic, idempotent reconciler that drives the air-gap deployment to its
target state from ANY intermediate or partial-failure state: discover -> diff
target -> remediate -> repeat to a fixpoint.

Design laws (see docs/.../runbook-airgap-update.md and the project plan):
  * CLOSURE   — every required Layer-A artifact must already be inside the closed
                world; the engine VERIFIES this and fails loud, it never fetches.
  * CONSERVATION — the engine has NO code path that deletes a transported (Layer-A)
                artifact (images, package, binaries). Layer-A invariants are
                detect-only with a manual hint; only Layer-B (the deployment) is
                ever rebuilt.
  * RECONCILIATION — everything else (order, partial failure) converges via
                dependency-ordered, idempotent passes with wall-time skipping of
                already-done expensive work.
  * PARTIAL ROLLOUT — every FSM/zarf-started intermediate artifact is a potential
                wedge. When the functional surface is not Ready across the board,
                the engine detects and unwinds partial rollouts (helm pending/
                dead/interrupted, stalled Deployments, Failed Jobs, husks, …)
                before remediating — still Layer-B only and idempotent.

Stdlib-only; independent of the `cybersec` Python package; kubectl-only at runtime
(runnable node-side, operator-side, or as an in-cluster Job).
"""

__version__ = "0.5.16"
