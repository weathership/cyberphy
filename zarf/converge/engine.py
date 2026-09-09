"""The reconciliation engine: dependency-ordered evaluation/remediation to a
fixpoint, the closure preflight, the structural Layer-A guard, and reporting.

Two consumers of the same catalog:
  * evaluate()  — discovery + single read-only pass (powers --verify and --dry-run)
  * reconcile() — discovery + vestige sweep + multi-pass apply to a fixpoint

Every apply/verify/dry-run **always** walks K8s entry points (discovery). Apply
additionally sweeps Layer-B vestiges each pass and **re-detects every invariant**
every pass (no sticky OK that masks regressions). Layer-A remains detect-only.
"""
from __future__ import annotations

from typing import Dict, List, Tuple

import time

from .discovery import functional_surface, print_discovery, sweep_vestiges
from .kube import Ctx
from . import manual as _manual
from .model import Cost, Eval, Invariant, Layer, Outcome

MAX_PASSES = 12
PASS_DELAY = 15  # seconds between passes — let async cluster ops (pod term/sched) settle
STALL_LIMIT = 3  # consecutive no-progress passes before declaring a stall; gives a freshly
                 # rescheduled pod time to pass its readiness probe before we give up on it
TEARDOWN_SETTLE_PASSES = 6  # force-finalize retries for a namespace stuck Terminating

_SYMBOL = {
    Outcome.OK: "\033[32m[ ok ]\033[0m",
    Outcome.REMEDIATED: "\033[36m[fixed]\033[0m",
    Outcome.WOULD_FIX: "\033[33m[would]\033[0m",
    Outcome.MANUAL: "\033[35m[MANUAL]\033[0m",
    Outcome.BLOCKED: "\033[90m[blkd]\033[0m",
    Outcome.FAILED: "\033[31m[FAIL]\033[0m",
    Outcome.SKIPPED: "\033[90m[skip]\033[0m",
}


def topo_order(catalog: List[Invariant]) -> List[Invariant]:
    """Kahn topological sort over depends_on (stable on catalog order for ties)."""
    indeg = {i.id: 0 for i in catalog}
    for i in catalog:
        for d in i.depends_on:
            if d in indeg:
                indeg[i.id] += 1
    out: List[Invariant] = []
    # process in original order to keep ties deterministic
    ready = [i for i in catalog if indeg[i.id] == 0]
    seen = set()
    while ready:
        nxt = ready.pop(0)
        if nxt.id in seen:
            continue
        seen.add(nxt.id)
        out.append(nxt)
        for i in catalog:
            if nxt.id in i.depends_on and i.id not in seen:
                indeg[i.id] -= 1
                if indeg[i.id] == 0:
                    ready.append(i)
        ready.sort(key=lambda x: catalog.index(x))
    # any cycle / leftover: append in catalog order
    for i in catalog:
        if i.id not in seen:
            out.append(i)
    return out


def _deps_satisfied(inv: Invariant, results: Dict[str, Eval]) -> bool:
    return all(results.get(d) is not None and results[d].converged for d in inv.depends_on)


def evaluate(ctx: Ctx, catalog: List[Invariant], apply_preview: bool = False
             ) -> Tuple[Dict[str, Eval], List[Invariant]]:
    """Discovery (always) + one read-only catalog pass.

    ``apply_preview`` (dry-run) marks fixable Layer-B WOULD_FIX and previews the
    vestige sweep; verify is detect-only after discovery.
    """
    print_discovery(ctx, preview_sweep=apply_preview)
    order = topo_order(catalog)
    results: Dict[str, Eval] = {}
    for inv in order:
        if not _deps_satisfied(inv, results):
            results[inv.id] = Eval(inv, Outcome.BLOCKED, "dependency unmet")
            continue
        probe = inv.detect(ctx)
        if probe.ok:
            results[inv.id] = Eval(inv, Outcome.OK, probe.detail)
        elif inv.layer is Layer.A or inv.remediate is None:
            results[inv.id] = Eval(
                inv, Outcome.MANUAL,
                _manual.join_detail(probe.detail, inv.manual_hint or _manual.hint_for(inv.id)))
        elif apply_preview:
            # Preview: include the same in-situ recipe an operator would run.
            results[inv.id] = Eval(
                inv, Outcome.WOULD_FIX,
                _manual.join_detail(probe.detail, inv.manual_hint or _manual.hint_for(inv.id)))
        else:
            results[inv.id] = Eval(inv, Outcome.FAILED, probe.detail)
    return results, order


def reconcile(ctx: Ctx, catalog: List[Invariant]) -> Tuple[Dict[str, Eval], List[Invariant]]:
    """Multi-pass apply to a fixpoint or stall.

    Each pass:
      1. Full discovery (entry points → relationships → roots)
      2. Functional-surface check — if not Ready across the board, deep
         partial-rollout unwind is armed inside the vestige sweep
      3. Layer-B vestige + partial-rollout sweep (helm pending/dead/interrupted,
         husks, Terminating, stalled Deployments, Failed Jobs, …)
      4. Re-detect **every** invariant (no sticky OK — regressions re-enter remediate)
      5. Remediate broken Layer-B only; Layer-A stays MANUAL

    Expensive remediations still gated by detect() (skipped when already healthy).
    Idempotent: a fully Ready surface makes deep unwind a no-op.
    """
    order = topo_order(catalog)
    results: Dict[str, Eval] = {}
    stalls = 0
    for pass_i in range(MAX_PASSES):
        progress = False
        print(f"\n  ── reconcile pass {pass_i + 1}/{MAX_PASSES} ──")
        print_discovery(ctx, preview_sweep=False)
        surface = functional_surface(ctx)
        if surface.get("ready"):
            print("  functional surface: Ready across the board")
        elif not surface.get("partial"):
            print(f"  functional surface: pre-deploy ({'; '.join(surface.get('issues') or [])})")
        else:
            print("  functional surface: NOT ready — partial-rollout unwind armed")
            for iss in (surface.get("issues") or [])[:8]:
                print(f"    · {iss}")
        swept = sweep_vestiges(ctx, dry_run=False)
        if swept:
            progress = True
            print(f"  vestige/partial-rollout sweep ({len(swept)} action(s)):")
            for a in swept[:30]:
                print(f"    • {a}")
            if len(swept) > 30:
                print(f"    … +{len(swept) - 30} more")
        else:
            print("  vestige/partial-rollout sweep: clean")

        # Fresh results each pass so dependency edges reflect this pass's detects.
        pass_results: Dict[str, Eval] = {}
        for inv in order:
            # Always re-detect — sticky OK from an earlier pass can mask mid-run drift.
            if not _deps_satisfied(inv, pass_results):
                pass_results[inv.id] = Eval(inv, Outcome.BLOCKED, "dependency unmet")
                continue
            probe = inv.detect(ctx)
            if probe.ok:
                prev = results.get(inv.id)
                pass_results[inv.id] = Eval(inv, Outcome.OK, probe.detail)
                if prev is None or prev.outcome not in (Outcome.OK, Outcome.REMEDIATED):
                    progress = True
                continue
            if inv.layer is Layer.A or inv.remediate is None:
                pass_results[inv.id] = Eval(
                    inv, Outcome.MANUAL,
                    _manual.join_detail(
                        probe.detail, inv.manual_hint or _manual.hint_for(inv.id)))
                continue
            cost = " (expensive)" if inv.cost is Cost.EXPENSIVE else ""
            print(f"  remediating {inv.id}{cost}: {probe.detail}")
            fix = inv.remediate(ctx)
            recheck = inv.detect(ctx)
            if recheck.ok:
                pass_results[inv.id] = Eval(inv, Outcome.REMEDIATED, fix.detail)
                progress = True
            else:
                # Rem failed: keep engine detail + attach DISCOVER/FIX if not already
                # embedded (e.g. zarf deploy failures already include the recipe).
                fail_detail = _manual.join_detail(
                    f"{fix.detail}; still: {recheck.detail}",
                    inv.manual_hint or _manual.hint_for(inv.id),
                )
                pass_results[inv.id] = Eval(inv, Outcome.FAILED, fail_detail)
                if fix.changed:
                    progress = True
        results = pass_results
        if _all_settled(order, results):
            break
        stalls = 0 if progress else stalls + 1
        if stalls >= STALL_LIMIT:
            break
        time.sleep(PASS_DELAY)
    return results, order


def teardown(ctx: Ctx) -> Tuple[List[str], List[str]]:
    """Drive to the CLEAN-SLATE state — remove the disposable Layer-B app stack,
    idempotently and hands-off (the inverse of reconcile()). Neutralize Dask CR
    finalizers first so the operator can't deadlock the delete, delete the workload
    namespaces, then force-finalize any stuck Terminating. NEVER touches Layer-A node
    images or the foundational tier (zarf registry + its claimRef PV / any default
    StorageClass), so a subsequent ``--apply`` redeploys fast from the still-present
    registry. Returns
    (attempted, remaining); remaining empty ⇒ clean slate reached."""
    from .catalog import _force_finalize_ns
    from .discovery import APP_NAMESPACES, DASK_CRD_KINDS

    # 1. neutralize Dask CR finalizers (avoid an operator-gone deletion deadlock)
    for kind in DASK_CRD_KINDS:
        for it in ctx.items(kind):
            md = it.get("metadata", {})
            name, ns = md.get("name"), md.get("namespace")
            if not name:
                continue
            patch = ["patch", kind, name] + (["-n", ns] if ns else [])
            ctx.k(patch + ["--type", "merge", "-p", '{"metadata":{"finalizers":null}}'])

    # 2. delete the app-stack namespaces (the platform tier is conserved)
    attempted = [ns for ns in APP_NAMESPACES if ctx.exists("namespace", ns)]
    for ns in attempted:
        print(f"  tearing down namespace {ns}")
        ctx.k(["delete", "namespace", ns, "--wait=false"])

    # 3. Settle. Let the namespace GC cascade-delete the workloads, and force-remove
    #    any lingering pods (grace=0) so NOTHING is left running when the namespace
    #    object disappears. Force-finalizing a namespace while its pods still run
    #    ORPHANS them — the ns is removed out from under live containers, which keep
    #    running with no owning namespace. So we force-finalize the namespace itself
    #    only as a LAST resort: after pods are cleared AND it's still genuinely stuck
    #    Terminating (≥3 grace passes).
    for i in range(TEARDOWN_SETTLE_PASSES):
        stuck = [ns for ns in attempted if ctx.exists("namespace", ns)]
        if not stuck:
            break
        for ns in stuck:
            ctx.run(ctx.kubectl + ["delete", "pods", "--all", "-n", ns,
                                   "--force", "--grace-period=0", "--wait=false"])
        if i >= 2:
            for ns in stuck:
                _force_finalize_ns(ctx, ns)
        time.sleep(PASS_DELAY)

    remaining = [ns for ns in APP_NAMESPACES if ctx.exists("namespace", ns)]
    return attempted, remaining


def report_teardown(attempted: List[str], remaining: List[str]) -> bool:
    """Print the teardown summary; return True iff the clean slate was reached."""
    print("\n  CLEAN-SLATE teardown — Layer-B app stack "
          "(registry +PV / StorageClass + node images CONSERVED)")
    print("  " + "-" * 60)
    if not attempted:
        print("  \033[90mnothing to remove — app stack already absent\033[0m")
    for ns in attempted:
        ok = ns not in remaining
        mark = "\033[36m[removed]\033[0m" if ok else "\033[31m[STUCK]\033[0m"
        print(f"  {mark}  namespace/{ns}")
    print()
    if remaining:
        print(f"  \033[31m✖ {len(remaining)} namespace(s) still Terminating: "
              f"{remaining}\033[0m")
        return False
    print("  \033[32m✔ CLEAN SLATE — app stack removed; redeploy with --apply\033[0m")
    return True


def _all_settled(order: List[Invariant], results: Dict[str, Eval]) -> bool:
    """Converged or stuck on MANUAL (no further automatic progress possible)."""
    for inv in order:
        ev = results.get(inv.id)
        if ev is None:
            return False
        if ev.outcome in (Outcome.OK, Outcome.REMEDIATED, Outcome.MANUAL):
            continue
        return False
    return True


def closure_violations(results: Dict[str, Eval]) -> List[Eval]:
    """Layer-A invariants that are not satisfied — CLOSURE/CONSERVATION failures the
    operator must resolve (re-transport/re-import); the engine cannot."""
    return [ev for ev in results.values()
            if ev.inv.layer is Layer.A and ev.outcome is Outcome.MANUAL]


def _print_detail_block(detail: str, indent: str = "            ") -> None:
    """Print multi-line detail (DISCOVER/FIX recipes) with stable indentation."""
    if not detail:
        return
    for line in detail.splitlines():
        print(f"{indent}{line}" if line else indent.rstrip())


def report(results: Dict[str, Eval], order: List[Invariant]) -> bool:
    """Print the status table; return True iff fully converged (all OK/REMEDIATED)."""
    print("\n  TIER  STATUS    INVARIANT")
    print("  ----  --------  " + "-" * 56)
    converged = True
    needs_action: List[Eval] = []
    for inv in order:
        ev = results.get(inv.id)
        if ev is None:
            continue
        if ev.outcome not in (Outcome.OK, Outcome.REMEDIATED):
            converged = False
        print(f"  {inv.tier:<4}  {_SYMBOL[ev.outcome]:<8}  {inv.id}")
        if ev.outcome not in (Outcome.OK,):
            # One-line summary in the table; full LIVE STATE + recipes in IN SITU.
            first = (ev.detail or "").splitlines()[0] if ev.detail else ""
            if first:
                print(f"            {first}")
            if ev.outcome in (Outcome.MANUAL, Outcome.FAILED, Outcome.WOULD_FIX):
                needs_action.append(ev)
            # Panel stack: always expand LIVE STATE under the row during verify so
            # operators see configured bucket / secret presence / pod issues without
            # scrolling only the trailing section.
            if inv.id.startswith("T5.") and "LIVE STATE" in (ev.detail or ""):
                for line in (ev.detail or "").splitlines()[1:]:
                    if line.startswith("LIVE STATE") or line.startswith("  "):
                        print(f"            {line}")
                    elif line.strip() == "":
                        continue
                    else:
                        break

    cv = closure_violations(results)
    if cv:
        print("\n  \033[35m── CLOSURE / CONSERVATION violations (operator action required) ──\033[0m")
        for ev in cv:
            print(f"    • {ev.inv.id}")
            _print_detail_block(ev.detail, indent="      ")
        print("    These cannot be auto-fixed: a transported (Layer-A) artifact is missing or the")
        print("    node disk policy is wrong. Resolve, then re-run converge.")

    # Full in-situ DISCOVER + FIX for every non-OK Layer-B (and any Layer-A not in cv).
    action_l_b = [ev for ev in needs_action if ev.inv.layer is Layer.B
                  or ev.outcome is Outcome.FAILED]
    if action_l_b:
        print("\n  \033[35m── IN SITU (discovery + intervention; copy-paste on the node) ──\033[0m")
        print("  Session setup once:")
        for line in _manual.SETUP_PREAMBLE.strip().splitlines():
            print(f"    {line}")
        for ev in action_l_b:
            print(f"\n  • {ev.inv.id}  [{ev.outcome.value}]  {ev.inv.title}")
            _print_detail_block(ev.detail, indent="    ")

    print()
    print("  \033[32m✔ CONVERGED — deployment matches target state\033[0m" if converged
          else "  \033[33m✖ NOT CONVERGED — see above\033[0m")
    return converged
