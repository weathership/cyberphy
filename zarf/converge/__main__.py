"""CLI for the convergence engine.

    python3 -m converge --verify                 # discovery + target oracle (no changes)
    python3 -m converge --dry-run                # discovery + vestige preview + would-fix
    python3 -m converge --apply                  # discovery + sweep husks + remediate to fixpoint
    python3 -m converge --apply --package zarf-package-...tar.zst --set S3_BUCKET=...

Every mode walks K8s entry points first (nodes → ns → controllers → pods → PVC/PV →
helm → CRs). Apply re-detects all invariants each pass and never deletes Layer-A.

Exit: 0 = converged / clean dry-run; 1 = not converged; 2 = CLOSURE violation
(a transported Layer-A artifact is missing — operator must re-import, engine won't).
"""
from __future__ import annotations

import argparse
import shlex
import shutil
import sys
from pathlib import Path

from . import __version__
from .catalog import build_catalog
from .engine import closure_violations, evaluate, reconcile, report, report_teardown, teardown
from .kube import Ctx, DEFAULT_MANIFESTS_DIR, load_manifest


def _default_zarf() -> str | None:
    for cand in ("zarf", "/usr/local/bin/zarf", "/var/lib/rancher/rke2/bin/zarf"):
        if shutil.which(cand) or Path(cand).exists():
            return cand
    return None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="converge", description="cyberphy (cybersec-dask package) deployment convergence engine")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="remediate to a fixpoint")
    mode.add_argument("--verify", action="store_true", help="assert target state (oracle); no changes")
    mode.add_argument("--dry-run", action="store_true", help="show what would be remediated (default)")
    mode.add_argument("--teardown", action="store_true",
                      help="clean-slate: remove the Layer-B app stack (registry/SC + images CONSERVED)")

    ap.add_argument("--kubectl", default="zarf tools kubectl",
                    help='base kubectl command (default: "zarf tools kubectl")')
    ap.add_argument("--kubeconfig", help="append --kubeconfig <path> to kubectl")
    ap.add_argument("--manifest", help="path to artifacts.manifest.json")
    ap.add_argument("--manifests-dir", default=str(DEFAULT_MANIFESTS_DIR),
                    help="dir with bundled k8s manifests (local-path-provisioner.yaml) for "
                         "registry-free StorageClass bootstrap")
    ap.add_argument("--zarf", default=_default_zarf(), help="path to the zarf binary")
    ap.add_argument("--package", help="path to the deploy .tar.zst (for component remediations)")
    ap.add_argument("--registry-pvc-size", default="5Gi")
    ap.add_argument("--no-registry-pvc", action="store_true",
                    help="run the internal registry on emptyDir (no PV — lost on pod restart)")
    ap.add_argument("--enable-dynamic-provisioning", action="store_true",
                    help="OPT-IN: restore the default-StorageClass tier (the node-preloaded "
                         "local-path-provisioner) for a resourced multi-node cluster running "
                         "workloads that need dynamic PVCs. DEFAULT is the resilient path — the "
                         "registry binds a claimRef hostPath PV, so no StorageClass is needed.")
    ap.add_argument("--set", action="append", default=[], metavar="KEY=VAL",
                    help="S3/zarf variable to pass to component deploys (repeatable)")
    ap.add_argument("--creds-file", help="file of KEY=VALUE lines (e.g. S3_SECRET_KEY=...) "
                    "merged into the deploy variables — keeps secrets off argv/ps")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--version", action="version", version=f"converge {__version__}")
    args = ap.parse_args(argv)

    kube = shlex.split(args.kubectl)
    if args.kubeconfig:
        kube += ["--kubeconfig", args.kubeconfig]

    s3 = {}
    if args.creds_file:  # loaded first so explicit --set can override
        try:
            for line in Path(args.creds_file).read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    # Operators write these files shell-style; tolerate quoted
                    # values (field 2026-07-29: S3_BUCKET='dhfo' became a
                    # literal-quoted bucket name and a false FAIL drift).
                    v = v.strip()
                    if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
                        v = v[1:-1]
                    s3[k.strip()] = v
        except OSError as e:
            print(f"FATAL: --creds-file unreadable: {e}", file=sys.stderr)
            return 2
    for kv in args.set:
        if "=" in kv:
            k, v = kv.split("=", 1)
            s3[k] = v

    ctx = Ctx(
        kubectl=kube,
        apply=args.apply,
        manifest=load_manifest(args.manifest),
        zarf_bin=args.zarf,
        package_path=args.package,
        manifests_dir=args.manifests_dir,
        registry_pv_size=args.registry_pvc_size,
        registry_pvc_enabled=not args.no_registry_pvc,
        dynamic_provisioning=args.enable_dynamic_provisioning,
        s3=s3,
        verbose=args.verbose,
    )

    # connectivity gate
    probe = ctx.k(["version", "--client=false", "-o", "json"])
    if probe.returncode != 0 and ctx.k(["get", "--raw", "/healthz"]).returncode != 0:
        print(f"FATAL: cannot reach the cluster via `{args.kubectl}`.\n  {probe.stderr.strip()}",
              file=sys.stderr)
        return 2

    mode_name = ("apply" if args.apply else "verify" if args.verify
                 else "teardown" if args.teardown else "dry-run")
    storage = ("dynamic-SC" if ctx.dynamic_provisioning
               else "claimRef-PV" if ctx.registry_pvc_enabled else "emptyDir")
    cap = ctx.node_capacity()
    print(f"converge {__version__}  |  mode={mode_name}  |  storage={storage}  |  "
          f"nodes={cap['ready_nodes']} mem={cap['total_mem_gib']}Gi")

    if args.teardown:
        attempted, remaining = teardown(ctx)
        return 0 if report_teardown(attempted, remaining) else 1

    catalog = build_catalog(dynamic_provisioning=ctx.dynamic_provisioning,
                            registry_pvc_enabled=ctx.registry_pvc_enabled)
    if args.apply:
        results, order = reconcile(ctx, catalog)
    else:
        results, order = evaluate(ctx, catalog, apply_preview=not args.verify)

    converged = report(results, order)

    if closure_violations(results):
        return 2
    return 0 if (converged or (not args.apply and not args.verify)) else 1


if __name__ == "__main__":
    sys.exit(main())
