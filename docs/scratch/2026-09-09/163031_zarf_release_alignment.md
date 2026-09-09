# Zarf release v1.6.7 vs local state — alignment audit

Date: 2026-09-09

## Question
Do we have any Dask/Jupyter stack work locally that isn't in the latest published Zarf release?

## Answer
No — the drift runs the **other way**. All Dask/Jupyter work is in the release; the
release branch holds 53 commits of Dask/Jupyter/converge work **not merged back to trunk**.

## Topology
- Latest published release: **zarf-v1.6.7** (GitHub release, 2026-07-30), assets complete:
  `zarf-package-cybersec-dask-amd64-1.6.7.tar.zst` (1.34 GB), `cybersec-converge-1.6.7.tar.gz`
  (engine 0.5.16), zarf binary + init package, runbooks, SHA256SUMS. Tag's `zarf.yaml`
  says `version: "1.6.7"` (trunk's still says 1.6.4).
- The tag is **not an ancestor of trunk**. It lives on `origin/rch/devenv`.
  Merge-base with trunk: 242f0244 (mdbook docs commit).
- `zarf-v1.6.7..HEAD` (release→trunk): 53 commits missing from trunk — converge engine
  0.4.x→0.5.16, notebook rewrites (Dask_S3_Validation O(files) discovery wall,
  OTEL generator alignment, prove-workers cells, require_parquet_stack), sample-notebooks
  configmap, verify-s3-datapath, field remediations. Net −12.5k lines on trunk's side of
  `zarf/`.
- Trunk since merge-base: 6 commits, **zero** touch `zarf/` or Dask/Jupyter
  (PyFlink vendoring, submodule remotes, docs, nautilus submodule, devenv.lock).
  Trunk is 6 ahead of origin/trunk (unpushed at audit time).
- Post-release: `origin/rch/devenv` is 1 commit past the tag — c2241f7f
  (FSM case-07 inducer fix + v1.6.7 matrix run record 12/17). Test/record only,
  not deployable stack content, and not in the published release.
- No stashes, no other local branches, clean working tree.

## Recommended follow-up
Merge `origin/rch/devenv` into trunk (same pattern as 39f7d121 "merge origin/rch/laptop")
so the release-grade Dask/Jupyter + converge work lands on the main line, then push trunk.

## Outcome (same day)
Merged `origin/rch/devenv` into trunk as 34c035eb. Conflict resolutions:
- **AGENTS.md**: rebuilt from trunk's single-agent-file version + branch deltas
  (cyberphy CLI preferred, RustFS console/creds, DHFO_* in portable-paths,
  extra converge table row, **Held backlog** section). CLAUDE.md stays deleted.
- **README.md**: trunk's HTTPS-gitlinks line + branch's cybersec/cyberphy line.
- **devenv.lock**: branch side (keeps `rustfs` input that devenv.yaml declares).
- **tests/test_converge_portable_paths.py**: trunk side (AGENTS.md reference,
  no unused pytest import).
Follow-up c64a02dc: nautilus submodule remote switched to HTTPS to satisfy
`test_portable_sources` (use local `url.insteadOf` for SSH auth if needed).

Tests: 140 passed with `PYTHONPATH=zarf` (converge tests import `converge.*`
from `zarf/converge/`). Pre-existing failures, untouched by merge: 5 rete tests
(venv ortools/libprotobuf undefined symbol) and 2 test_transforms tests
(`np.True_ is True`). `packages/hdf5_iceberg` is standalone (own pyproject),
not collected from the root env. Trunk now 9 ahead of origin/trunk — unpushed.
