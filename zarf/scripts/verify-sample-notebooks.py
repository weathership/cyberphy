#!/usr/bin/env python3
"""Release gate: sample notebooks ConfigMap is complete and on-spec for JH in situ.

Checks (fail non-zero):
  1. Re-embed from zarf/notebooks/ (or --check-only against existing YAML)
  2. All required *.ipynb + generate_hdf5.py + cluster_env.py present
  3. No known anti-patterns in notebook code cells
  4. ConfigMap size under 1 MiB
  5. generate_hdf5 has ensure_parts + lab profile (HDF5 notebook contract)

Usage:
  python3 zarf/scripts/verify-sample-notebooks.py
  python3 zarf/scripts/verify-sample-notebooks.py --check-only
  python3 zarf/scripts/verify-sample-notebooks.py --embed-only

Must stay aligned with:
  zarf/scripts/embed-notebooks.py INCLUDE_NOTEBOOKS
  zarf/converge/catalog.py _SAMPLE_NOTEBOOK_KEYS / _SAMPLE_NOTEBOOK_SIDECARS
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOKS = ROOT / "zarf" / "notebooks"
CM_YAML = ROOT / "zarf" / "manifests" / "sample-notebooks-configmap.yaml"
EMBED = ROOT / "zarf" / "scripts" / "embed-notebooks.py"
GEN = ROOT / "zarf" / "scripts" / "generate_hdf5.py"
CLUSTER_ENV = ROOT / "zarf" / "notebooks" / "snippets" / "cluster_env.py"

# Keep in sync with embed-notebooks.py + converge catalog
REQUIRED_NOTEBOOKS = [
    "OTEL_Data_Generator.ipynb",
    "Dask_S3_Validation.ipynb",
    "Dask_S3_Workers_OneCell.ipynb",
    "HDF5_CPHY_Acquisition_Generator.ipynb",
    "HDF5_Iceberg_Metadata_Provider.ipynb",
]
REQUIRED_SIDECARS = ["generate_hdf5.py", "cluster_env.py"]

# Live code only (comments allowed to mention these)
BAD_LIVE = [
    (
        re.compile(r"map_partitions\s*\(\s*lambda[^)\n]*len\s*\([^)\n]*\)\s*\)\s*\.sum\s*\("),
        "map_partitions(len).sum() — use int(ddf.shape[0].compute())",
    ),
    (re.compile(r"\bminioadmin\b"), "hardcoded minioadmin (RustFS default is admin)"),
    (re.compile(r"validation-30gb"), "stale validation-30gb path"),
]


def _code_src(nb_path: Path) -> str:
    nb = json.loads(nb_path.read_text())
    parts = []
    for c in nb.get("cells") or []:
        if c.get("cell_type") == "code":
            parts.append("".join(c.get("source") or []))
    return "\n".join(parts)


def _live_issues(src: str) -> list[str]:
    issues = []
    for pat, msg in BAD_LIVE:
        for line in src.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "not map_partitions" in s or "Do NOT" in s or "never" in s.lower():
                continue
            if pat.search(line):
                issues.append(f"{msg}: {s[:90]}")
                break
    return issues


def _cm_data_keys(yaml_text: str) -> set[str]:
    """Parse top-level data keys from our generated ConfigMap YAML (no pyyaml required)."""
    keys: set[str] = set()
    in_data = False
    for line in yaml_text.splitlines():
        if line.startswith("data:"):
            in_data = True
            continue
        if not in_data:
            continue
        if line and not line.startswith(" ") and not line.startswith("\t"):
            break
        # "  Foo.ipynb: |" or "  README.md: |"
        m = re.match(r"^  ([A-Za-z0-9_.-]+):\s*\|?\s*$", line)
        if m:
            keys.add(m.group(1))
    return keys


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--check-only",
        action="store_true",
        help="Do not re-embed; only validate tree + existing ConfigMap YAML",
    )
    ap.add_argument(
        "--embed-only",
        action="store_true",
        help="Only run embed-notebooks.py",
    )
    args = ap.parse_args()
    errors: list[str] = []

    if not args.check_only:
        print("=== embed-notebooks.py ===")
        r = subprocess.run([sys.executable, str(EMBED)], cwd=str(ROOT))
        if r.returncode != 0:
            print("ERROR: embed failed", file=sys.stderr)
            return r.returncode
        if args.embed_only:
            return 0

    print("=== required sources on disk ===")
    for name in REQUIRED_NOTEBOOKS:
        p = NOTEBOOKS / name
        if not p.is_file():
            errors.append(f"missing notebook source: {p}")
        else:
            print(f"  OK {name} ({p.stat().st_size} bytes)")
            for issue in _live_issues(_code_src(p)):
                errors.append(f"{name}: {issue}")

    if not GEN.is_file():
        errors.append(f"missing {GEN}")
    else:
        gt = GEN.read_text()
        if "def ensure_parts" not in gt:
            errors.append("generate_hdf5.py missing ensure_parts()")
        if "lab" not in gt or "PROFILES" not in gt:
            errors.append("generate_hdf5.py missing PROFILES/lab")
        print(f"  OK generate_hdf5.py ensure_parts+lab")

    if not CLUSTER_ENV.is_file():
        errors.append(f"missing {CLUSTER_ENV}")
    else:
        ct = CLUSTER_ENV.read_text()
        for needle in (
            "sanitize_dask_scheduler_env",
            "list_span_parquet_keys",
            "load_active_spans_ddf",
        ):
            if needle not in ct:
                errors.append(f"cluster_env.py missing {needle}")
        print(f"  OK cluster_env.py")

    # OneCell must stay free of project imports (hand-carry)
    one = NOTEBOOKS / "Dask_S3_Workers_OneCell.ipynb"
    if one.is_file():
        src = _code_src(one)
        if "cluster_env" in src or "list_span_parquet_keys" in src:
            errors.append("Dask_S3_Workers_OneCell must not import cluster_env helpers")
        if "fs.find" not in src or "read_parquet" not in src:
            errors.append("Dask_S3_Workers_OneCell missing fs.find / read_parquet")

    # HDF5 must resolve generate_hdf5
    hdf5 = NOTEBOOKS / "HDF5_CPHY_Acquisition_Generator.ipynb"
    if hdf5.is_file():
        src = _code_src(hdf5)
        if "generate_hdf5" not in src:
            errors.append("HDF5_CPHY notebook does not reference generate_hdf5")
        if "sample-notebooks/generate_hdf5" not in src and "/root/sample-notebooks" not in src:
            errors.append("HDF5_CPHY notebook should look under sample-notebooks for generate_hdf5.py")

    print("=== ConfigMap YAML keys ===")
    if not CM_YAML.is_file():
        errors.append(f"missing {CM_YAML} — run embed")
    else:
        raw = CM_YAML.read_text()
        size = len(raw.encode("utf-8"))
        keys = _cm_data_keys(raw)
        print(f"  keys: {sorted(keys)}")
        print(f"  size: {size:,} bytes ({100*size/1_048_576:.1f}% of 1 MiB)")
        if size > 1_048_576:
            errors.append("ConfigMap exceeds 1 MiB")
        for name in REQUIRED_NOTEBOOKS + REQUIRED_SIDECARS:
            if name not in keys:
                errors.append(f"ConfigMap missing key {name}")
            else:
                print(f"  OK cm:{name}")
        # In-situ seed contract (jupyterhub-values copies these)
        for name in REQUIRED_NOTEBOOKS + REQUIRED_SIDECARS:
            if name not in keys:
                continue

    print("=== align catalog constants ===")
    catalog = (ROOT / "zarf" / "converge" / "catalog.py").read_text()
    for name in REQUIRED_NOTEBOOKS:
        if f'"{name}"' not in catalog:
            errors.append(f"converge catalog missing required notebook {name}")
    for name in REQUIRED_SIDECARS:
        if f'"{name}"' not in catalog:
            errors.append(f"converge catalog missing sidecar {name}")

    if errors:
        print("\nFAIL:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print("\nOK — sample notebooks ready to package; converge will require HDF5 in situ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
