#!/usr/bin/env python3
"""Embed Jupyter notebooks into a Kubernetes ConfigMap YAML.

Reads .ipynb files from zarf/notebooks/, strips output cells,
and generates zarf/manifests/sample-notebooks-configmap.yaml
with notebook JSON embedded as ConfigMap data entries.

Usage:
    python zarf/scripts/embed-notebooks.py

The ConfigMap YAML is auto-generated — do not edit it by hand.
Edit the source notebooks in zarf/notebooks/ instead.
"""
import json
import sys
import textwrap
from pathlib import Path

# Paths relative to project root
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
NOTEBOOKS_DIR = PROJECT_ROOT / "zarf" / "notebooks"
GENERATOR_SCRIPT = PROJECT_ROOT / "zarf" / "scripts" / "generate_hdf5.py"
# Prefer scripts/generate_hdf5.py; fall back to notebooks tree / image copy
if not GENERATOR_SCRIPT.is_file():
    GENERATOR_SCRIPT = PROJECT_ROOT / "zarf" / "images" / "sample-notebooks" / "generate_hdf5.py"
CLUSTER_ENV_SCRIPT = PROJECT_ROOT / "zarf" / "notebooks" / "snippets" / "cluster_env.py"
OUTPUT_FILE = PROJECT_ROOT / "zarf" / "manifests" / "sample-notebooks-configmap.yaml"

# Notebooks to include (order matters for README table)
INCLUDE_NOTEBOOKS = [
    "OTEL_Data_Generator.ipynb",
    "Dask_S3_Validation.ipynb",
    "Dask_S3_Workers_OneCell.ipynb",
    "HDF5_CPHY_Acquisition_Generator.ipynb",
    "HDF5_Iceberg_Metadata_Provider.ipynb",
]

# ConfigMap limit is 1 MiB; warn if we get close
MAX_CONFIGMAP_BYTES = 1_048_576
WARN_THRESHOLD = 0.8  # 80%


def strip_notebook(nb: dict) -> dict:
    """Strip outputs and execution counts from a notebook."""
    nb = json.loads(json.dumps(nb))  # deep copy
    for cell in nb.get("cells", []):
        if cell.get("cell_type") == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
        # Remove cell IDs (non-essential, saves space)
        cell.pop("id", None)
    return nb


def yaml_block_scalar(text: str, indent: int = 4) -> str:
    """Format a string as a YAML block scalar (literal style |)."""
    prefix = " " * indent
    lines = text.split("\n")
    return "\n".join(prefix + line if line else "" for line in lines)


def main():
    if not NOTEBOOKS_DIR.is_dir():
        print(f"Error: notebooks directory not found: {NOTEBOOKS_DIR}", file=sys.stderr)
        sys.exit(1)

    # Process notebooks
    notebook_entries = {}
    for nb_name in INCLUDE_NOTEBOOKS:
        nb_path = NOTEBOOKS_DIR / nb_name
        if not nb_path.exists():
            print(f"Warning: {nb_name} not found, skipping", file=sys.stderr)
            continue
        with open(nb_path) as f:
            nb = json.load(f)
        stripped = strip_notebook(nb)
        # Compact JSON (no indent) to save space in ConfigMap
        nb_json = json.dumps(stripped, separators=(",", ":"), ensure_ascii=False)
        notebook_entries[nb_name] = nb_json
        print(f"  {nb_name}: {len(nb_json):,} bytes")

    # Build README content
    readme = textwrap.dedent("""\
        # Sample Notebooks

        Seeded **in situ** by JupyterHub: ConfigMap mount at ``/root/sample-notebooks/``
        (RO), copied to writable ``/root/*`` on singleuser start. Converge task
        ``T5.sample-notebooks`` requires all notebooks below + ``generate_hdf5.py``
        + ``cluster_env.py`` so HDF5 samples work after package deploy.

        ## Available Notebooks

        | Notebook | Description |
        |----------|-------------|
        | `OTEL_Data_Generator.ipynb` | Synthetic OTEL spans → ``s3://$BUCKET/$PREFIX/spans/date=…/`` |
        | `Dask_S3_Validation.ipynb` | Explicit LIST + distributed parquet (O(files) wall notes) |
        | `Dask_S3_Workers_OneCell.ipynb` | Minimal hand-carry cell (s3fs+dask only, no project imports) |
        | `HDF5_CPHY_Acquisition_Generator.ipynb` | CPHY HDF5 + Dask (idempotent; needs ``generate_hdf5.py``) |
        | `HDF5_Iceberg_Metadata_Provider.ipynb` | ``hdf5_iceberg`` SDK metadata plane |

        Sidecars on the same ConfigMap (also copied to ``/root``):

        - ``generate_hdf5.py`` — imported by the CPHY HDF5 notebook
        - ``cluster_env.py`` — S3/Dask config from JupyterHub env (no hard-coded secrets)

        ## Getting Started

        JupyterLab home is ``/root`` (**writable**). Open top-level copies e.g.
        ``/root/HDF5_CPHY_Acquisition_Generator.ipynb`` — not files inside
        ``sample-notebooks/`` (RO ConfigMap; Duplicate → Errno 30).

        After converge/package updates the CM: **Stop My Server → Start My Server**
        so startup re-seeds ``/root/*.ipynb`` and sidecars.

        HDF5 CPHY is **idempotent by default** (reuses S3 parts). Set
        ``FORCE_REGENERATE = True`` / ``HDF5_FORCE_REGENERATE=1`` only for a full rewrite.

        ## Environment Variables (injected — no notebook edits)

        From Zarf vars that **converge** passes (``--creds-file`` / ``S3_*``).
        Local lab object store is **RustFS** (default keys ``admin``/``admin``).

        - ``DASK_SCHEDULER_ADDRESS`` — ``tcp://…:8786`` (**do not** set ``DASK_SCHEDULER=tcp://…``;
          dask treats that env as scheduler *type* and breaks ``dd.read_parquet`` planning)
        - ``S3_ENDPOINT``, ``S3_BUCKET``, ``AWS_REGION`` / ``S3_REGION``
        - ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` / ``AWS_SESSION_TOKEN``
        - ``OTEL_DATA_PATH`` / ``OTEL_PREFIX`` — parquet under ``…/spans/``
        - ``HDF5_PROFILE`` / ``HDF5_FORCE_REGENERATE`` / ``USE_DASK``

        **Large data:** Dask on workers only. Do **not** ``pyarrow.dataset.to_table()``
        multi‑GB sets in the kernel. Row counts: ``int(ddf.shape[0].compute())``,
        never ``map_partitions(len).sum()`` (dask-expr).

        ## Cluster Resources

        Default package: ``DASK_WORKER_REPLICAS=4`` × 2 threads × 6 GiB.
        Air-gap 2 TiB HDF5: raise workers (8–32). Prefer the DaskCluster CR:

        ```bash
        kubectl -n dask patch daskcluster cybersec-dask --type merge \
          -p '{"spec":{"worker":{"replicas":8}}}'
        ```

        ## Rebuild / package

        ```bash
        python3 zarf/scripts/embed-notebooks.py   # regenerates ConfigMap YAML
        python3 zarf/scripts/verify-sample-notebooks.py
        # then: zarf package create (ops.sh package / devenv package task)
        ```
    """)

    # Build ConfigMap YAML
    yaml_parts = [
        "# Auto-generated by zarf/scripts/embed-notebooks.py",
        "# DO NOT EDIT — regenerate with: python zarf/scripts/embed-notebooks.py",
        "#",
        "# Notebooks embedded from zarf/notebooks/",
        "# Mounted read-only at ~/sample-notebooks/ ($HOME/sample-notebooks) in JupyterHub",
        "---",
        "apiVersion: v1",
        "kind: ConfigMap",
        "metadata:",
        "  name: sample-notebooks",
        "  namespace: jupyterhub",
        "  labels:",
        "    app: jupyterhub",
        "    component: sample-notebooks",
        "data:",
        "  README.md: |",
    ]
    yaml_parts.append(yaml_block_scalar(readme))

    for nb_name, nb_json in notebook_entries.items():
        yaml_parts.append(f"  {nb_name}: |")
        yaml_parts.append(yaml_block_scalar(nb_json))

    # Ship generate_hdf5.py + cluster_env.py (ConfigMap mount → /root/sample-notebooks/)
    if GENERATOR_SCRIPT.is_file():
        gen_text = GENERATOR_SCRIPT.read_text()
        yaml_parts.append("  generate_hdf5.py: |")
        yaml_parts.append(yaml_block_scalar(gen_text))
        print(f"  generate_hdf5.py: {len(gen_text):,} bytes")
    else:
        print(f"Warning: {GENERATOR_SCRIPT} not found — CPHY notebook import may fail", file=sys.stderr)

    if CLUSTER_ENV_SCRIPT.is_file():
        ce_text = CLUSTER_ENV_SCRIPT.read_text()
        yaml_parts.append("  cluster_env.py: |")
        yaml_parts.append(yaml_block_scalar(ce_text))
        print(f"  cluster_env.py: {len(ce_text):,} bytes")
    else:
        print(f"Warning: {CLUSTER_ENV_SCRIPT} not found — notebooks use env fallback", file=sys.stderr)

    yaml_content = "\n".join(yaml_parts) + "\n"

    # Size check
    size_bytes = len(yaml_content.encode("utf-8"))
    pct = size_bytes / MAX_CONFIGMAP_BYTES * 100
    print(f"\nConfigMap size: {size_bytes:,} bytes ({pct:.1f}% of 1 MiB limit)")
    if size_bytes > MAX_CONFIGMAP_BYTES:
        print("ERROR: ConfigMap exceeds 1 MiB limit!", file=sys.stderr)
        sys.exit(1)
    if pct > WARN_THRESHOLD * 100:
        print(f"WARNING: ConfigMap is {pct:.0f}% of limit", file=sys.stderr)

    # Write output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        f.write(yaml_content)

    print(f"Wrote {OUTPUT_FILE}")
    extra = " + generate_hdf5.py" if GENERATOR_SCRIPT.is_file() else ""
    print(f"  README.md + {len(notebook_entries)} notebooks{extra} embedded")


if __name__ == "__main__":
    main()
