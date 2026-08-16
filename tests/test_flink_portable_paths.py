"""Guard: Flink artifacts and submitters must not bake in host-specific paths.

The Flink dist is relocatable. CI, submit scripts, and job graphs must resolve
FLINK_HOME / JARs from the environment or the repo root — never
`/home/runner/work/<old-repo>/…`, a developer home, or a nix store hash.

See AGENTS.md «Portable Flink artifacts».
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SCAN_GLOBS = (
    ".github/workflows/*.yml",
    "submit_iceberg_job.sh",
    "submit_iceberg_with_classpath.sh",
    "test_complete_e2e.sh",
    "test_flink_iceberg_e2e.sh",
    "test_continuous_streaming.py",
    "flink_jobs/*.py",
    "scripts/flink-env.sh",
    "cybersec/flink_paths.py",
    "FLINK_BUILD_GUIDE.md",
)

# Assignment-like or defaulted host paths. Comments that only *forbid* these
# may still mention them.
_FORBIDDEN = [
    (
        re.compile(r"/home/runner/work/"),
        "GitHub Actions runner path — use ${{ github.workspace }}",
    ),
    (
        re.compile(r"/__w/(flink|cybersec)/"),
        "container workspace hardcoded to another repo name",
    ),
    (
        re.compile(r"/Users/[A-Za-z0-9_.-]+/"),
        "developer home path",
    ),
    (
        re.compile(r"/home/[A-Za-z0-9_.-]+/local/src/"),
        "site checkout path",
    ),
    (
        re.compile(r"/nix/store/[a-z0-9]{20,}-flink"),
        "nix store Flink path as a default",
    ),
]


def _iter_source_files():
    for pattern in SCAN_GLOBS:
        yield from ROOT.glob(pattern)


def test_no_host_specific_paths_in_flink_submitters():
    violations = []
    for path in _iter_source_files():
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        rel = path.relative_to(ROOT)
        for i, line in enumerate(text.splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("#") or stripped.startswith("//"):
                lowered = line.lower()
                if any(w in lowered for w in ("never", "not", "forbid", "portable", "relocatable")):
                    continue
            for pat, why in _FORBIDDEN:
                if pat.search(line):
                    violations.append(f"{rel}:{i}: {why}: {line.strip()[:120]}")
    assert not violations, "host-specific Flink path pollution:\n  " + "\n  ".join(violations)


def test_ci_dependency_graph_uses_github_workspace():
    workflow = (ROOT / ".github/workflows/build_and_test.yml").read_text()
    assert "${{ github.workspace }}/flink-cyber" in workflow
    assert "/home/runner/work/cybersec/cybersec" not in workflow


def test_submit_scripts_source_flink_env():
    for name in ("submit_iceberg_job.sh", "submit_iceberg_with_classpath.sh"):
        text = (ROOT / name).read_text()
        assert "scripts/flink-env.sh" in text, f"{name} must source scripts/flink-env.sh"
        assert "nix/store" not in text
        assert "/Users/" not in text
