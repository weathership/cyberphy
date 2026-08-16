"""Guard: converge entrypoints must not encode site-specific filesystem layouts.

Package/creds paths are always operator-provided (argv, CWD, co-located with
the script, or the portable /var/tmp staging convention). Discovery must never
walk /mnt/…, /home/…, or named site trees — those are not portable across
air-gap nodes.

See AGENTS.md «Portable paths (air-gap converge)».
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# Air-gap entrypoint + engine only. (converge-aws.sh is AWS sandbox-specific and
# may use account layout; do not mix that into portable air-gap rules.)
SCAN_GLOBS = (
    "zarf/scripts/converge-node.sh",
    "zarf/converge/**/*.py",
)

# Path-like site pollution in *code* (not operator docs). Comments that only
# forbid these patterns may still mention them in prose — we ban assignment-like
# and glob discovery forms.
_FORBIDDEN = [
    # Site mount walks / discovery globs
    (re.compile(r"""/mnt/\*|['\"]/mnt/"""), "/mnt/… discovery or string literals"),
    (re.compile(r"/home/[A-Za-z0-9_.-]+/"), "user home path"),
    # Named field sites / products (do not hardcode)
    (re.compile(r"\bDHFO\b"), "DHFO site name"),
    (re.compile(r"\busfwdbig\d*\b", re.I), "field hostname"),
    (re.compile(r"\bL3bkcamp\b", re.I), "field username"),
]


def _iter_source_files():
    for pattern in SCAN_GLOBS:
        yield from ROOT.glob(pattern)


def test_no_site_specific_paths_in_converge_code():
    violations = []
    for path in _iter_source_files():
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        rel = path.relative_to(ROOT)
        for i, line in enumerate(text.splitlines(), 1):
            stripped = line.lstrip()
            # Allow comments/docstrings that *forbid* these patterns
            if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'''"):
                if "NEVER" in line or "never" in line or "not" in line.lower() or "portable" in line.lower():
                    continue
            for pat, why in _FORBIDDEN:
                if pat.search(line):
                    violations.append(f"{rel}:{i}: {why}: {line.strip()[:120]}")
    assert not violations, "site-specific path pollution:\n  " + "\n  ".join(violations)
