"""Cyberphy Toolkit — CPS observability and analytics lakehouse.

**Product / distribution name:** ``cyberphy`` (``pip install`` / ``uv sync``).
**Import path:** ``cybersec`` (stable wire identifier; do not rename mid-flight).
**CLI:** ``cyberphy`` / ``cyberphy-mcp`` preferred; ``cybersec`` / ``cybersec-mcp``
aliases still install for scripts and docs that have not switched yet.

Zarf package and container image remain ``cybersec-dask`` for air-gap continuity
until a dedicated rename cut.
"""

__version__ = "0.1.0"
__product__ = "cyberphy"
__import_name__ = "cybersec"
