# zarf-v1.6.6 ship (cybersec-dask) + cyberphy distro rebrand

## Release (wire name unchanged)

| Item | Value |
|------|--------|
| Tag | `zarf-v1.6.6` on `rch/cldr-cybersec` |
| Package | `zarf-package-cybersec-dask-amd64-1.6.6.tar.zst` |
| Image | `cybersec-dask:2025.2.0-85f3d9ecf5` |
| Build | `just image` → `just package` → `just release-bundle` |

**Kept as cybersec-dask:** Zarf package `metadata.name`, container image name, AWS/resource tags that already use that string, Polaris/catalog/DB names where they are wire identifiers.

## Post-ship: tooling + Python → cyberphy

| Layer | Name |
|-------|------|
| Distribution (`pyproject.toml` / `uv.lock`) | **`cyberphy`** |
| Import path | `cybersec.*` (stable) |
| CLI scripts | `cyberphy`, `cyberphy-mcp` (primary); `cybersec*` aliases |
| MCP server display name | already `cyberphy` |
| devenv task CLI invokes | `uv run cyberphy "…"` |
| Standalone SDK | `packages/hdf5_iceberg` (`hdf5-iceberg`) — already product-agnostic |

**Not renamed in this pass:** `cybersec/` directory tree, postgres DB `cybersec`, S3 bucket patterns `cybersec-dask-*`, Zarf package/image `cybersec-dask`.
