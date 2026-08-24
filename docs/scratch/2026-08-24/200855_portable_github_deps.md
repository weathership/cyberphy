# Portable GitHub remotes and vendored PyFlink

Migration follow-up: `weathership/cyberphy` must clone and `uv sync` without
`~/local/src/cldr/cybersec`, rch SSH keys, or a 5+ GB `thirdparty/flink`
checkout.

## What was wrong

| Pin | Before | Problem |
|-----|--------|---------|
| `thirdparty/flink` | `git@github.com:rch/asf-flink.git` | SSH-only personal fork |
| `thirdparty/iceberg` | `git@github.com:rch/asf-iceberg.git` | SSH; SHA is already on `apache/iceberg` |
| `thirdparty/cdpcli` | `git@github.com:rch/cldr-cdpcli.git` | SSH |
| `flink-solr-log-indexer` | listed in `.gitmodules` | no gitlink — dead entry |
| `[tool.uv.sources] apache-flink` | `path = thirdparty/flink/flink-python` | `uv sync` failed on a disk-light clone |

NiFi and Polaris were already `https://github.com/apache/…`.

## What we did

1. **`.gitmodules`** — HTTPS only. Iceberg retargeted to
   `https://github.com/apache/iceberg.git` (gitlink SHA `7dbafb438` is that
   upstream commit). Dropped `flink-solr-log-indexer`.
2. **PyFlink** — vendored Apache Flink 1.20.1 `apache-flink` sdist plus the
   cloudpickle 3.x / Dask and avro 1.12 patches (`rch/asf-flink`
   `0238438c3f4`, `0401b71d9a6`) at `thirdparty/flink-python` (~15 MB).
   `uv sync` no longer initializes Java Flink.
3. **Shell** — `devenv` `enterShell` does not auto-init GB-scale submodules.
   Opt-in: `CYBERPHY_INIT_SUBMODULES=1`.
4. **Guard** — `tests/test_portable_sources.py`.

## Not done (needs an explicit org-repo request)

A dedicated `weathership/oss-flink-python` (and optionally
`weathership/oss-flink` fork of `rch/asf-flink`) would let uv pin a git URL
instead of vendoring. Creating those GitHub repos was not done here.

Java Flink / Iceberg / NiFi / Polaris source builds still use gitlinks; do not
`git submodule update --init --recursive` on a disk-constrained machine unless
the operator asks.
