# weathership/oss-flink-python

Apache Flink **1.20.1** PyFlink (`apache-flink`) with the small patches
[weathership/cyberphy](https://github.com/weathership/cyberphy) needs so
Python 3.12 + Dask can share a venv.

This is **not** a replacement for `thirdparty/flink` (the Java distribution).
It exists so `uv sync` on a disk-light clone does not have to initialize the
5+ GB Flink submodule.

## Upstream

- PyPI sdist: `apache-flink==1.20.1`
- Same delta as `rch/asf-flink` branch `rch/devenv-cybersec`
  (`0238438c3f4`, `0401b71d9a6`)

## Patches

1. **cloudpickle 3.x / Dask** — `apache-beam>=2.71.0` (Beam dropped its
   cloudpickle pin), `cloudpickle>=3.0.0`, Python 3.12-compatible bounds for
   numpy/pandas/pyarrow/pemja.
2. **avro 1.12** — `AvroTypeException` / `SchemaResolutionException` moved to
   `avro.errors`; `Validate` is `avro.io.validate`.

ASF License 2.0 — see `LICENSE`.
