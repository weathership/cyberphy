"""Minimal CLI: hdf5-iceberg register --data s3://... --meta s3://cyberphy-md/..."""

from __future__ import annotations

import argparse
import json
import sys


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="hdf5-iceberg")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("register", help="Scan data roots and register into metadata warehouse")
    r.add_argument("--data", action="append", required=True, help="Dataset root URI (repeatable)")
    r.add_argument("--meta", required=True, help="Metadata warehouse URI (writes only here)")
    r.add_argument("--adapter", default="flat_prefix")
    r.add_argument("--endpoint", default=None)
    r.add_argument("--max-files", type=int, default=None)
    r.add_argument("--min-size", type=int, default=1)
    r.add_argument("--json", action="store_true")

    args = p.parse_args(argv)
    if args.cmd == "register":
        from hdf5_iceberg import DatasetProvider, MetadataProvider, register_root

        data = DatasetProvider(
            roots=args.data,
            adapter=args.adapter,
            endpoint_url=args.endpoint,
            min_size_bytes=args.min_size,
        )
        meta = MetadataProvider(warehouse=args.meta, endpoint_url=args.endpoint)
        result = register_root(
            data,
            meta,
            max_files=args.max_files,
        )
        if args.json:
            print(
                json.dumps(
                    {
                        "summary": result.summary(),
                        "n_registered": result.n_registered,
                        "n_reused": result.n_reused,
                        "pointer_table_uri": result.pointer_table_uri,
                        "semantic_uris": result.semantic_uris,
                        "errors": result.errors,
                    },
                    indent=2,
                )
            )
        else:
            print(result.summary())
            if result.semantic_uris:
                print("semantic:", result.semantic_uris)
            for e in result.errors:
                print("error:", e, file=sys.stderr)
        return 1 if result.errors else 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
