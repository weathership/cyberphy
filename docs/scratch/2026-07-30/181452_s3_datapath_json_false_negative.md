# T5.s3-datapath JSON false-negative (engine 0.5.12)

## Symptom

In-situ `bash zarf/scripts/verify-s3-datapath.sh` raised a host-side JSON error
(`JSONDecodeError: Expecting value…`). Converge T5.s3-datapath FAIL with
timeout / unusable detail while CM/Secret LOOKED populated.

## Root cause

`kubectl exec … -- python3 - <<'PY'` **without `-i`**: the heredoc never reaches
the pod. Remote `python3 -` gets empty stdin → empty `RESULT` → host
`json.load` blows up. Converge runs the same script with `--quiet --json` and
treated non-JSON / empty as datapath FAIL (false negative).

Quoting of creds file was unrelated; unquoted remains correct.

## Fix (0.5.12)

1. `verify-s3-datapath.sh`: `kubectl exec -i`; always normalize RESULT to valid
   JSON; clear error if empty.
2. `catalog._det_s3_datapath`: robust `_parse_s3_datapath_json`; catch script
   timeout; on empty/non-JSON fall back to inline `python3 -c` probe.
3. Tests: `tests/test_converge_s3_datapath_parse.py`.

## Operator recheck

```bash
bash zarf/scripts/verify-s3-datapath.sh
bash zarf/scripts/verify-s3-datapath.sh --json
```

If still FAIL after -i fix: real S3 path (endpoint reachability, marker at
`s3://$bucket/_active_dataset.json`, parquet under dataset).
