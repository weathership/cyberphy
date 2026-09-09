"""T5.s3-datapath JSON extraction — empty/mixed stdout must not brick converge."""

from converge.catalog import _parse_s3_datapath_json


def test_empty_raw():
    assert _parse_s3_datapath_json("") == {}
    assert _parse_s3_datapath_json("   \n  ") == {}


def test_pure_object_line():
    d = _parse_s3_datapath_json('{"ok": true, "bucket": "dhfo", "parquet_count": 2}')
    assert d["ok"] is True
    assert d["bucket"] == "dhfo"


def test_kubectl_warning_prefix():
    raw = (
        'Defaulted container "otel-navigator" out of: otel-navigator, pty-proxy\n'
        '{"ok": true, "bucket_configmap": "dhfo", "parquet_count": 3}\n'
    )
    d = _parse_s3_datapath_json(raw)
    assert d.get("ok") is True
    assert d.get("bucket_configmap") == "dhfo"


def test_pretty_printed_json():
    raw = '{\n  "ok": true,\n  "bucket_configmap": "dhfo"\n}\n'
    d = _parse_s3_datapath_json(raw)
    assert d.get("ok") is True
    assert d.get("bucket_configmap") == "dhfo"


def test_garbage_not_json():
    d = _parse_s3_datapath_json("JSONDecodeError: Expecting value: line 1 column 1")
    assert d == {}


def test_error_object():
    d = _parse_s3_datapath_json(
        '{"ok": false, "error": "empty probe output — kubectl exec did not deliver"}'
    )
    assert d.get("ok") is False
    assert "empty probe" in d.get("error", "")
