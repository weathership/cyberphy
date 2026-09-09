#!/usr/bin/env bash
# verify-s3-datapath.sh — prove the *deployed* panel/Dask stack can reach S3 and
# read the configured dataset (marker + span parquet already in place).
#
# Why this exists: Kubernetes readiness is TCP-only for otel-navigator. Pods can
# be Ready while S3_BUCKET is blank, the endpoint is wrong, auth fails, or the
# bucket has no spans. This check runs *inside* a cluster pod so it uses the
# same env/creds the app uses — secrets never appear on argv/ps.
#
# Usage (control-plane or any host with kubeconfig):
#   sudo bash zarf/scripts/verify-s3-datapath.sh
#   bash verify-s3-datapath.sh --json
#   bash verify-s3-datapath.sh --min-parquet 1
#
# Exit codes:
#   0  — bucket reachable, marker OK, ≥1 span parquet readable (or --allow-empty)
#   1  — config/auth/data failure (see printed diagnosis)
#   2  — preconditions (no kubectl / no Ready pod to exec into)
#
# Env (optional overrides; default = read from panel-viz ConfigMap + pod env):
#   KUBECONFIG, S3_BUCKET (only if CM empty — prefer CM), --allow-empty
set -euo pipefail

JSON=0
ALLOW_EMPTY=0
MIN_PARQUET=1
QUIET=0
while [ $# -gt 0 ]; do
  case "$1" in
    --json) JSON=1; shift ;;
    --allow-empty) ALLOW_EMPTY=1; MIN_PARQUET=0; shift ;;
    --min-parquet) MIN_PARQUET="${2:?}"; shift 2 ;;
    --quiet|-q) QUIET=1; shift ;;
    -h|--help)
      sed -n '2,25p' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

export KUBECONFIG="${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}"

_log() { [ "$QUIET" = 1 ] || echo "$*"; }
_err() { echo "$*" >&2; }

# Resolve kubectl (RKE2 layout, zarf tools, PATH)
kc() {
  if [ -x /var/lib/rancher/rke2/bin/kubectl ]; then
    /var/lib/rancher/rke2/bin/kubectl --kubeconfig "$KUBECONFIG" "$@"
  elif command -v kubectl >/dev/null 2>&1; then
    kubectl --kubeconfig "$KUBECONFIG" "$@"
  elif command -v zarf >/dev/null 2>&1; then
    zarf tools kubectl --kubeconfig "$KUBECONFIG" "$@"
  else
    _err "❌ no kubectl (checked RKE2 bin, PATH, zarf tools)"
    exit 2
  fi
}

# --------------------------------------------------------------------------- #
# 1) Configured location (cluster ConfigMap — what the app actually got)
# --------------------------------------------------------------------------- #
_log "== configured S3 (panel-viz ConfigMap) =="
CM_JSON=$(kc -n panel-viz get configmap otel-navigator-config -o json 2>/dev/null || true)
if [ -z "$CM_JSON" ]; then
  _err "❌ otel-navigator-config ConfigMap missing in panel-viz — deploy panel-viz first"
  exit 2
fi
CFG_BUCKET=$(printf '%s' "$CM_JSON" | python3 -c 'import sys,json; d=json.load(sys.stdin).get("data") or {}; print((d.get("S3_BUCKET") or "").strip())')
CFG_PATH=$(printf '%s' "$CM_JSON" | python3 -c 'import sys,json; d=json.load(sys.stdin).get("data") or {}; print((d.get("OTEL_DATA_PATH") or "").strip())')
CFG_REGION=$(printf '%s' "$CM_JSON" | python3 -c 'import sys,json; d=json.load(sys.stdin).get("data") or {}; print((d.get("AWS_REGION") or "us-east-1").strip())')

_log "  S3_BUCKET       = ${CFG_BUCKET:-<empty>}"
_log "  OTEL_DATA_PATH  = ${CFG_PATH:-<empty>}"
_log "  AWS_REGION      = ${CFG_REGION}"

if [ -z "$CFG_BUCKET" ] || [ "$CFG_BUCKET" = "s3:///" ] || [[ "$CFG_PATH" == "s3:///"* ]] || [[ "$CFG_PATH" == "s3:///" ]]; then
  _err "❌ blank/unrendered S3_BUCKET or OTEL_DATA_PATH=s3:/// — redeploy panel-viz with S3_* (see converge T5.otel-navigator)"
  exit 1
fi

# Secret presence only (never print values)
if ! kc -n panel-viz get secret otel-navigator-credentials >/dev/null 2>&1; then
  _err "❌ otel-navigator-credentials Secret missing"
  exit 1
fi
_log "  credentials Secret: present"

# --------------------------------------------------------------------------- #
# 2) Pick an in-cluster exec target that has s3fs + the same creds as the app
# --------------------------------------------------------------------------- #
EXEC_NS=""
EXEC_TARGET=""  # deploy/name or pod/name
EXEC_C=""

if kc -n panel-viz get deploy otel-navigator >/dev/null 2>&1; then
  ready=$(kc -n panel-viz get pods -l app=otel-navigator \
    -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null | grep -c True || true)
  if [ "${ready:-0}" -ge 1 ]; then
    EXEC_NS=panel-viz
    EXEC_TARGET="deploy/otel-navigator"
    EXEC_C="-c otel-navigator"
  fi
fi
if [ -z "$EXEC_TARGET" ] && kc -n dask get deploy -l dask.org/component=scheduler -o name 2>/dev/null | head -1 | grep -q .; then
  EXEC_NS=dask
  # Prefer the standard name; fall back to first scheduler pod
  if kc -n dask get deploy cybersec-dask-scheduler >/dev/null 2>&1; then
    EXEC_TARGET="deploy/cybersec-dask-scheduler"
  else
    EXEC_TARGET=$(kc -n dask get pods -l dask.org/component=scheduler -o name 2>/dev/null | head -1)
  fi
fi
if [ -z "$EXEC_TARGET" ]; then
  _err "❌ no Ready otel-navigator or Dask scheduler to exec into"
  exit 2
fi
_log "== in-cluster probe via $EXEC_NS/$EXEC_TARGET ${EXEC_C:-} =="

# --------------------------------------------------------------------------- #
# 3) In-pod check: auth + marker + span parquet (mirrors otel-navigator loader)
# --------------------------------------------------------------------------- #
# CRITICAL: kubectl exec needs -i so the heredoc reaches remote `python3 -`.
# Without -i, the pod gets empty stdin → empty RESULT → host-side json.loads
# blows up ("Expecting value...") and converge T5.s3-datapath fails as a
# false negative even when S3 is fine.
#
# MIN_PARQUET / ALLOW_EMPTY / CFG_BUCKET injected via env on the exec so we
# never embed operator secrets; the pod already has AWS_* and S3_ENDPOINT.
# shellcheck disable=SC2086
PROBE_RC=0
RESULT=$(kc -n "$EXEC_NS" exec -i $EXEC_TARGET $EXEC_C -- env \
  CHECK_BUCKET="$CFG_BUCKET" \
  CHECK_MIN_PARQUET="$MIN_PARQUET" \
  CHECK_ALLOW_EMPTY="$ALLOW_EMPTY" \
  CHECK_CFG_PATH="$CFG_PATH" \
  python3 - <<'PY'
import json, os, sys

def die(code, msg, **extra):
    out = {"ok": False, "error": msg, **extra}
    print(json.dumps(out))
    sys.exit(code)

bucket = (os.environ.get("CHECK_BUCKET") or os.environ.get("S3_BUCKET") or "").strip()
cfg_path = (os.environ.get("CHECK_CFG_PATH") or os.environ.get("OTEL_DATA_PATH") or "").strip()
min_pq = int(os.environ.get("CHECK_MIN_PARQUET") or "1")
allow_empty = os.environ.get("CHECK_ALLOW_EMPTY") == "1"

# Presence / lengths only — never print secret material
akid = os.environ.get("AWS_ACCESS_KEY_ID") or ""
secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or ""
endpoint = (os.environ.get("S3_ENDPOINT") or "").strip()
region = (os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1").strip()
pod_bucket = (os.environ.get("S3_BUCKET") or "").strip()

info = {
    "ok": True,
    "bucket_configmap": bucket,
    "bucket_pod_env": pod_bucket,
    "otel_data_path": cfg_path,
    "endpoint_set": bool(endpoint),
    "endpoint_host": endpoint.split("://")[-1].split("/")[0] if endpoint else "",
    "region": region,
    "aws_access_key_len": len(akid),
    "aws_secret_key_len": len(secret),
}

if not bucket:
    die(1, "S3_BUCKET empty in check env and pod", **info)
if len(akid) == 0 or len(secret) == 0:
    die(1, "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY empty in pod env — redeploy with S3 creds", **info)
if pod_bucket and pod_bucket != bucket:
    info["warning"] = f"pod S3_BUCKET={pod_bucket!r} != ConfigMap {bucket!r}"
if not endpoint:
    die(1, "S3_ENDPOINT empty in pod env — Secret may be set but container not restarted "
           "(rollout restart otel-navigator)", **info)

# Fail fast: TCP to endpoint before s3fs can hang (field converge-26: 180s timeout).
import socket, urllib.parse
try:
    u = urllib.parse.urlparse(endpoint)
    host = u.hostname or info.get("endpoint_host") or ""
    port = u.port or (443 if (u.scheme or "https") == "https" else 80)
    info["endpoint_host"] = host
    info["endpoint_port"] = port
    s = socket.create_connection((host, port), timeout=5)
    s.close()
    info["tcp_ok"] = True
except Exception as e:
    info["tcp_ok"] = False
    die(1, f"TCP to S3_ENDPOINT failed: {type(e).__name__}: {e} — "
           f"pod cannot reach endpoint host (network/URL), not a JSON/creds-quote issue",
        **info)

try:
    import s3fs
except ImportError as e:
    die(1, f"s3fs not installed in probe image: {e}", **info)

# Match app / RUNBOOK: path-style when a custom endpoint is set (MinIO / gateway).
# Short botocore timeouts via config_kwargs only — do NOT also pass
# client_kwargs["config"] (aiobotocore: multiple values for keyword 'config').
kw = {
    "key": akid,
    "secret": secret,
    "client_kwargs": {"endpoint_url": endpoint, "region_name": region},
    "config_kwargs": {
        "s3": {"addressing_style": "path"},
        "connect_timeout": 5,
        "read_timeout": 20,
        "retries": {"max_attempts": 2, "mode": "standard"},
    },
}
# Session token for temporary IAM creds
tok = os.environ.get("AWS_SESSION_TOKEN") or ""
if tok:
    kw["token"] = tok
socket.setdefaulttimeout(30)

try:
    fs = s3fs.S3FileSystem(**kw)
except Exception as e:
    die(1, f"S3FileSystem init failed: {e}", **info)

# --- reach bucket ---
try:
    # ls root of bucket (auth + existence)
    fs.ls(bucket)
    info["bucket_reachable"] = True
except Exception as e:
    die(1, f"cannot list bucket {bucket!r}: {type(e).__name__}: {e}", **info)

# --- resolve data root (marker dataset and/or OTEL_DATA_PATH) ---
# App loads s3://{bucket}/{dataset}/spans/… (partitioned), never only top-level
# of OTEL_DATA_PATH. Field: OTEL_DATA_PATH=s3://dhfo/otel-notebook/ with parquet
# under otel-notebook/spans/date=*/hour=*/*.parquet.
def _s3_key_from_uri(uri: str, default_bucket: str) -> str:
    """s3://b/prefix/ → b/prefix  (no leading/trailing slash on prefix side)."""
    u = (uri or "").strip()
    if u.startswith("s3://"):
        rest = u[5:].strip("/")
        return rest
    return f"{default_bucket}/{u.strip('/')}" if u else default_bucket

def _dataset_roots():
    """Ordered unique roots to search (bucket/prefix without trailing slash)."""
    roots = []
    # 1) marker at bucket root (same as otel-navigator.get_active_dataset)
    marker_key = f"{bucket}/_active_dataset.json"
    marker = None
    try:
        if fs.exists(marker_key):
            with fs.open(marker_key, "r") as f:
                marker = json.load(f)
    except Exception as e:
        die(1, f"cannot read marker s3://{marker_key}: {e}", **info)
    if marker is not None:
        ds = (marker.get("dataset") or marker.get("prefix") or "").strip().strip("/")
        if ds:
            roots.append(f"{bucket}/{ds}")
            info["marker"] = {
                "dataset": ds,
                "phase": marker.get("phase"),
                "total_spans": marker.get("total_spans", marker.get("span_count")),
                "updated_at": marker.get("updated_at"),
            }
        else:
            info["marker_warning"] = "marker has no dataset/prefix key"
    else:
        info["marker_warning"] = f"marker missing s3://{marker_key}"
    # 2) OTEL_DATA_PATH from ConfigMap (may be s3://bucket/otel-notebook/)
    if cfg_path:
        root = _s3_key_from_uri(cfg_path, bucket)
        if root and root not in roots:
            # If path is just the bucket, skip; need a dataset prefix
            if root != bucket and root != f"{bucket}/":
                roots.append(root.rstrip("/"))
    # de-dupe preserve order
    seen = set()
    out_roots = []
    for r in roots:
        r = r.rstrip("/")
        if r and r not in seen:
            seen.add(r)
            out_roots.append(r)
    return out_roots

def _find_span_parquet(data_root: str):
    """Parquet under {root}/spans/ only (partitioned layout), not top-level of root.

    Mirrors otel-navigator.load_span_data: always append /spans, then partition globs.
    s3fs ** is unreliable — use find() + explicit partition patterns.
    """
    spans = f"{data_root.rstrip('/')}/spans"
    found = []
    method = None
    # Explicit layouts (same order as otel-navigator)
    patterns = [
        f"{spans}/shard=*/date=*/batch_*.parquet",
        f"{spans}/shard=*/date=*/*.parquet",
        f"{spans}/date=*/hour=*/*.parquet",
        f"{spans}/date=*/*.parquet",
        f"{spans}/date=*/hour=*/**/*.parquet",
    ]
    for pat in patterns:
        try:
            hits = list(fs.glob(pat) or [])
        except Exception:
            hits = []
        hits = [h for h in hits if str(h).endswith(".parquet")]
        if hits:
            found, method = hits, f"glob:{pat}"
            break
    if not found:
        try:
            # Recursive listing under spans/ only (never dataset top-level)
            entries = fs.find(spans) if fs.exists(spans) else []
            found = [e for e in entries if str(e).endswith(".parquet")]
            if found:
                method = f"find:{spans}"
        except Exception:
            found = []
    return found, spans, method

roots = _dataset_roots()
if not roots:
    die(1, "no data root: need _active_dataset.json dataset=… and/or OTEL_DATA_PATH "
           "with a prefix (e.g. s3://bucket/otel-notebook/)", **info)

files = []
chosen_root = None
spans_path = None
for root in roots:
    hits, spans_p, method = _find_span_parquet(root)
    info.setdefault("roots_tried", []).append({
        "root": f"s3://{root}/",
        "spans": f"s3://{spans_p}/",
        "parquet": len(hits),
        "method": method,
    })
    if hits:
        files = hits
        chosen_root = root
        spans_path = spans_p
        info["glob_pattern"] = method
        break

if not chosen_root:
    r0 = roots[0]
    msg = (
        f"no parquet under s3://{r0}/spans/ (partitioned layout required: "
        f"spans/date=*/hour=*/*.parquet or spans/shard=*/date=*/*.parquet) — "
        f"parquet only at top-level of OTEL_DATA_PATH is NOT enough"
    )
    if allow_empty:
        info["ok"] = True
        info["parquet_count"] = 0
        info["warning"] = msg
        print(json.dumps(info))
        sys.exit(0)
    die(1, msg, **info)

info["dataset_path"] = f"s3://{chosen_root}/"
info["spans_path"] = f"s3://{spans_path}/"
ds_name = chosen_root[len(bucket):].lstrip("/") if chosen_root.startswith(bucket) else chosen_root
if not isinstance(info.get("marker"), dict):
    info["marker"] = {"dataset": ds_name}
elif not info["marker"].get("dataset"):
    info["marker"]["dataset"] = ds_name

info["parquet_count"] = len(files)
info["parquet_sample"] = files[:5]

if len(files) < min_pq and not allow_empty:
    die(1,
        f"found {len(files)} parquet under s3://{spans_path}/ "
        f"(need ≥{min_pq}) — check partitioned spans layout under dataset root",
        **info)

# --- open first object (read path, not just list) ---
if files:
    sample = files[0]
    try:
        with fs.open(sample, "rb") as f:
            head = f.read(64)
        info["sample_readable"] = True
        info["sample_key"] = sample
        info["sample_head_bytes"] = len(head)
        # Optional: parquet magic
        if head[:4] == b"PAR1" or b"PAR1" in head:
            info["sample_looks_like_parquet"] = True
    except Exception as e:
        die(1, f"list OK but cannot read sample {sample}: {e}", **info)

# --- optional: dask/pyarrow smoke if available (same stack as workers) ---
if files and os.environ.get("CHECK_DEEP") == "1":
    try:
        import pyarrow.parquet as pq
        with fs.open(files[0], "rb") as f:
            t = pq.read_table(f, columns=None)
        info["deep_rows"] = t.num_rows
        info["deep_cols"] = t.column_names[:12]
    except Exception as e:
        info["deep_error"] = str(e)

print(json.dumps(info))
sys.exit(0)
PY
) || PROBE_RC=$?

# Normalize RESULT to a single JSON object (strips kubectl warnings / empty).
# Always emits valid JSON so host-side never raises JSONDecodeError.
RESULT=$(printf '%s' "${RESULT:-}" | python3 -c '
import json, sys
raw = sys.stdin.read()
raw_s = (raw or "").strip()
if not raw_s:
    print(json.dumps({
        "ok": False,
        "error": "empty probe output — kubectl exec did not deliver script stdin "
                 "(need: kubectl exec -i … -- python3 -). Converge false-negative.",
    }))
    sys.exit(0)
# Prefer last line that is a full JSON object (probe prints one line).
for line in reversed(raw_s.splitlines()):
    line = line.strip()
    if line.startswith("{") and line.endswith("}"):
        try:
            json.loads(line)
            print(line)
            sys.exit(0)
        except json.JSONDecodeError:
            continue
try:
    json.loads(raw_s)
    print(raw_s)
except json.JSONDecodeError:
    print(json.dumps({
        "ok": False,
        "error": "non-json probe output: " + repr(raw_s[:240]),
    }))
')

# Annotate empty/non-json with exec rc when useful
if [ "$PROBE_RC" -ne 0 ]; then
  RESULT=$(printf '%s' "$RESULT" | python3 -c '
import json,sys
d=json.loads(sys.stdin.read())
if not d.get("ok") and "kubectl exec" not in (d.get("error") or ""):
    d["error"] = (d.get("error") or "probe failed") + f" (kubectl exec rc='"$PROBE_RC"')"
print(json.dumps(d))
')
fi

if [ "$JSON" = 1 ]; then
  echo "$RESULT" | python3 -m json.tool 2>/dev/null || echo "$RESULT"
else
  echo "$RESULT" | python3 -c '
import json,sys
d=json.load(sys.stdin)
ok=d.get("ok", False)
print("== result ==")
print("  ok:                 ", ok)
print("  bucket:             ", d.get("bucket_configmap"))
print("  endpoint:           ", d.get("endpoint_host") or "(AWS default)")
print("  access_key_len:     ", d.get("aws_access_key_len"))
print("  secret_key_len:     ", d.get("aws_secret_key_len"))
print("  bucket_reachable:   ", d.get("bucket_reachable"))
m=d.get("marker") or {}
print("  active dataset:     ", m.get("dataset"))
print("  marker phase:       ", m.get("phase"))
print("  marker spans:       ", m.get("total_spans"))
print("  dataset_path:       ", d.get("dataset_path"))
print("  spans_path:         ", d.get("spans_path"))
print("  parquet_count:      ", d.get("parquet_count"))
print("  discovery:          ", d.get("glob_pattern"))
print("  sample_readable:    ", d.get("sample_readable"))
if d.get("parquet_sample"):
    print("  sample keys:")
    for k in d["parquet_sample"][:5]:
        print("   -", k)
if d.get("error"):
    print("  ERROR:", d["error"])
    sys.exit(1)
if not ok:
    sys.exit(1)
print()
print("✔ S3 datapath OK — app can reach configured bucket and read span parquet")
'
fi

# Propagate python exit via JSON ok field (RESULT is always valid JSON now)
echo "$RESULT" | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d.get("ok") else 1)'
