#!/bin/bash
# Test the observability module in an isolated Python environment
# This avoids the Flink/Dask dependency conflict

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=== Testing Observability Module (Isolated) ==="
echo "Project root: $PROJECT_ROOT"

# Create isolated virtual environment
VENV_DIR="/tmp/observability-test-venv"
if [ -d "$VENV_DIR" ]; then
    echo "Removing existing test venv..."
    rm -rf "$VENV_DIR"
fi

echo "Creating isolated virtual environment..."
python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

echo "Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet \
    pyarrow>=15.0.0 \
    pandas>=2.0.0 \
    numpy>=2.0.0 \
    dask[complete]>=2025.2.0 \
    distributed>=2025.2.0 \
    holoviews>=1.18.0 \
    bokeh>=3.3.0 \
    networkx>=3.0 \
    pytest>=8.0.0

# Install the project in editable mode (observability only)
echo "Installing cyberphy package (import path cybersec.*)..."
pip install --quiet -e "$PROJECT_ROOT" --no-deps

echo ""
echo "=== Running Tests ==="

# Run schema tests
echo "Testing schema module..."
python -c "
from cybersec.observability.schema import SPANS_SCHEMA, METRICS_SCHEMA, LOGS_SCHEMA
print(f'  SPANS_SCHEMA: {len(SPANS_SCHEMA)} fields')
print(f'  METRICS_SCHEMA: {len(METRICS_SCHEMA)} fields')
print(f'  LOGS_SCHEMA: {len(LOGS_SCHEMA)} fields')
"

# Run transforms tests
echo "Testing transforms module..."
python -c "
import pandas as pd
from cybersec.observability.transforms import (
    duration_to_ms,
    compute_latency_percentiles,
    extract_attribute,
)
ns = pd.Series([1_000_000, 2_000_000, 3_000_000])
ms = duration_to_ms(ns)
assert list(ms) == [1.0, 2.0, 3.0], 'duration_to_ms failed'
print('  duration_to_ms: OK')

percs = compute_latency_percentiles(pd.Series([10, 20, 30, 40, 50]))
assert percs['mean'] == 30.0, 'compute_latency_percentiles failed'
print('  compute_latency_percentiles: OK')
"

# Run writer tests
echo "Testing writer module..."
python -c "
import tempfile
from datetime import datetime, timezone
from cybersec.observability.writer import OTelWriter, SERVICE_TEMPLATES

with tempfile.TemporaryDirectory() as tmpdir:
    writer = OTelWriter(tmpdir)

    # Test trace generation
    spans = writer._generate_trace(datetime.now(timezone.utc), SERVICE_TEMPLATES[:2], max_depth=2)
    assert len(spans) > 0, 'Trace generation failed'
    print(f'  _generate_trace: OK ({len(spans)} spans)')

    # Test file writing
    files = writer.write_spans(spans, partition_by_service=False)
    assert len(files) > 0, 'File writing failed'
    print(f'  write_spans: OK ({len(files)} files)')
"

# Run reader tests
echo "Testing reader module..."
python -c "
from cybersec.observability.reader import OTelDataset
# Just verify it imports and initializes
import tempfile
with tempfile.TemporaryDirectory() as tmpdir:
    dataset = OTelDataset(tmpdir)
    print('  OTelDataset init: OK')
"

# Run visualization imports
echo "Testing visualization imports..."
python -c "
import holoviews as hv
hv.extension('bokeh')
from cybersec.observability.viz.traces import TraceVisualizer
from cybersec.observability.viz.metrics import MetricsVisualizer
from cybersec.observability.viz.topology import TopologyVisualizer
print('  TraceVisualizer: OK')
print('  MetricsVisualizer: OK')
print('  TopologyVisualizer: OK')
"

# Run pytest on test files
echo ""
echo "=== Running pytest ==="
cd "$PROJECT_ROOT"
pytest tests/test_observability/test_schema.py -v --tb=short
pytest tests/test_observability/test_transforms.py -v --tb=short
pytest tests/test_observability/test_writer.py -v --tb=short

echo ""
echo "=== All Tests Passed ==="

# Cleanup
deactivate
rm -rf "$VENV_DIR"
