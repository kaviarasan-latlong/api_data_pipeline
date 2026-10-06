#!/usr/bin/env bash
# Entry point called by the Airflow DAG (data_dag.py).
# Activates the pipeline's Python environment, runs one API window, then
# exports Anuga submissions for that successful window.

set -euo pipefail

PIPELINE_DIR="/var/www/kaviarasan/data_pipeline"
VENV_PATH="/var/www/kaviarasan/data_pipeline/venv"

if [[ -f "$VENV_PATH/bin/activate" ]]; then
    source "$VENV_PATH/bin/activate"
else
    echo "WARNING: venv not found at $VENV_PATH - falling back to system python3" >&2
fi

if [[ -x "$VENV_PATH/bin/python3" ]]; then
    PYTHON_BIN="$VENV_PATH/bin/python3"
elif [[ -x "$VENV_PATH/bin/python" ]]; then
    PYTHON_BIN="$VENV_PATH/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
else
    echo "ERROR: Python was not found in the venv or system PATH." >&2
    exit 127
fi

cd "$PIPELINE_DIR"
"$PYTHON_BIN" main.py
"$PYTHON_BIN" submissions_geo_export.py --from-watermark
