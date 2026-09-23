#!/usr/bin/env bash
# Entry point called by the Airflow DAG (api_dag.py).
# Activates the pipeline's Python environment and runs one pipeline
# invocation (one window; main.py itself loops chunks within the window).

set -euo pipefail

PIPELINE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="${PIPELINE_VENV_PATH:-$PIPELINE_DIR/venv}"

if [ -f "$VENV_PATH/bin/activate" ]; then
    source "$VENV_PATH/bin/activate"
else
    echo "WARNING: venv not found at $VENV_PATH - falling back to system python3" >&2
fi

cd "$PIPELINE_DIR"
python main.py
