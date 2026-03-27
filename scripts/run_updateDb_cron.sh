#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/home/alex_g/src"
LOG_DIR="${REPO_DIR}/logs"
LOG_FILE="${LOG_DIR}/updateDb-cron.log"
PYTHON_BIN="${REPO_DIR}/.venv/bin/python"

mkdir -p "$LOG_DIR"
cd "$REPO_DIR"

if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="/usr/bin/python3"
fi

if [ -f "${REPO_DIR}/postprod.env" ]; then
    set -a
    # shellcheck disable=SC1090
    . "${REPO_DIR}/postprod.env"
    set +a
fi

{
    echo "----- $(date -Iseconds) -----"
    "$PYTHON_BIN" updateDb.py
} >> "$LOG_FILE" 2>&1
