#!/usr/bin/env bash
# Refresh all KeynesWatch data sources.
#
# Optional environment overrides:
#   PROJECT_DIR=/path/to/Keynes-Watch
#   LOG_FILE=/path/to/update.log
#   VENV_DIR=/path/to/.venv
#   FETCH_SOURCES="fred bea fiscal"

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
LOG_FILE="${LOG_FILE:-$PROJECT_DIR/update.log}"
VENV_DIR="${VENV_DIR:-$PROJECT_DIR/.venv}"
PYTHON_BIN="$VENV_DIR/bin/python"
REQUIREMENTS_FILE="$PROJECT_DIR/requirements.txt"
REQUIREMENTS_STAMP="$VENV_DIR/.requirements.stamp"
FAILED=0

DEFAULT_SOURCES="fred bea fiscal nyfed treasury indeed pboc nbs mof"
FETCH_SOURCES="${FETCH_SOURCES:-$DEFAULT_SOURCES}"

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

requirements_need_install() {
    if [ ! -f "$REQUIREMENTS_STAMP" ] || [ "$REQUIREMENTS_FILE" -nt "$REQUIREMENTS_STAMP" ]; then
        return 0
    fi

    if ! "$PYTHON_BIN" -u -c "import akshare, bs4, mysql.connector, pandas, requests" 2>&1 | tee -a "$LOG_FILE"; then
        log_message "Virtual environment is missing required modules"
        return 0
    fi

    return 1
}

ensure_venv() {
    local bootstrap_python

    if [ ! -x "$PYTHON_BIN" ]; then
        if command -v python3 >/dev/null 2>&1; then
            bootstrap_python="python3"
        elif command -v python >/dev/null 2>&1; then
            bootstrap_python="python"
        else
            log_message "FAILED: python3/python not found for venv bootstrap"
            exit 1
        fi

        log_message "Creating virtual environment: $VENV_DIR"
        if ! "$bootstrap_python" -u -m venv "$VENV_DIR" 2>&1 | tee -a "$LOG_FILE"; then
            log_message "FAILED: could not create virtual environment"
            exit 1
        fi
    fi

    if [ -f "$REQUIREMENTS_FILE" ] && requirements_need_install; then
        log_message "Installing requirements into virtual environment"
        if ! "$PYTHON_BIN" -u -m pip install -r "$REQUIREMENTS_FILE" 2>&1 | tee -a "$LOG_FILE"; then
            log_message "FAILED: could not install requirements"
            exit 1
        fi
        touch "$REQUIREMENTS_STAMP"
    fi

    log_message "Using Python: $("$PYTHON_BIN" -c 'import sys; print(sys.executable)' 2>/dev/null || echo "$PYTHON_BIN")"
}

run_source() {
    local source=$1
    log_message "Starting: $source"
    if ! "$PYTHON_BIN" -u -m fetch_data.run --source "$source" 2>&1 | tee -a "$LOG_FILE"; then
        log_message "FAILED: $source"
        FAILED=$((FAILED + 1))
    fi
}

log_message "=== Data update started ==="
cd "$PROJECT_DIR" || exit 1
ensure_venv

for source in $FETCH_SOURCES; do
    run_source "$source"
done

if [ "$FAILED" -gt 0 ]; then
    log_message "=== Data update completed with $FAILED failure(s) ==="
    exit 1
fi

log_message "=== Data update completed successfully ==="
