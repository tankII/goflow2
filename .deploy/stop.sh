#!/usr/bin/env bash
# GoFlow2 停止脚本
# 用法: bash stop.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# deployed tarball layout
PACKAGE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PACKAGE_ROOT}/logs"
PID_FILE="${GOFLOW2_PID_FILE:-${LOG_DIR}/goflow2.pid}"
# fallback: local .release/ dir produced by .deploy/build.sh
if [[ ! -d "${LOG_DIR}" && -d "${SCRIPT_DIR}/../.release" ]]; then
    PACKAGE_ROOT="$(cd "${SCRIPT_DIR}/../.release" && pwd)"
    LOG_DIR="${PACKAGE_ROOT}/logs"
    PID_FILE="${GOFLOW2_PID_FILE:-${LOG_DIR}/goflow2.pid}"
fi

if [[ ! -f "$PID_FILE" ]]; then
    echo "pid file not found: $PID_FILE (maybe already stopped?)" >&2
    exit 0
fi

PID=$(cat "$PID_FILE")
if kill -0 "$PID" 2>/dev/null; then
    echo ">>> stopping goflow2 pid=$PID ..."
    kill -TERM "$PID"
    # 等待最多 10 秒
    for i in $(seq 1 10); do
        kill -0 "$PID" 2>/dev/null || break
        sleep 0.5
    done
    # 强制杀
    if kill -0 "$PID" 2>/dev/null; then
        echo ">>> force kill pid=$PID"
        kill -9 "$PID"
    fi
    rm -f "$PID_FILE"
    echo ">>> stopped"
else
    echo ">>> process $PID not running, cleaning pid file"
    rm -f "$PID_FILE"
fi