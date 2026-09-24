#!/usr/bin/env bash
# GoFlow2 启停脚本
# 用法: bash startstop.sh [start|stop|restart|status] [CMD ...]
#
# 环境变量:
#   GOFLOW2_LISTEN          监听地址，默认 sflow://:6343,netflow://:2055
#   GOFLOW2_FORMAT          输出格式，默认 json
#   GOFLOW2_TRANSPORT       传输方式，默认 file
#   GOFLOW2_MAPPING         mapping.yaml 路径
#   GOFLOW2_LOGLEVEL        日志级别，默认 info
#   GOFLOW2_PID_FILE        pid 文件路径
#   GOFLOW2_LOG_FILE        日志文件路径
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# deployed tarball layout: bin/conf at package root
PACKAGE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# fallback: local .release/ dir produced by .deploy/build.sh
if [[ ! -d "${PACKAGE_ROOT}/bin" && -d "${SCRIPT_DIR}/../.release/bin" ]]; then
    PACKAGE_ROOT="$(cd "${SCRIPT_DIR}/../.release" && pwd)"
fi
# build.sh nests into goflow2-deploy-*/; walk up one level if needed
if [[ ! -d "${PACKAGE_ROOT}/bin" && -d "${PACKAGE_ROOT}/goflow2-deploy-"* ]]; then
    local_candidate=$(ls -d "${PACKAGE_ROOT}"/goflow2-deploy-*/ 2>/dev/null | head -1)
    if [[ -n "$local_candidate" && -d "${local_candidate}bin" ]]; then
        PACKAGE_ROOT="$local_candidate"
    fi
fi
BIN_DIR="${PACKAGE_ROOT}/bin"
CONF_DIR="${PACKAGE_ROOT}/conf"
LOG_DIR="${PACKAGE_ROOT}/logs"

MAPPING="${GOFLOW2_MAPPING:-${CONF_DIR}/mapping.yaml}"
LISTEN="${GOFLOW2_LISTEN:-sflow://:6343,netflow://:2055}"
FORMAT="${GOFLOW2_FORMAT:-json}"
TRANSPORT="${GOFLOW2_TRANSPORT:-file}"
LOGLEVEL="${GOFLOW2_LOGLEVEL:-info}"
ADDR="${GOFLOW2_ADDR:-:18080}"
PID_FILE="${GOFLOW2_PID_FILE:-${LOG_DIR}/goflow2.pid}"
LOG_FILE="${GOFLOW2_LOG_FILE:-${LOG_DIR}/goflow2.log}"

pick_binary() {
    if [[ -x "${BIN_DIR}/goflow2-linux-amd64" ]]; then
        echo "${BIN_DIR}/goflow2-linux-amd64"
    elif [[ -x "${BIN_DIR}/goflow2-linux-arm64" ]]; then
        echo "${BIN_DIR}/goflow2-linux-arm64"
    else
        echo "ERROR: no binary found in ${BIN_DIR}" >&2
        exit 1
    fi
}

do_start() {
    if [[ -f "$PID_FILE" ]]; then
        local PID
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "goflow2 already running (pid=$PID). stop first: bash startstop.sh stop" >&2
            exit 1
        fi
        rm -f "$PID_FILE"
    fi

    mkdir -p "$(dirname "$LOG_FILE")" "$LOG_DIR"
    local BINARY
    BINARY=$(pick_binary)
    echo ">>> starting goflow2 ..."
    echo "    binary : $BINARY"
    echo "    mapping: $MAPPING"
    echo "    listen : $LISTEN"
    echo "    format : $FORMAT"
    echo "    transport: $TRANSPORT"
    echo "    addr     : $ADDR"
    echo "    loglevel: $LOGLEVEL"
    echo "    pid_file: $PID_FILE"
    echo "    log_file: $LOG_FILE"

    exec "$BINARY" \
        -listen "$LISTEN" \
        -format "$FORMAT" \
        -transport "$TRANSPORT" \
        -mapping "$MAPPING" \
        -loglevel "$LOGLEVEL" \
        -addr "$ADDR" \
        >"$LOG_FILE" 2>&1 &
    local PID=$!
    echo "$PID" > "$PID_FILE"
    echo ">>> started pid=$PID log=$LOG_FILE"
}

do_stop() {
    if [[ ! -f "$PID_FILE" ]]; then
        echo "pid file not found: $PID_FILE (maybe already stopped?)" >&2
        exit 0
    fi
    local PID
    PID=$(cat "$PID_FILE")
    if ! kill -0 "$PID" 2>/dev/null; then
        echo ">>> process $PID not running, cleaning pid file"
        rm -f "$PID_FILE"
        exit 0
    fi
    echo ">>> stopping goflow2 pid=$PID ..."
    kill -TERM "$PID"
    for i in $(seq 1 10); do
        kill -0 "$PID" 2>/dev/null || break
        sleep 0.5
    done
    if kill -0 "$PID" 2>/dev/null; then
        echo ">>> force kill pid=$PID"
        kill -9 "$PID"
    fi
    rm -f "$PID_FILE"
    echo ">>> stopped"
}

do_status() {
    if [[ ! -f "$PID_FILE" ]]; then
        echo "not running (no pid file: $PID_FILE)"
        exit 1
    fi
    local PID
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "running (pid=$PID)"
        exit 0
    else
        echo "not running (stale pid file, cleaned)"
        rm -f "$PID_FILE"
        exit 1
    fi
}

do_restart() {
    do_stop || true
    # small delay to let port release
    sleep 1
    do_start
}

usage() {
    echo "Usage: bash startstop.sh [start|stop|restart|status] [-- <goflow2 args>]"
    echo ""
    echo "Commands:"
    echo "  start    启动采集器（后台运行，写入 pid/log）"
    echo "  stop     优雅停止（TERM → 10s → KILL）"
    echo "  restart  重启"
    echo "  status   查看运行状态"
    echo ""
    echo "环境变量:"
    echo "  GOFLOW2_LISTEN     监听地址 (default sflow://:6343,netflow://:2055)"
    echo "  GOFLOW2_FORMAT     输出格式 (default json)"
    echo "  GOFLOW2_TRANSPORT  传输方式 (default file)"
    echo "  GOFLOW2_MAPPING    mapping.yaml 路径"
    echo "  GOFLOW2_LOGLEVEL   日志级别 (default info)"
    echo "  GOFLOW2_PID_FILE   pid 文件路径"
    echo "  GOFLOW2_LOG_FILE   日志文件路径"
    exit 1
}

[[ $# -lt 1 ]] && usage

case "$1" in
    start)    shift; do_start "$@" ;;
    stop)     shift; do_stop ;;
    restart)  shift; do_restart ;;
    status)   shift; do_status ;;
    -h|--help|help) usage ;;
    *) echo "unknown command: $1" >&2; usage ;;
esac