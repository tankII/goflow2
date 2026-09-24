#!/usr/bin/env bash
# GoFlow2 启动脚本
# 用法: bash run.sh [CMD ...]
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

# 变量入口，可被外部覆盖
MAPPING="${GOFLOW2_MAPPING:-${CONF_DIR}/mapping.yaml}"
LISTEN="${GOFLOW2_LISTEN:-sflow://:6343,netflow://:2055}"
FORMAT="${GOFLOW2_FORMAT:-json}"
TRANSPORT="${GOFLOW2_TRANSPORT:-file}"
LOGLEVEL="${GOFLOW2_LOGLEVEL:-info}"
PID_FILE="${GOFLOW2_PID_FILE:-${LOG_DIR}/goflow2.pid}"
LOG_FILE="${GOFLOW2_LOG_FILE:-${LOG_DIR}/goflow2.log}"

check_deps() {
    if [[ ! -x "${BIN_DIR}/goflow2-linux-amd64" && ! -x "${BIN_DIR}/goflow2-linux-arm64" ]]; then
        echo "ERROR: no binary found in ${BIN_DIR}" >&2
        exit 1
    fi
}

pick_binary() {
    if [[ -x "${BIN_DIR}/goflow2-linux-amd64" ]]; then
        echo "${BIN_DIR}/goflow2-linux-amd64"
    elif [[ -x "${BIN_DIR}/goflow2-linux-arm64" ]]; then
        echo "${BIN_DIR}/goflow2-linux-arm64"
    fi
}

main() {
    check_deps
    local BINARY
    BINARY=$(pick_binary)

    if [[ -f "$PID_FILE" ]]; then
        local PID
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "goflow2 already running (pid=$PID). stop first: bash stop.sh" >&2
            exit 1
        fi
        rm -f "$PID_FILE"
    fi

    mkdir -p "$(dirname "$LOG_FILE")" "$LOG_DIR"
    echo ">>> starting goflow2 ..."
    echo "    binary : $BINARY"
    echo "    mapping: $MAPPING"
    echo "    listen : $LISTEN"
    echo "    format : $FORMAT"
    echo "    transport: $TRANSPORT"
    echo "    loglevel: $LOGLEVEL"
    echo "    pid_file: $PID_FILE"
    echo "    log_file: $LOG_FILE"

    exec "$BINARY" \
        -listen "$LISTEN" \
        -format "$FORMAT" \
        -transport "$TRANSPORT" \
        -mapping "$MAPPING" \
        -loglevel "$LOGLEVEL" \
        >"$LOG_FILE" 2>&1 &
    local PID=$!
    echo "$PID" > "$PID_FILE"
    echo ">>> started pid=$PID log=$LOG_FILE"
}

main "$@"