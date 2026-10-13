#!/usr/bin/env bash
# GoFlow2 启停脚本
# 用法: bash run.sh [start|stop|restart|status] [CMD ...]
#
# 环境变量:
#   GOFLOW2_LISTEN          监听地址，默认 sflow://:6343,netflow://:2055
#   GOFLOW2_COUNT           每个监听地址的 UDP socket 数（可选，追加 ?count=N）
#   GOFLOW2_WORKERS         每个监听地址的解码并行 worker 数（可选，追加 ?workers=N）
#   GOFLOW2_FORMAT          输出格式，默认 json
#   GOFLOW2_TRANSPORT       传输方式，默认 file
#   GOFLOW2_MAPPING         mapping.yaml 路径
#   GOFLOW2_LOGLEVEL        日志级别，默认 info
#   GOFLOW2_PID_FILE        pid 文件路径
#   GOFLOW2_TRANSPORT=file 时生效（见 setenv.sh 示例）
#     GOFLOW2_FILE_PATH      flow 输出文件（默认空 → stdout）
#     GOFLOW2_FILE_SEP       行分隔符，默认换行
#   GOFLOW2_TRANSPORT=netcat 时生效（见 setenv.sh 示例）
#     GOFLOW2_NETCAT_TARGETS  远端 host:port 列表（逗号分隔，轮询）
#     GOFLOW2_NETCAT_SEP      行分隔符，默认换行
#     GOFLOW2_NETCAT_DIAL_TIMEOUT / RECONNECT_INTERVAL / QUEUE / DROP
#   GOFLOW2_TRANSPORT=kafka 时生效的环境变量（见 setenv.sh 示例）
#     GOFLOW2_KAFKA_BROKERS      Kafka broker 地址列表
#     GOFLOW2_KAFKA_TOPIC        输出 topic，默认 flow-messages
#     GOFLOW2_KAFKA_SASL         SASL 机制：none/plain/scram-sha256/scram-sha512/gssapi
#     GOFLOW2_KAFKA_TLS          是否启用 TLS（1=true）
#     GOFLOW2_KAFKA_SASL_USER    SASL 用户名（plain/scram；或 Kerberos principal 短名）
#     GOFLOW2_KAFKA_SASL_PASS    SASL 密码（plain/scram）
#     GOFLOW2_KAFKA_KEYTAB       Kerberos keytab 路径
#     GOFLOW2_KAFKA_REALM        Kerberos realm
#     GOFLOW2_KAFKA_KERBEROS_SERVICE_NAME  Kerberos service name，默认 kafka
#     GOFLOW2_KAFKA_KERBEROS_CONFIG    krb5.conf 路径
#   设备统计（可选，均有默认值，可不设）
#     GOFLOW2_DEVICE_STATS_PATH       统计端点前缀，默认 /device-stats
#     GOFLOW2_DEVICE_RETENTION_HOURLY 小时桶保留时长，默认 72h
#     GOFLOW2_DEVICE_RETENTION_DAILY  日桶保留时长，默认 2160h（90 天）
#   日志（可选，默认 flow→stdout、slog→stderr，不重定向）
#     GOFLOW2_LOG_PATH       额外写一份到该文件（-log.file，带大小轮转）
#     GOFLOW2_LOG_ALSO_STDERR 设了 GOFLOW2_LOG_PATH 时仍镜像到 stderr (1/true)
#     GOFLOW2_LOG_SOURCE      日志附 source 文件:行 (1/true)
#     GOFLOW2_LOG_HTTP        HTTP 访问日志 (1/true)
#     GOFLOW2_LOG_ROT_SIZE    单文件大小上限 MB (默认 100)
#     GOFLOW2_LOG_ROT_BACKUPS 保留旧文件个数 (默认 3)
#     GOFLOW2_LOG_ROT_AGE     保留天数 (默认 28)
#     GOFLOW2_LOG_ROT_COMPRESS 旧文件 gzip 压缩 (默认 true，0/false 关闭)
set -euo pipefail

# 目录布局（解压后即刻可用）：
#   包根/
#   ├── run.sh        (启停脚本)
#   ├── setenv.sh     (环境变量预配置，可选)
#   ├── bin/          (采集器二进制)
#   └── conf/         (mapping.yaml)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="${SCRIPT_DIR}/bin"
CONF_DIR="${SCRIPT_DIR}/conf"
LOG_DIR="${SCRIPT_DIR}/logs"

# 读取可选的环境变量预配置
if [[ -r "${SCRIPT_DIR}/setenv.sh" ]]; then
    # shellcheck disable=SC1090
    . "${SCRIPT_DIR}/setenv.sh"
fi

MAPPING="${GOFLOW2_MAPPING:-${CONF_DIR}/mapping.yaml}"
LISTEN="${GOFLOW2_LISTEN:-sflow://:6343,netflow://:2055}"

# 修改 URL query：追加或覆写指定参数
_modify_query() {
    local addr="$1" param="$2" val="$3"
    local pattern="([?&])${param}=(0|[1-9][0-9]*)"
    if [[ "$addr" =~ $pattern ]]; then
        # 已有该参数 → 覆写值
        addr="${addr%%${BASH_REMATCH[0]}*}${BASH_REMATCH[1]}${param}=${val}${addr#*${BASH_REMATCH[0]}}"
    elif [[ "$addr" == *"?"* ]]; then
        # 有其他 query 参数 → 追加
        addr="${addr}&${param}=${val}"
    else
        # 无 query → 新增
        addr="${addr}?${param}=${val}"
    fi
    echo "$addr"
}

if [[ -n "${GOFLOW2_COUNT:-}" || -n "${GOFLOW2_WORKERS:-}" ]]; then
    parts=""
    IFS=',' read -ra addrs <<< "$LISTEN"
    for addr in "${addrs[@]}"; do
        if [[ -n "${GOFLOW2_COUNT:-}" ]]; then
            addr="$(_modify_query "$addr" "count" "$GOFLOW2_COUNT")"
        fi
        if [[ -n "${GOFLOW2_WORKERS:-}" ]]; then
            addr="$(_modify_query "$addr" "workers" "$GOFLOW2_WORKERS")"
        fi
        parts="${parts}${parts:+,}${addr}"
    done
    LISTEN="$parts"
fi
FORMAT="${GOFLOW2_FORMAT:-json}"
TRANSPORT="${GOFLOW2_TRANSPORT:-file}"
LOGLEVEL="${GOFLOW2_LOGLEVEL:-info}"
ADDR="${GOFLOW2_ADDR:-:18080}"
PID_FILE="${GOFLOW2_PID_FILE:-${LOG_DIR}/goflow2.pid}"

# ── Kafka 参数构建（当 TRANSPORT=kafka 时生效）──────────────────────
# 将 GOFLOW2_KAFKA_* 环境变量转换为 -transport.kafka.* 命令行参数。
# 布尔型参数（值为 1 或 true 时）输出为无值 flag；其他输出为 key=value。
_build_kafka_args() {
    local args=""
    _kf_append() {
        local key="$1" val="$2"
        if [[ -n "$val" ]]; then
            # 布尔型：1/true → flag；其他 → flag value
            if [[ "$val" == "1" || "$val" == "true" ]]; then
                args="${args} ${key}"
            else
                args="${args} ${key} ${val}"
            fi
        fi
    }
    _kf_append "-transport.kafka.brokers"      "${GOFLOW2_KAFKA_BROKERS:-}"
    _kf_append "-transport.kafka.topic"        "${GOFLOW2_KAFKA_TOPIC:-}"
    _kf_append "-transport.kafka.sasl"         "${GOFLOW2_KAFKA_SASL:-}"
    _kf_append "-transport.kafka.tls"          "${GOFLOW2_KAFKA_TLS:-}"
    _kf_append "-transport.kafka.tls.client"   "${GOFLOW2_KAFKA_CLIENT_CERT:-}"
    _kf_append "-transport.kafka.tls.key"      "${GOFLOW2_KAFKA_CLIENT_KEY:-}"
    _kf_append "-transport.kafka.tls.ca"       "${GOFLOW2_KAFKA_CA:-}"
    _kf_append "-transport.kafka.tls.insecure" "${GOFLOW2_KAFKA_TLS_INSECURE:-}"
    # plain/scram 认证（KAFKA_SASL_USER/PASS 已由 goflow2 原生读取，此处也支持通过环境变量注入）
    if [[ -n "${GOFLOW2_KAFKA_SASL_USER:-}" || -n "${GOFLOW2_KAFKA_SASL_PASS:-}" ]]; then
        export KAFKA_SASL_USER="${GOFLOW2_KAFKA_SASL_USER:-}"
        export KAFKA_SASL_PASS="${GOFLOW2_KAFKA_SASL_PASS:-}"
    fi
    # Kerberos 相关
    _kf_append "-transport.kafka.sasl.kerberos.keytab"      "${GOFLOW2_KAFKA_KEYTAB:-}"
    _kf_append "-transport.kafka.sasl.kerberos.username"    "${GOFLOW2_KAFKA_SASL_USER:-}"
    _kf_append "-transport.kafka.sasl.kerberos.realm"       "${GOFLOW2_KAFKA_REALM:-}"
    _kf_append "-transport.kafka.sasl.kerberos.service-name" "${GOFLOW2_KAFKA_KERBEROS_SERVICE_NAME:-}"
    _kf_append "-transport.kafka.sasl.kerberos.config"      "${GOFLOW2_KAFKA_KERBEROS_CONFIG:-}"
    _kf_append "-transport.kafka.sasl.kerberos.disable-paf-x-fast" "${GOFLOW2_KAFKA_KERBEROS_DISABLE_PAFXFAST:-}"
    echo "$args"
}

# ── file/netcat 传输参数构建（分别当 TRANSPORT=file / netcat 时生效）──
# 字符串/数值型：flag key value；布尔型（drop）用 key=value 形式。
_build_transport_args() {
    local args=""
    _t_append() {
        local key="$1" val="$2"
        [[ -n "$val" ]] && args="${args} ${key} ${val}"
    }
    _t_bool() {
        local key="$1" val="$2"
        case "$val" in
            1|true)       args="${args} ${key}" ;;
            0|false)      args="${args} ${key}=${val}" ;;
            "")           ;; # 未设置 → 跳过（用默认值）
            *)            args="${args} ${key}=${val}" ;;
        esac
    }
    if [[ "$TRANSPORT" == "file" ]]; then
        _t_append "-transport.file"    "${GOFLOW2_FILE_PATH:-}"
        _t_append "-transport.file.sep" "${GOFLOW2_FILE_SEP:-}"
    fi
    if [[ "$TRANSPORT" == "netcat" ]]; then
        _t_append "-transport.netcat.targets"            "${GOFLOW2_NETCAT_TARGETS:-}"
        _t_append "-transport.netcat.sep"                "${GOFLOW2_NETCAT_SEP:-}"
        _t_append "-transport.netcat.dial.timeout"       "${GOFLOW2_NETCAT_DIAL_TIMEOUT:-}"
        _t_append "-transport.netcat.reconnect.interval" "${GOFLOW2_NETCAT_RECONNECT_INTERVAL:-}"
        _t_append "-transport.netcat.queue"              "${GOFLOW2_NETCAT_QUEUE:-}"
        _t_bool  "-transport.netcat.drop"                "${GOFLOW2_NETCAT_DROP:-}"
    fi
    echo "$args"
}

# ── 设备统计参数构建（可选，均有默认值）─────────────────────
_build_device_stats_args() {
    local args=""
    if [[ -n "${GOFLOW2_DEVICE_STATS_PATH:-}" ]]; then
        args="${args} -device.stats.path ${GOFLOW2_DEVICE_STATS_PATH}"
    fi
    if [[ -n "${GOFLOW2_DEVICE_RETENTION_HOURLY:-}" ]]; then
        args="${args} -device.retention.hourly ${GOFLOW2_DEVICE_RETENTION_HOURLY}"
    fi
    if [[ -n "${GOFLOW2_DEVICE_RETENTION_DAILY:-}" ]]; then
        args="${args} -device.retention.daily ${GOFLOW2_DEVICE_RETENTION_DAILY}"
    fi
    if [[ -n "${GOFLOW2_QUEUE_STATS_PATH:-}" ]]; then
        args="${args} -queue.stats.path ${GOFLOW2_QUEUE_STATS_PATH}"
    fi
    echo "$args"
}

# ── 日志参数构建（可选，默认仅 stderr 重定向）───────────────────
# 布尔型环境变量：1/true → 无值 flag；0/false → key=false；其他值 → key=值
_build_log_args() {
    local args=""
    _lg_bool() {
        local key="$1" val="$2"
        case "$val" in
            1|true)       args="${args} ${key}" ;;
            0|false)      args="${args} ${key}=${val}" ;;
            "")           ;; # 未设置 → 跳过
            *)            args="${args} ${key}=${val}" ;;
        esac
    }
    if [[ -n "${GOFLOW2_LOG_PATH:-}" ]]; then
        args="${args} -log.file ${GOFLOW2_LOG_PATH}"
    fi
    _lg_bool "-log.also-stderr"   "${GOFLOW2_LOG_ALSO_STDERR:-}"
    _lg_bool "-log.source"        "${GOFLOW2_LOG_SOURCE:-}"
    _lg_bool "-log.http"          "${GOFLOW2_LOG_HTTP:-}"
    _lg_bool "-log.file.compress" "${GOFLOW2_LOG_ROT_COMPRESS:-}"
    if [[ -n "${GOFLOW2_LOG_ROT_SIZE:-}" ]]; then
        args="${args} -log.file.max-size ${GOFLOW2_LOG_ROT_SIZE}"
    fi
    if [[ -n "${GOFLOW2_LOG_ROT_BACKUPS:-}" ]]; then
        args="${args} -log.file.max-backups ${GOFLOW2_LOG_ROT_BACKUPS}"
    fi
    if [[ -n "${GOFLOW2_LOG_ROT_AGE:-}" ]]; then
        args="${args} -log.file.max-age ${GOFLOW2_LOG_ROT_AGE}"
    fi
    echo "$args"
}

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
            echo "goflow2 already running (pid=$PID). stop first: bash run.sh stop" >&2
            exit 1
        fi
        rm -f "$PID_FILE"
    fi

    mkdir -p "$LOG_DIR"
    local BINARY
    BINARY=$(pick_binary)
    echo ">>> starting goflow2 ..."
    echo "    binary : $BINARY"
    echo "    mapping: $MAPPING"
    echo "    listen : $LISTEN"
    if [[ -n "${GOFLOW2_COUNT:-}" ]]; then
        echo "    count    : $GOFLOW2_COUNT (socket per listen address)"
    fi
    if [[ -n "${GOFLOW2_WORKERS:-}" ]]; then
        echo "    workers: $GOFLOW2_WORKERS (per listen address)"
    fi
    echo "    format : $FORMAT"
    echo "    transport: $TRANSPORT"
    local KAFKA_ARGS=""
    if [[ "$TRANSPORT" == "kafka" ]]; then
        KAFKA_ARGS="$(_build_kafka_args)"
        if [[ -n "$KAFKA_ARGS" ]]; then
            echo "    kafka args :${KAFKA_ARGS}"
        fi
    fi
    local TRANSPORT_ARGS
    TRANSPORT_ARGS="$(_build_transport_args)"
    if [[ -n "$TRANSPORT_ARGS" ]]; then
        echo "    transport args:${TRANSPORT_ARGS}"
    fi
    local DEVICE_ARGS
    DEVICE_ARGS="$(_build_device_stats_args)"
    if [[ -n "$DEVICE_ARGS" ]]; then
        echo "    device stats:${DEVICE_ARGS}"
    fi
    local LOG_ARGS
    LOG_ARGS="$(_build_log_args)"
    if [[ -n "$LOG_ARGS" ]]; then
        echo "    log args  :${LOG_ARGS}"
    fi
    echo "    addr     : $ADDR"
    echo "    loglevel: $LOGLEVEL"
    echo "    pid_file: $PID_FILE"

    # 解析用户传入的额外参数：-- 之后的部分直接透传给 goflow2
    local USER_ARGS=""
    local after_sep=false
    for arg in "$@"; do
        if [[ "$arg" == "--" ]]; then
            after_sep=true
            continue
        fi
        if $after_sep; then
            USER_ARGS="${USER_ARGS} ${arg}"
        fi
    done

    exec "$BINARY" \
        -listen "$LISTEN" \
        -format "$FORMAT" \
        -transport "$TRANSPORT" \
        -mapping "$MAPPING" \
        -loglevel "$LOGLEVEL" \
        -addr "$ADDR" \
        ${KAFKA_ARGS:-} \
        ${TRANSPORT_ARGS:-} \
        ${DEVICE_ARGS:-} \
        ${LOG_ARGS:-} \
        ${USER_ARGS:-} \
        < /dev/null &
    local PID=$!
    echo "$PID" > "$PID_FILE"
    echo ">>> started pid=$PID"
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
    echo "Usage: bash run.sh [start|stop|restart|status] [-- <goflow2 args>]"
    echo ""
    echo "Commands:"
    echo "  start    启动采集器（后台运行，写入 pid/log）"
    echo "  stop     优雅停止（TERM → 10s → KILL）"
    echo "  restart  重启"
    echo "  status   查看运行状态"
    echo ""
    echo "环境变量:"
    echo "  GOFLOW2_LISTEN     监听地址 (default sflow://:6343,netflow://:2055)"
    echo "  GOFLOW2_COUNT      UDP socket 数：每个监听地址 socket 数（追加 ?count=N，默认无）"
    echo "  GOFLOW2_WORKERS    解码并行度：每个监听地址 worker 数（追加 ?workers=N，默认无）"
    echo "  GOFLOW2_FORMAT     输出格式 (default json)"
    echo "  GOFLOW2_TRANSPORT  传输方式 (default file)"
    echo "  GOFLOW2_MAPPING    mapping.yaml 路径"
    echo "  GOFLOW2_LOGLEVEL   日志级别 (default info)"
    echo "  GOFLOW2_PID_FILE   pid 文件路径"
    echo "  GOFLOW2_DEVICE_STATS_PATH       设备统计端点前缀 (default /device-stats)"
    echo "  GOFLOW2_DEVICE_RETENTION_HOURLY 小时桶保留时长 (default 72h)"
    echo "  GOFLOW2_DEVICE_RETENTION_DAILY  日桶保留时长 (default 2160h=90天)
  GOFLOW2_QUEUE_STATS_PATH        队列监控端点路径 (default /queue-stats)"
    echo "  GOFLOW2_LOG_PATH      额外日志文件（-log.file，带大小轮转）"
    echo "  GOFLOW2_LOG_ALSO_STDERR 设了 LOG_PATH 时同时输出 stderr (1/true)"
    echo "  GOFLOW2_LOG_SOURCE    日志附 source (1/true)"
    echo "  GOFLOW2_LOG_HTTP      HTTP 访问日志 (1/true)"
    echo "  GOFLOW2_LOG_ROT_SIZE    单文件大小上限 MB (default 100)"
    echo "  GOFLOW2_LOG_ROT_BACKUPS 保留旧文件数 (default 3)"
    echo "  GOFLOW2_LOG_ROT_AGE     保留天数 (default 28)"
    echo "  GOFLOW2_LOG_ROT_COMPRESS 旧文件 gzip (default true)"
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