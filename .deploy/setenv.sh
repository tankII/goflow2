# 环境变量预配置文件（可选）
#
# 用法：编辑此文件，取消对应行的注释并填写即可生效。
#   例如：
#     [[ -z "${GOFLOW2_WORKERS:-}" ]] && export GOFLOW2_WORKERS=4
#     [[ -z "${GOFLOW2_FORMAT:-}" ]] && export GOFLOW2_FORMAT=csv
#     [[ -z "${GOFLOW2_TRANSPORT:-}" ]] && export GOFLOW2_TRANSPORT=netcat
#
# 注意：命令行环境变量优先级最高；当变量已由调用方设置时，本文件不会覆盖。
#       即：export VAR=val bash run.sh start  的 VAR 会保留，不受此文件影响。

# 采集监听地址，默认 sflow://:6343,netflow://:2055
# [[ -z "${GOFLOW2_LISTEN:-}" ]] && export GOFLOW2_LISTEN="sflow://:6343,netflow://:2055"

# 每个监听地址的 UDP socket 数；留空时保持 goflow2 原始默认（= 1）
# [[ -z "${GOFLOW2_COUNT:-}" ]] && export GOFLOW2_COUNT=2

# 每个监听地址的解码并行 worker 数；留空时保持 goflow2 原始默认（= count*2）
# [[ -z "${GOFLOW2_WORKERS:-}" ]] && export GOFLOW2_WORKERS=4

# 输出格式：json | csv | text | bin
# [[ -z "${GOFLOW2_FORMAT:-}" ]] && export GOFLOW2_FORMAT=json

# 传输方式：file | netcat | kafka
# [[ -z "${GOFLOW2_TRANSPORT:-}" ]] && export GOFLOW2_TRANSPORT=file

# 字段映射 YAML
# [[ -z "${GOFLOW2_MAPPING:-}" ]] && export GOFLOW2_MAPPING="/path/to/mapping.yaml"

# HTTP 状态端口
# [[ -z "${GOFLOW2_ADDR:-}" ]] && export GOFLOW2_ADDR=:18080

# 日志级别：debug | info | warn | error
# [[ -z "${GOFLOW2_LOGLEVEL:-}" ]] && export GOFLOW2_LOGLEVEL=info

# pid 文件路径（默认位于 logs/ 下）
# [[ -z "${GOFLOW2_PID_FILE:-}" ]] && export GOFLOW2_PID_FILE=/var/run/goflow2.pid

# ── 设备统计参数（可选，均有默认值，可不设则使用默认）───────────
# 统计端点前缀，默认 /device-stats（实际的 3 个端点：/device-list、
# <前缀>/hourly、<前缀>/daily）
# [[ -z "${GOFLOW2_DEVICE_STATS_PATH:-}" ]] && export GOFLOW2_DEVICE_STATS_PATH=/device-stats

# 小时桶保留时长，超过后不再出现在统计中，默认 72h
# [[ -z "${GOFLOW2_DEVICE_RETENTION_HOURLY:-}" ]] && export GOFLOW2_DEVICE_RETENTION_HOURLY=72h

# 日桶保留时长（goflow2 用 Go duration，不支持 d 单位，90 天写作 2160h），默认 2160h
# [[ -z "${GOFLOW2_DEVICE_RETENTION_DAILY:-}" ]] && export GOFLOW2_DEVICE_RETENTION_DAILY=2160h

# ── 日志参数（可选）────────────────────────────────────────────
# 默认不重定向：flow 记录 → stdout，slog 日志 → stderr（由 systemd/
# 调用方接管）。需要独立落盘（按大小轮转 + gzip）时设 GOFLOW2_LOG_PATH：
# [[ -z "${GOFLOW2_LOG_PATH:-}" ]] && export GOFLOW2_LOG_PATH=/var/log/goflow2-rotated.log

# 设了 GOFLOW2_LOG_PATH 时，是否仍同时输出到 stderr（1/true 开启）
# [[ -z "${GOFLOW2_LOG_ALSO_STDERR:-}" ]] && export GOFLOW2_LOG_ALSO_STDERR=0

# 日志附加 source 文件:行（排障用，1/true 开启）
# [[ -z "${GOFLOW2_LOG_SOURCE:-}" ]] && export GOFLOW2_LOG_SOURCE=1

# HTTP 访问日志（1/true 开启；会记录每个请求的 method/path/status/耗时）
# [[ -z "${GOFLOW2_LOG_HTTP:-}" ]] && export GOFLOW2_LOG_HTTP=1

# 轮转参数（仅设 LOG_PATH 时生效，均有默认值）
# [[ -z "${GOFLOW2_LOG_ROT_SIZE:-}" ]] && export GOFLOW2_LOG_ROT_SIZE=100
# [[ -z "${GOFLOW2_LOG_ROT_BACKUPS:-}" ]] && export GOFLOW2_LOG_ROT_BACKUPS=3
# [[ -z "${GOFLOW2_LOG_ROT_AGE:-}" ]] && export GOFLOW2_LOG_ROT_AGE=28
# [[ -z "${GOFLOW2_LOG_ROT_COMPRESS:-}" ]] && export GOFLOW2_LOG_ROT_COMPRESS=true

# ── file 传输参数（TRANSPORT=file 时生效，默认即 file）────────────
# 不设 GOFLOW2_FILE_PATH → flow 记录走 stdout（配合外层 systemd/管道）。

# flow 输出文件路径；留空则输出 stdout
# [[ -z "${GOFLOW2_FILE_PATH:-}" ]] && export GOFLOW2_FILE_PATH=/var/log/goflow2-flows.out

# 行分隔符，默认换行
# [[ -z "${GOFLOW2_FILE_SEP:-}" ]] && export GOFLOW2_FILE_SEP="\n"

# ── netcat 传输参数（TRANSPORT=netcat 时生效）────────────────────
# 布尔值用 1 或 true 表示开启；留空表示不传递该参数（使用 goflow2 默认值）。

# 远端目标列表（逗号分隔，轮询发送）
# [[ -z "${GOFLOW2_NETCAT_TARGETS:-}" ]] && export GOFLOW2_NETCAT_TARGETS="127.0.0.1:9995,127.0.0.1:9996"

# 行分隔符，默认换行
# [[ -z "${GOFLOW2_NETCAT_SEP:-}" ]] && export GOFLOW2_NETCAT_SEP="\n"

# 拨号超时（Go duration，如 3s），默认 3s；连接失败后重连间隔，默认 3s
# [[ -z "${GOFLOW2_NETCAT_DIAL_TIMEOUT:-}" ]] && export GOFLOW2_NETCAT_DIAL_TIMEOUT=3s
# [[ -z "${GOFLOW2_NETCAT_RECONNECT_INTERVAL:-}" ]] && export GOFLOW2_NETCAT_RECONNECT_INTERVAL=3s

# 每目标缓冲队列大小，默认 1024；队列满时是否丢弃新记录（默认丢弃，0/false 阻塞 Send）
# [[ -z "${GOFLOW2_NETCAT_QUEUE:-}" ]] && export GOFLOW2_NETCAT_QUEUE=1024
# [[ -z "${GOFLOW2_NETCAT_DROP:-}" ]] && export GOFLOW2_NETCAT_DROP=1

# ── Kafka 传输参数（TRANSPORT=kafka 时生效）──────────────────────
# 注意：GOFLOW2_TRANSPORT 需先设为 kafka 才会启用以下配置。
# 布尔值用 1 或 true 表示开启；留空或不设表示不传递该参数（使用 goflow2 默认值）。

# Kafka broker 地址列表（逗号分隔），必填
# [[ -z "${GOFLOW2_KAFKA_BROKERS:-}" ]] && export GOFLOW2_KAFKA_BROKERS="kafka-1.example.com:9093,kafka-2.example.com:9093"

# 输出 topic，默认 flow-messages
# [[ -z "${GOFLOW2_KAFKA_TOPIC:-}" ]] && export GOFLOW2_KAFKA_TOPIC="flow-messages"

# SASL 机制：none | plain | scram-sha256 | scram-sha512 | gssapi
# 默认 none；生产环境建议设为 gssapi（Kerberos）或 scram-sha256
# [[ -z "${GOFLOW2_KAFKA_SASL:-}" ]] && export GOFLOW2_KAFKA_SASL=gssapi

# 启用 TLS 连接（生产环境必须）
# [[ -z "${GOFLOW2_KAFKA_TLS:-}" ]] && export GOFLOW2_KAFKA_TLS=1

# plain / scram 认证（若 SASL=plain 或 scram-sha256/sha512 时使用）
# [[ -z "${GOFLOW2_KAFKA_SASL_USER:-}" ]] && export GOFLOW2_KAFKA_SASL_USER="goflow2"
# [[ -z "${GOFLOW2_KAFKA_SASL_PASS:-}" ]] && export GOFLOW2_KAFKA_SASL_PASS=""

# Kerberos (SASL/GSSAPI) 认证（若 SASL=gssapi 时使用）
# [[ -z "${GOFLOW2_KAFKA_KEYTAB:-}" ]] && export GOFLOW2_KAFKA_KEYTAB="/etc/krb5/goflow2.keytab"
# [[ -z "${GOFLOW2_KAFKA_REALM:-}" ]] && export GOFLOW2_KAFKA_REALM="EXAMPLE.COM"
# [[ -z "${GOFLOW2_KAFKA_SASL_USER:-}" ]] && export GOFLOW2_KAFKA_SASL_USER="goflow2"  # principal 短名
# [[ -z "${GOFLOW2_KAFKA_KERBEROS_CONFIG:-}" ]] && export GOFLOW2_KAFKA_KERBEROS_CONFIG="/etc/krb5.conf"
# [[ -z "${GOFLOW2_KAFKA_KERBEROS_SERVICE_NAME:-}" ]] && export GOFLOW2_KAFKA_KERBEROS_SERVICE_NAME="kafka"
# [[ -z "${GOFLOW2_KAFKA_KERBEROS_DISABLE_PAFXFAST:-}" ]] && export GOFLOW2_KAFKA_KERBEROS_DISABLE_PAFXFAST=1

# mTLS 客户端证书（若 SASL=none 但需要双向 TLS 认证时）
# [[ -z "${GOFLOW2_KAFKA_CLIENT_CERT:-}" ]] && export GOFLOW2_KAFKA_CLIENT_CERT="/etc/ssl/certs/goflow2.crt"
# [[ -z "${GOFLOW2_KAFKA_CLIENT_KEY:-}" ]] && export GOFLOW2_KAFKA_CLIENT_KEY="/etc/ssl/private/goflow2.key"
# [[ -z "${GOFLOW2_KAFKA_CA:-}" ]] && export GOFLOW2_KAFKA_CA="/etc/ssl/certs/ca.crt"

# 跳过 TLS 证书验证（仅测试环境使用）
# [[ -z "${GOFLOW2_KAFKA_TLS_INSECURE:-}" ]] && export GOFLOW2_KAFKA_TLS_INSECURE=1
