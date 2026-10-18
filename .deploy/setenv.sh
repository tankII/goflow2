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

# 每个监听地址的接收队列容量（每包约占 9KB，容量×9KB ≈ 内存占用）；
# 留空时保持 goflow2 原始默认：非阻塞时 1000000（≈9GB，注意 OOM）、blocking 时 0
# [[ -z "${GOFLOW2_QUEUE_SIZE:-}" ]] && export GOFLOW2_QUEUE_SIZE=100000

# 每个监听地址是否同步阻塞接收：1/true 开启，0/false 关闭。
# 开启后不丢包，但接收协程会阻塞，且 queue_size 失效（同步模式容量恒 0）。
# 留空＝不传该参数（goflow2 默认非阻塞、满则丢）
# [[ -z "${GOFLOW2_BLOCKING:-}" ]] && export GOFLOW2_BLOCKING=1

# 输出格式：json | csv | text | bin
# [[ -z "${GOFLOW2_FORMAT:-}" ]] && export GOFLOW2_FORMAT=json

# 传输方式：file | netcat | kafka
# [[ -z "${GOFLOW2_TRANSPORT:-}" ]] && export GOFLOW2_TRANSPORT=file

# 字段映射 YAML
# [[ -z "${GOFLOW2_MAPPING:-}" ]] && export GOFLOW2_MAPPING="/path/to/mapping.yaml"

# 时区（全局）：影响时间字段渲染（mapping.render 的 datetime/datetimenano）
# 与 /device-stats 按小时/按天分桶。默认 UTC；设了 TZ 则用本地时间。
# 未显式指定时默认使用本地时区；调用方已设 TZ 时以调用方为准。
[[ -z "${TZ:-}" ]] && export TZ=Asia/Shanghai

# HTTP 状态端口
# [[ -z "${GOFLOW2_ADDR:-}" ]] && export GOFLOW2_ADDR=:18080

# 日志级别：debug | info | warn | error
# [[ -z "${GOFLOW2_LOGLEVEL:-}" ]] && export GOFLOW2_LOGLEVEL=info

# pid 文件路径（默认位于 logs/ 下）
# [[ -z "${GOFLOW2_PID_FILE:-}" ]] && export GOFLOW2_PID_FILE=/var/run/goflow2.pid

# ── 全局统计开关（可选，默认开启）─────────────────────────────
# 关闭后：不发射任何 Prometheus 指标（-metrics=false）、不做设备统计聚合；
# /device-list、<前缀>/hourly、<前缀>/daily 仍返回 200 但为空数据；
# 接收队列 /queue-stats、发送队列 /transport-stats 与 /ui 面板照常。
# [[ -z "${GOFLOW2_METRICS:-}" ]] && export GOFLOW2_METRICS=0

# ── 性能剖析端点（可选，默认关闭）──────────────────────────────
# 开启后在状态端口（-addr，默认 :18080）暴露 /debug/pprof/（CPU、heap、
# goroutine 等），用于排查吞吐/CPU 问题；端点无鉴权，仅排查期间开启，用完关闭。
# [[ -z "${GOFLOW2_PPROF:-}" ]] && export GOFLOW2_PPROF=1

# ── 模板 / 采样率 TTL（可选，默认 0＝永不过期）──────────────────
# 不设置时 goflow2 默认 templates.ttl=0、sampling.ttl=0，即模板与采样率条目
# 永不清理：长期运行会随「设备数 × 观测域 × 模板 ID」持续累积，并放大模板查询
# 的锁竞争（表现为跑久了 CPU 逐步上升、flow_process_nf_template_entries 单调增长）。
# NetFlow v9 / IPFIX 模板的标准刷新周期为 60s，建议设为 1h，留足余量。
# 取值过小会反复丢失模板并触发 fallback 解码，请勿低于刷新周期。
# [[ -z "${GOFLOW2_TEMPLATES_TTL:-}" ]] && export GOFLOW2_TEMPLATES_TTL=1h
# [[ -z "${GOFLOW2_SAMPLING_TTL:-}" ]] && export GOFLOW2_SAMPLING_TTL=1h
# 访问模板时是否续期（默认关闭）。长连接且模板持续刷新的场景可开启。
# [[ -z "${GOFLOW2_TEMPLATES_TTL_EXTEND_ON_ACCESS:-}" ]] && export GOFLOW2_TEMPLATES_TTL_EXTEND_ON_ACCESS=1
# [[ -z "${GOFLOW2_SAMPLING_TTL_EXTEND_ON_ACCESS:-}" ]] && export GOFLOW2_SAMPLING_TTL_EXTEND_ON_ACCESS=1

# ── Go 运行时调优（可选）────────────────────────────────────
# 背景：现网实测分配速率约 377 MiB/s、GC 673 次/分钟，且"堆只涨不降"
#（Go 未及时把页归还 OS），内存会单调增长直至内核 OOM kill。
# GOGC 调低 → GC 更积极 → 内存下降、CPU 上升（用 CPU 换内存）。
# GOMEMLIMIT 为软上限：接近时 Go 自动加大 GC 力度，而非任由堆膨胀。
# 这两项是 Go 运行时环境变量，不需要重新编译；run.sh 会在启动前透传。
#
# GOGC      默认 100；40 约可把分配速率与 GC 频率减半
# GOMEMLIMIT 建议设为整机内存的 60~70%，如 16GiB 机器设 10GiB
# [[ -z "${GOGC:-}" ]] && export GOGC=40
# [[ -z "${GOMEMLIMIT:-}" ]] && export GOMEMLIMIT=2GiB

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

# 批量写：记录先累积进写缓冲，缓冲写满或到达该间隔才下发给 socket，
# 从而把"每条记录一次 write 系统调用"合并为"每 64KiB 一次"。
# 延迟上限默认 200ms；下游为 flume/HDFS 等汇聚链路时可放宽到 1s 以上。
# 每目标写缓冲大小，默认 65536（字节）。
# [[ -z "${GOFLOW2_NETCAT_FLUSH_INTERVAL:-}" ]] && export GOFLOW2_NETCAT_FLUSH_INTERVAL=200ms
# [[ -z "${GOFLOW2_NETCAT_WRITE_BUFFER:-}" ]] && export GOFLOW2_NETCAT_WRITE_BUFFER=65536

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
# 队列监控端点路径（默认 /queue-stats）
# [[ -z "${GOFLOW2_QUEUE_STATS_PATH:-}" ]] && export GOFLOW2_QUEUE_STATS_PATH=/queue-stats

# 发送队列监控端点路径（默认 /transport-stats；仅 netcat 传输有数据）
# [[ -z "${GOFLOW2_TRANSPORT_STATS_PATH:-}" ]] && export GOFLOW2_TRANSPORT_STATS_PATH=/transport-stats

# Web 监控面板路径（默认 /ui；置空则禁用它）
# [[ -z "${GOFLOW2_WEB_PATH:-}" ]] && export GOFLOW2_WEB_PATH=/ui
