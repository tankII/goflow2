# GoFlow2 部署包说明

## 目录结构

```
goflow2-deploy-<version>-<date>/
├── bin/
│   ├── goflow2-linux-amd64       # x86_64 采集器
│   ├── goflow2-linux-arm64       # ARM64 采集器
│   ├── enricher-goflow2-linux-amd64
│   └── enricher-goflow2-linux-arm64
├── conf/
│   └── mapping.yaml              # 字段映射配置（可替换）
├── logs/                         # 运行时 pid
├── run.sh                        # 启停脚本（start/stop/restart/status）
├── setenv.sh                     # 环境变量预配置（可选）
└── README.md                     # 本文档
```

## 快速开始

```bash
# 解压
tar xzf goflow2-deploy-*.tar.gz
cd goflow2-deploy-*

# 启动（默认用 conf/mapping.yaml）
bash run.sh start

# 指定 mapping
GOFLOW2_MAPPING=/path/to/your/mapping.yaml bash run.sh start

# 查看状态
bash run.sh status

# 停止
bash run.sh stop

# 重启
bash run.sh restart
```

## 环境变量预配置

包根下的 `setenv.sh` 用于固定启动参数，取消注释并填写即可。命令行传入的变量仍优先级更高。

```bash
# 编辑部署包根目录下的 setenv.sh
vim run.sh 旁边的 setenv.sh
bash run.sh start
```

## 关键环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `GOFLOW2_LISTEN` | `sflow://:6343,netflow://:2055` | 采集监听地址 |
| `GOFLOW2_COUNT` | 无 | 每个监听地址的 UDP socket 数（自动追加 `?count=N`，默认 1） |
| `GOFLOW2_WORKERS` | 无 | 解码并行度：每个监听地址的 worker 数（自动追加 `?workers=N`，默认 = count×2） |
| `GOFLOW2_FORMAT` | `json` | 输出格式：`json` / `csv` / `text` / `bin` |
| `GOFLOW2_TRANSPORT` | `file` | 传输方式：`file` / `kafka` / `netcat` |
| `GOFLOW2_FILE_PATH` | 无 | flow 输出文件路径（`TRANSPORT=file` 时；留空 → stdout） |
| `GOFLOW2_FILE_SEP` | `\n` | 行分隔符（`TRANSPORT=file`） |
| `GOFLOW2_NETCAT_TARGETS` | 无 | 远端 host:port 列表（逗号分隔，轮询；`TRANSPORT=netcat` 时必填） |
| `GOFLOW2_NETCAT_SEP` | `\n` | 行分隔符（`TRANSPORT=netcat`） |
| `GOFLOW2_NETCAT_DIAL_TIMEOUT` | `3s` | 拨号超时 |
| `GOFLOW2_NETCAT_RECONNECT_INTERVAL` | `3s` | 连接失败后重连间隔 |
| `GOFLOW2_NETCAT_QUEUE` | `1024` | 每目标缓冲队列大小 |
| `GOFLOW2_NETCAT_DROP` | `1` | 队列满时丢弃新记录（`0` 则阻塞 Send） |
| `GOFLOW2_KAFKA_BROKERS` | 无 | Kafka broker 地址（逗号分隔，`TRANSPORT=kafka` 时必填） |
| `GOFLOW2_KAFKA_TOPIC` | `flow-messages` | Kafka topic |
| `GOFLOW2_KAFKA_SASL` | `none` | SASL 机制：`none` / `plain` / `scram-sha256` / `scram-sha512` / `gssapi` |
| `GOFLOW2_KAFKA_TLS` | 无 | 启用 TLS（设 `1`） |
| `GOFLOW2_KAFKA_SASL_USER` | 无 | SASL 用户名 / Kerberos principal 短名 |
| `GOFLOW2_KAFKA_SASL_PASS` | 无 | SASL 密码（plain/scram） |
| `GOFLOW2_KAFKA_KEYTAB` | 无 | Kerberos keytab 路径（`SASL=gssapi` 时必填） |
| `GOFLOW2_KAFKA_REALM` | 无 | Kerberos realm |
| `GOFLOW2_KAFKA_KERBEROS_CONFIG` | 无 | krb5.conf 路径 |
| `GOFLOW2_MAPPING` | `conf/mapping.yaml` | 字段映射配置文件路径 |
| `GOFLOW2_LOGLEVEL` | `info` | 日志级别：`debug` / `info` / `warn` / `error` |
| `GOFLOW2_PID_FILE` | `logs/goflow2.pid` | pid 文件路径 |
| `GOFLOW2_LOG_PATH` | 无 | 额外日志文件（`-log.file`，带大小轮转 + gzip） |
| `GOFLOW2_LOG_ALSO_STDERR` | `0` | 设了 `LOG_PATH` 时仍镜像到 stderr（设 `1`/`true`） |
| `GOFLOW2_LOG_SOURCE` | `0` | 日志附 source 文件:行（设 `1`/`true`） |
| `GOFLOW2_LOG_HTTP` | `0` | HTTP 访问日志（设 `1`/`true`） |
| `GOFLOW2_LOG_ROT_SIZE` | `100` | 单日志文件大小上限 MB（`0` 关闭） |
| `GOFLOW2_LOG_ROT_BACKUPS` | `3` | 保留旧文件数 |
| `GOFLOW2_LOG_ROT_AGE` | `28` | 保留天数 |
| `GOFLOW2_LOG_ROT_COMPRESS` | `true` | 旧文件 gzip 压缩（设 `0`/`false` 关闭） |
| `GOFLOW2_DEVICE_STATS_PATH` | `/device-stats` | 设备统计端点前缀 |
| `GOFLOW2_DEVICE_RETENTION_HOURLY` | `72h` | 小时桶保留时长 |
| `GOFLOW2_DEVICE_RETENTION_DAILY` | `2160h` | 日桶保留时长（Go duration，90 天写作 `2160h`） |

## 示例：设备统计查询

采集器按来源设备（router IP）聚合统计，开箱即用（有默认值，无需额外配置）。端点挂在 HTTP 状态端口（`GOFLOW2_ADDR`，默认 `:18080`）：

```bash
# 设备及其总量（UDP 报文数 / flow 记录数 / 最后活跃时间，本地时区）
curl http://127.0.0.1:18080/device-list

# 按小时 / 按日聚合（最近 1 小时桶 / 当日桶；无数据时返回 404）
curl "http://127.0.0.1:18080/device-stats/hourly?router=127.0.0.1"
curl "http://127.0.0.1:18080/device-stats/daily?router=127.0.0.1"
```

在 Prometheus 侧可抓取 `/metrics`，其中 `goflow2_device_*` 系列（`device_active_count`、`device_last_seen_timestamp_seconds{router}`、`device_packets_total{router}`、`device_flows_total{router}`）实时反映设备活跃度。

## 示例：日志轮转与调试

默认**不重定向**：flow 记录走 `stdout`，slog 日志走 `stderr`，由调用方（systemd/journald、管道等）接管。需要“按大小轮转 + gzip 归档”的独立日志时，设置 `GOFLOW2_LOG_PATH` —— goflow2 会额外写一份到该文件，并按以下规则轮转（`run.sh start` 的回显里会出现 `log args`）：

```bash
GOFLOW2_LOG_PATH=/var/log/goflow2-rot.log \
GOFLOW2_LOG_ROT_SIZE=100 \
GOFLOW2_LOG_ROT_BACKUPS=3 \
GOFLOW2_LOG_ROT_AGE=28 \
GOFLOW2_LOG_ROT_COMPRESS=true \
bash run.sh start
```

排障时建议开启：

```bash
GOFLOW2_LOG_SOURCE=1 \   # 每条日志带 source 文件:行，便于定位代码
GOFLOW2_LOG_HTTP=1 \     # 记录 HTTP 请求（method/path/status/耗时）
GOFLOW2_LOGLEVEL=debug   # 完整输出
bash run.sh start
```

启动时 goflow2 会输出一行 `goflow2 startup` 摘要，含监听地址、HTTP 端口、格式、传输、设备统计与轮转参数，便于核对当前生效配置。

## 示例：CSV + netcat 负载分担

```bash
GOFLOW2_FORMAT=csv \
GOFLOW2_TRANSPORT=netcat \
GOFLOW2_NETCAT_TARGETS="10.0.0.5:9995,10.0.0.6:9995" \
GOFLOW2_NETCAT_DIAL_TIMEOUT=3s \
GOFLOW2_MAPPING=conf/mapping.yaml \
bash run.sh start -- \
  -format.csv.delimiter "|" \
  -transport.netcat.targets 10.0.0.1:9001,10.0.0.2:9001
```

## 示例：Kafka Kerberos

```bash
# 方式一：通过环境变量（推荐，可写入 setenv.sh）
GOFLOW2_TRANSPORT=kafka \
GOFLOW2_KAFKA_BROKERS="kafka-1.example.com:9093,kafka-2.example.com:9093" \
GOFLOW2_KAFKA_SASL=gssapi \
GOFLOW2_KAFKA_TLS=1 \
GOFLOW2_KAFKA_SASL_USER=goflow2 \
GOFLOW2_KAFKA_KEYTAB=/etc/krb5/goflow2.keytab \
GOFLOW2_KAFKA_REALM=EXAMPLE.COM \
bash run.sh start

# 方式二：命令行参数（覆盖 setenv.sh 中的设置）
GOFLOW2_TRANSPORT=kafka bash run.sh start -- \
  -transport.kafka.brokers kafka-1.example.com:9093,kafka-2.example.com:9093 \
  -transport.kafka.tls \
  -transport.kafka.sasl gssapi \
  -transport.kafka.sasl.kerberos.username goflow2 \
  -transport.kafka.sasl.kerberos.keytab /etc/krb5/goflow2.keytab \
  -transport.kafka.sasl.kerberos.realm EXAMPLE.COM
```

## 从源码构建部署包

```bash
make package-deploy ARCH=all   # linux-amd64 + linux-arm64
# 产物在 dist/goflow2-deploy-<version>-<date>.tar.gz
```