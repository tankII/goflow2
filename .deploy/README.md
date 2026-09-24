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
├── logs/                         # 运行时日志、pid
├── run.sh                        # 启动脚本
├── stop.sh                       # 停止脚本
├── build.sh                      # 重新构建脚本（本地使用）
└── README.md                     # 本文档
```

## 快速开始

```bash
# 解压
tar xzf goflow2-deploy-*.tar.gz
cd goflow2-deploy-*

# 启动（默认用 conf/mapping.yaml）
bash run.sh

# 指定 mapping
GOFLOW2_MAPPING=/path/to/your/mapping.yaml bash run.sh

# 停止
bash stop.sh
```

## 关键环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `GOFLOW2_LISTEN` | `sflow://:6343,netflow://:2055` | 采集监听地址 |
| `GOFLOW2_FORMAT` | `json` | 输出格式：`json` / `csv` / `text` / `bin` |
| `GOFLOW2_TRANSPORT` | `file` | 传输方式：`file` / `kafka` / `netcat` |
| `GOFLOW2_MAPPING` | `conf/mapping.yaml` | 字段映射配置文件路径 |
| `GOFLOW2_LOGLEVEL` | `info` | 日志级别：`debug` / `info` / `warn` / `error` |
| `GOFLOW2_PID_FILE` | `logs/goflow2.pid` | pid 文件路径 |
| `GOFLOW2_LOG_FILE` | `logs/goflow2.log` | 日志文件路径 |

## 示例：CSV + netcat 负载分担

```bash
GOFLOW2_FORMAT=csv \
GOFLOW2_TRANSPORT=netcat \
GOFLOW2_MAPPING=conf/mapping.yaml \
bash run.sh -- \
  -format.csv.delimiter "|" \
  -transport.netcat.targets 10.0.0.1:9001,10.0.0.2:9001
```

## 示例：Kafka Kerberos

```bash
GOFLOW2_TRANSPORT=kafka \
bash run.sh -- \
  -transport.kafka.brokers kafka-1.example.com:9093 \
  -transport.kafka.tls \
  -transport.kafka.sasl gssapi \
  -transport.kafka.sasl.kerberos.username goflow2 \
  -transport.kafka.sasl.kerberos.keytab /etc/krb5/goflow2.keytab \
  -transport.kafka.sasl.kerberos.realm EXAMPLE.COM
```

## 从源码构建部署包

```bash
bash .deploy/build.sh all   # linux-amd64 + linux-arm64
# 产物在 .release/goflow2-deploy-<version>-<date>.tar.gz
```