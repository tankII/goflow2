# GoFlow2 命令行使用指南

> 本文档说明新增的 CSV 输出格式、netcat 传输、Kafka Kerberos 认证的命令行用法。

## 1. CSV 输出格式

### 基本用法

```bash
# 输出到文件（以竖线分隔）
goflow2 \
  -listen sflow://:6343,netflow://:2055 \
  -format csv \
  -transport file

# 指定其他分隔符（逗号）
goflow2 \
  -listen sflow://:6343 \
  -format csv \
  -format.csv.delimiter "," \
  -transport file

# 输出到 stdout（默认）
goflow2 \
  -listen sflow://:6343 \
  -format csv \
  -transport file
```

### CSV 输出示例

默认输出全部 50 个字段，以 `|` 分隔：

```
SFLOW_5|1790153021915485038|19338722|256|192.168.1.1|1790153021915485038|1790153021915485038|1522|1|20.60.225.225|192.168.1.164|IPv4|TCP|443|12593|24|0|60:be:b4:07:6a:18|20:7b:d2:0c:2c:db|1|1|1|116|0|102|2|16|0|0|0|40799|0|0|0||0|0.0.0.0/0|0.0.0.0/0||[]|[]|[]|[]|[]|0|0|[Ethernet,Dot1Q,IPv4,TCP]|[14,4,20,24]|[]|0
```

### 配合 mapping 选择字段

```bash
# 仅输出关键字段
goflow2 \
  -listen sflow://:6343 \
  -format csv \
  -mapping cmd/goflow2/mapping.yaml \
  -transport file
```

## 2. Netcat 传输

### 基本用法

```bash
# 单目标
goflow2 \
  -listen sflow://:6343 \
  -format json \
  -transport netcat \
  -transport.netcat.targets 10.0.0.1:9001

# 多目标负载分担（round-robin）
goflow2 \
  -listen sflow://:6343 \
  -format json \
  -transport netcat \
  -transport.netcat.targets 10.0.0.1:9001,10.0.0.2:9001,10.0.0.3:9001
```

### Netcat 参数说明

| Flag | 默认值 | 说明 |
|---|---|---|
| `-transport.netcat.targets` | `""` | 目标地址列表，逗号分隔 `host:port` |
| `-transport.netcat.sep` | `\n` | 行结束符 |
| `-transport.netcat.dial.timeout` | `3s` | 拨号超时 |
| `-transport.netcat.reconnect.interval` | `3s` | 断连重连间隔 |
| `-transport.netcat.queue` | `1024` | 每目标队列深度 |
| `-transport.netcat.drop` | `true` | 队列满时丢弃新数据 |

### 对接 nc -l 调试

```bash
# 终端 1：监听
nc -l 127.0.0.1 9001

# 终端 2：采集
goflow2 \
  -listen sflow://:6343 \
  -format json \
  -transport netcat \
  -transport.netcat.targets 127.0.0.1:9001
```

### 对接 Flume netcat source

Flume 配置示例：

```properties
a1.sources = r1
a1.channels = c1
a1.sinks = k1

a1.sources.r1.type = netcat
a1.sources.r1.bind = localhost
a1.sources.r1.port = 9001

a1.channels.c1.type = memory
a1.channels.c1.capacity = 10000

a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /raw/logs/%y-%m-%d/
a1.sinks.k1.hdfs.filePrefix = events
a1.sinks.k1.channel = c1
a1.sources.r1.channel = c1
```

Goflow2 侧：

```bash
goflow2 \
  -listen sflow://:6343 \
  -format json \
  -transport netcat \
  -transport.netcat.targets 10.0.0.10:9001
```

## 3. Kafka Kerberos 认证

### 基本用法

```bash
goflow2 \
  -listen sflow://:6343 \
  -format json \
  -transport kafka \
  -transport.kafka.brokers kafka-1.example.com:9093,kafka-2.example.com:9093 \
  -transport.kafka.tls \
  -transport.kafka.tls.ca /etc/kafka/ca.pem \
  -transport.kafka.sasl gssapi \
  -transport.kafka.sasl.kerberos.username goflow2 \
  -transport.kafka.sasl.kerberos.keytab /etc/krb5/goflow2.keytab \
  -transport.kafka.sasl.kerberos.realm EXAMPLE.COM \
  -transport.kafka.sasl.kerberos.service-name kafka \
  -transport.kafka.sasl.kerberos.config /etc/krb5.conf
```

### Kerberos 认证必填项

| Flag | 必填 | 说明 |
|---|---|---|
| `-transport.kafka.sasl` | 是 | 设为 `gssapi` |
| `-transport.kafka.sasl.kerberos.username` | 是 | 客户端 principal（不含 realm），如 `goflow2` |
| `-transport.kafka.sasl.kerberos.keytab` | 是 | keytab 文件路径 |
| `-transport.kafka.sasl.kerberos.realm` | 是 | Kerberos realm，如 `EXAMPLE.COM` |
| `-transport.kafka.sasl.kerberos.service-name` | 否 | 服务名，默认 `kafka` |
| `-transport.kafka.sasl.kerberos.config` | 否 | krb5.conf 路径；不填时按 `KRB5_CONFIG` 环境变量 → `/etc/krb5.conf` 顺序查找，三者均不存在则启动时报错 |
| `-transport.kafka.sasl.kerberos.disable-paf-x-fast` | 否 | 禁用 PA-FX-FAST，默认 false |

### Kerberos 前提条件

1. **Broker FQDN**：Kerberos SPN 为 `kafka/<broker_fqdn>@REALM`，broker 地址必须用 FQDN 且可解析。
2. **krb5.conf**：客户端需配置 krb5.conf，realm 与 broker 一致。可通过 `-transport.kafka.sasl.kerberos.config` 显式指定，或依赖 `KRB5_CONFIG` 环境变量，或默认 `/etc/krb5.conf`；三者均不存在时 goflow2 启动报错并提示配置路径。
3. **Keytab**：keytab 包含客户端 principal 密钥，principal 命名规范为 `goflow2@REALM`。
4. **网络**：客户端可连接 broker 的 88 端口（KDC）与 9093（Kafka over TLS）。

### Keytab 生成示例

```bash
# KDC 上操作
ktadd -k /tmp/goflow2.keytab goflow2@EXAMPLE.COM
```

## 4. 完整示例

### 示例 1：CSV 直出 + nc 调试

```bash
# 对端
nc -l 127.0.0.1 9001

# 采集端
goflow2 \
  -listen sflow://:6343 \
  -format csv \
  -format.csv.delimiter "|" \
  -transport netcat \
  -transport.netcat.targets 127.0.0.1:9001
```

### 示例 2：多目标负载分担 + Kafka Kerberos 归档

```bash
# 主采集到 Kafka Kerberos
goflow2 \
  -listen sflow://:6343,netflow://:2055 \
  -format json \
  -transport kafka \
  -transport.kafka.brokers kafka-1.example.com:9093 \
  -transport.kafka.tls \
  -transport.kafka.sasl gssapi \
  -transport.kafka.sasl.kerberos.username goflow2 \
  -transport.kafka.sasl.kerberos.keytab /etc/krb5/goflow2.keytab \
  -transport.kafka.sasl.kerberos.realm EXAMPLE.COM \
  -transport.kafka.sasl.kerberos.service-name kafka

# 副采集到 Flume 负载分担
goflow2 \
  -listen sflow://:6343,netflow://:2055 \
  -format json \
  -transport netcat \
  -transport.netcat.targets 10.0.0.10:9001,10.0.0.11:9001,10.0.0.12:9001
```

### 示例 3：Enricher 后接 CSV + netcat

```bash
# 第一步：goflow2 输出二进制流
goflow2 \
  -listen sflow://:6343 \
  -format bin \
  -transport file

# 第二步：enricher 地理信息富化 + CSV 输出
enricher \
  -format csv \
  -transport netcat \
  -transport.netcat.targets 10.0.0.1:9001 \
  -db.asn /path/to/GeoIP2-ASN.mmdb \
  -db.country /path/to/GeoIP2-Country.mmdb
```