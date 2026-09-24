# GoFlow2 配置手册

> 本文档说明新增功能（CSV 格式、netcat 传输、Kafka Kerberos）的配置项与使用场景。

## 1. CSV 输出格式配置

### 格式驱动注册

CSV 格式通过 `format.RegisterFormatDriver("csv", ...)` 注册，由 `cmd/goflow2/main.go` 的 blank import 激活：

```go
_ "github.com/netsampler/goflow2/v3/format/csv"
```

### 配置项

| Flag | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `-format.csv.delimiter` | string | `\|` | 字段分隔符，支持单字节字符（`,` `;` `\t` 等） |

### 行为说明

- **无表头**：输出的每行均为数据行，不包含列名。
- **字段顺序**：与 `-mapping` 中 `formatter.fields` 顺序一致；未配置 mapping 则按默认全字段顺序。
- **缺失字段**：映射到的 proto 字段不存在时输出空字符串 `""`，保证列对齐。
- **转义规则**：遵循 RFC 4180。含分隔符、引号、换行的字段加双引号并转义内部引号。
- **切片字段**：`as_path`、`bgp_communities` 等切片字段输出为 `[val1,val2,...]` 形式。

### 与 mapping 配合

```yaml
# cmd/goflow2/mapping.yaml
formatter:
  fields:
    - sampler_address
    - src_addr
    - dst_addr
    - src_port
    - dst_port
    - proto
    - bytes
    - packets
  key:
    - sampler_address
```

```bash
goflow2 \
  -listen sflow://:6343 \
  -format csv \
  -format.csv.delimiter "," \
  -mapping cmd/goflow2/mapping.yaml \
  -transport file
```

输出：
```
192.168.1.1,10.0.0.1,203.0.113.5,443,12345,TCP,1500,10
```

## 2. Netcat 传输配置

### 传输驱动注册

Netcat 传输通过 `transport.RegisterTransportDriver("netcat", ...)` 注册，由 blank import 激活。

### 配置项

| Flag | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `-transport.netcat.targets` | string | `""` | 目标地址列表，逗号分隔 `host:port` |
| `-transport.netcat.sep` | string | `\n` | 行结束符，可改为 `\r\n` 等 |
| `-transport.netcat.dial.timeout` | duration | `3s` | TCP 拨号超时 |
| `-transport.netcat.reconnect.interval` | duration | `3s` | 断连重连间隔 |
| `-transport.netcat.queue` | int | `1024` | 每目标有界队列容量 |
| `-transport.netcat.drop` | bool | `true` | 队列满时丢弃新数据；`false` 时阻塞 Send |

### 负载分担语义

**Round-robin**：每条记录仅发送到**一个**目标，按目标列表顺序轮询。

| 目标数 | 适用场景 |
|---|---|
| 1 个 | 单一 flume/kafka 代理、调试用 nc |
| 多个 | 多个 flume agent 分摊流量 |

**注意**：LB 模式下某目标离线不会把记录重发给其他目标。如需容错请使用多副本（相同地址）或上游增加 redundancy。

### 重连策略

- 连接成功 → 正常运行
- 连接失败 / 写错误 → 关闭连接，等待 `reconnect.interval` 后重试
- 重连期间：新数据进入队列；队列满则触发 `drop` 策略
- Close 时：flush 队列存量数据后退出

### 性能调优

| 场景 | 推荐配置 |
|---|---|
| 低延迟 | `-transport.netcat.queue 256`，`-transport.netcat.drop=true` |
| 高吞吐 | `-transport.netcat.queue 4096`，`-transport.netcat.reconnect.interval 1s` |
| 不丢数据 | `-transport.netcat.drop=false`（背压模式，可能拖慢采集） |

## 3. Kafka Kerberos 认证配置

### 认证方式总览

| SASL 算法 | 认证方式 | 凭证来源 |
|---|---|---|
| `none` | 无认证 | — |
| `plain` | 用户名/密码 | 环境变量 `KAFKA_SASL_USER` / `KAFKA_SASL_PASS` |
| `scram-sha256` | SCRAM-SHA-256 | 同上 |
| `scram-sha512` | SCRAM-SHA-512 | 同上 |
| **`gssapi`** | **Kerberos (GSSAPI)** | **keytab 文件** |

### Kerberos 配置项

在 `-transport.kafka.sasl gssapi` 时使用以下专属 flag：

| Flag | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `-transport.kafka.sasl.kerberos.username` | ✅ | `""` | 客户端 principal（不含 @realm），如 `goflow2` |
| `-transport.kafka.sasl.kerberos.keytab` | ✅ | `""` | keytab 文件绝对路径 |
| `-transport.kafka.sasl.kerberos.realm` | ✅ | `""` | Kerberos realm，如 `EXAMPLE.COM` |
| `-transport.kafka.sasl.kerberos.service-name` | 否 | `kafka` | Kafka 服务名（SPN 前缀） |
| `-transport.kafka.sasl.kerberos.config` | 否* | `""` | krb5.conf 路径；空则读 `KRB5_CONFIG` 环境变量，再试 `/etc/krb5.conf`；三者均不存在时报错 |
| `-transport.kafka.sasl.kerberos.disable-paf-x-fast` | 否 | `false` | 禁用 PA-FX-FAST（兼容旧版 KDC） |

### Kerberos Principal 命名规范

Goflow2 作为 Kafka 客户端，其 principal 命名规范：

```
goflow2@EXAMPLE.COM
```

`username` flag 填入 `goflow2`，`realm` 填入 `EXAMPLE.COM`，两者组合即 principal。

Kafka 服务的 SPN（Service Principal Name）：

```
kafka/broker1.example.com@EXAMPLE.COM
```

由 `-transport.kafka.sasl.kerberos.service-name kafka` + broker FQDN + realm 组成。

### 验证连接

使用 klist 检查 keytab：

```bash
klist -kte /etc/krb5/goflow2.keytab
```

使用 kinit 测试 principal：

```bash
kinit goflow2 -k -t /etc/krb5/goflow2.keytab
klist
```

使用 kafka-topics 测试 SASL 连接：

```bash
kafka-console-producer \
  --broker-list kafka-1.example.com:9093 \
  --topic test \
  --security-protocol SASL_PLAINTEXT \
  --sasl-mechanism GSSAPI \
  --sasl-jaas-config "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab=/etc/krb5/goflow2.keytab principal=goflow2@EXAMPLE.COM;"
```

## 4. 配置优先级

Flags > 环境变量 > 代码默认值。

示例：

```bash
# 强制使用特定 krb5.conf
export KRB5_CONFIG=/etc/krb5/custom.conf
goflow2 -transport.kafka.sasl.kerberos.config /etc/krb5/override.conf \
  # 优先使用 override.conf
```

## 5. 故障排查

### CSV 格式问题

| 现象 | 原因 | 解决 |
|---|---|---|
| 列数不一致 | mapping.fields 配置有误 | 检查 fields 列表 |
| 缺失列错位 | 使用了虚拟字段但未定义 renderer | 确认 renderer 存在 |
| 特殊字符丢失 | 值含换行 | RFC 4180 转义已处理，无需额外操作 |

### Netcat 传输问题

| 现象 | 原因 | 解决 |
|---|---|---|
| 连接失败 | 目标未监听 | 确认对端服务启动 |
| 数据未到达 | 对端断开未重连 | 开启 `-loglevel debug` 查看重连日志 |
| 背压严重 | drop=false 且对端消费慢 | 调大 queue 或改 drop=true |
| 部分目标未收到 | round-robin 正常行为 | 多目标时应预期数据分片 |

### Kafka Kerberos 问题

| 现象 | 原因 | 解决 |
|---|---|---|
| `GSSAPI mechanism unauthorized` | username/realm/keytab 配置错误 | 检查 klist、principal 与 keytab 匹配 |
| `Kerberos config not found` | krb5.conf 路径无效 | 设置 `-transport.kafka.sasl.kerberos.config` |
| `Service principal not found` | broker FQDN 无法解析 | 检查 DNS，使用 FQDN 而非 IP |
| `PA-FX-FAST 兼容问题` | 旧版 KDC 不支持 FAST | 加 `-transport.kafka.sasl.kerberos.disable-paf-x-fast=true` |