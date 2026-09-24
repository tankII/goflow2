# GoFlow2 功能增强设计方案

> 状态：已评审定稿
> 影响面：格式/传输插件层，核心采集环不变

## 1. 背景与目标

本方案为 goflow2 增加三项能力，均通过现有插件注册体系（`format` / `transport` 注册表 + `flag` 绑定，见 `format/format.go`、`transport/transport.go`、`pkg/goflow2/config/config.go`）实现，不改动采集/解码/producer 主流程：

1. **CSV 输出格式**：新增 `csv` 格式驱动，字段分隔符可配置（默认 `|`），无表头。
2. **netcat 传输**：新增 `netcat` 传输驱动，逐行发送、支持多个 `host:port` 目标，可对接 `nc -l` 或 flume netcat source；多目标按 **round-robin（负载分担）** 分发——每条记录只发往其中一个目标；目标不可达时丢弃新数据并自动重连。
3. **Kafka Kerberos**：扩展 `kafka` 传输的 SASL 认证，支持通过 **keytab** 访问 Kerberos 认证的 Kafka 服务（GSSAPI 机制）。

## 2. 总体架构

```
  decoders -> producer(Frame/Proto) -> formatter -> transport
                                          |
                     format/csv (new)     |      transport/netcat (new)
                     format/json/text/bin |      transport/kafka(+gssapi) transport/file
```

- 格式驱动通过 `format.RegisterFormatDriver(name, driver)` 注册，flag 命名空间 `-format.<name>.*`。
- 传输驱动通过 `transport.RegisterTransportDriver(name, driver)` 注册，flag 命名空间 `-transport.<name>.*`。
- 新增插件只需在 `cmd/goflow2/main.go` 增加 blank import 挂接（`init()` 自动注册并绑定 flag）。

## 3. 功能一：CSV 输出格式

### 3.1 需求

- 输出的每条流记录为一行 CSV。
- 字段分隔符可配置（单字符，默认 `|`）。
- 不需要表头行。
- 列序与现用 `formatter.Fields()`（`-mapping` 配置）一致；缺失字段以空字符串占位，保证列对齐。
- 值中若含分隔符、引号或换行，按 RFC 4180 转义（整体加引号、内部双写引号）。

### 3.2 关键设计点

CSV 必须「定列」，而现有 `ProtoProducerMessage.FormatMessageReflectCustom`（`producer/proto/messages.go:195`）对缺失字段执行 `continue`（列会错位），且字段名/渲染逻辑依赖未导出的 `m.formatter`。因此：

- 在 `producer/proto/messages.go` 抽取公共渲染 helper，从 `FormatMessageReflectCustom` 复用同一套 `fields/Rename/Remap/Render/mapUnknown/IsArray` 逻辑，避免行为分叉。
- 新增方法 `CSVValues() []string`：按 `formatter.Fields()` 顺序返回每个字段渲染后的字符串；缺失（且非虚拟）字段返回 `""` 占位；切片字段（如 `as_path`、`bgp_communities`）沿用现有 `[...]` 拼接后整体转义。
- 新增 `format/csv/csv.go`：对 `data` 做接口断言 `interface{ CSVValues() []string }`（沿用现有 `encoding.TextMarshaler` 断言模式，`format/text/text.go:30`），按分隔符 join 并转义，追加 `\n`。

### 3.3 Flags

| Flag | 默认 | 说明 |
|---|---|---|
| `-format.csv.delimiter` | `\|` | 字段分隔符（单字符，支持 `,` `;` `\t` 等） |

### 3.4 取舍说明

- 无表头（需求确认）。
- 分隔符限定为单字符，简化"字段内是否转义"的判断；多字符分隔符如后续需要可扩展。
- CSV 为"尽力渲染"，不感知采集错误（沿用 json/text 的行为，错误由上游丢弃处理）。

## 4. 功能二：netcat 逐行 TCP 传输

### 4.1 需求

- 逐行发送：每条格式化后的记录作为一行（默认以 `\n` 结尾）经 TCP 发出。
- 支持多个目标地址:端口（`-transport.netcat.targets` 逗号分隔）。
- **多目标用于负载分担**：每条记录仅发送到**一个**目标，采用 round-robin 轮询选择，实现按目标分摊流量（如多个 flume 采集端分压）。
- 可对接 `nc -l <port>` 或 flume 的 netcat source（netcat source 按行读、剥离行分隔符作为一个事件）。
- 目标不可达/断连时：**丢弃新数据 + 按退避自动重连**，不阻塞采集流水线。

### 4.2 关键设计点

采用「**每目标一个 writer goroutine + 有界队列**」：

- 同步写（每行遍历所有目标直接写）会被单个死目标拖死整条流水线，故不用。
- `Init()` 解析 `targets`，为每个目标启动 goroutine：dial（成功建立连接）→ 从队列取 `data + sep` 写入并 flush → 写/读错误即关闭连接 → 按退避间隔进入重连循环。
- `Send(key, data)` **按 round-robin 选一个目标**投递（原子计数器 `rr % len(targets)`），不做复制；该目标队列满时按丢弃策略处理（默认丢新、预算计数），不阻塞。
- `Close()`：向各 goroutine 发停止信号、关闭连接、等待退出并 flush 存量。
- 目标同时离线时数据照常流转到对应队列并丢弃，producer 不受影响。
- 丢弃/重连计数通过 `slog`（debug 级）暴露，便于排障。
- 注：LB 语义下，单个目标的记录丢失不会被重发给其他目标（无容错）；如需容错应增加目标副本。

### 4.3 Flags

| Flag | 默认 | 说明 |
|---|---|---|
| `-transport.netcat.targets` | `""` | 目标列表，逗号分隔的 `host:port`（可多个，负载分担） |
| `-transport.netcat.sep` | `\n` | 行结束符 |
| `-transport.netcat.dial.timeout` | `3s` | 拨号超时 |
| `-transport.netcat.reconnect.interval` | `3s` | 断连后的重连间隔（固定间隔） |
| `-transport.netcat.queue` | `1024` | 每个目标的队列容量 |
| `-transport.netcat.drop` | `true` | 队列满时丢弃新数据；`false` 时阻塞 Send 背压 |

### 4.4 兼容性语义

- `nc -l 9999`：行式展示，末行带 `\n` 正常。
- flume netcat source：对端按 `\n` 分隔读取事件，`\n` 被剥离，事件内容即格式化行——完全兼容。

## 5. 功能三：kafka transport 支持 Kerberos(GSSAPI)

### 5.1 需求

- `-transport kafka -transport.kafka.sasl gssapi` 时通过 Kerberos 访问 Kafka。
- 认证方式：**仅 keytab**（需求确认，`KRB5_KEYTAB_AUTH`）。
- 凭证、realm、service name、krb5.conf 均可配置。

### 5.2 现状分析

- `transport/kafka/kafka.go` 现有 SASL 仅支持 `PLAIN / SCRAM-SHA256 / SCRAM-SHA512`，且 `Init()`（kafka.go:225-254）存在守卫：SASL 启用且 `KAFKA_SASL_USER/KAFKA_SASL_PASS` 全空即报错（该守卫仅适用于 PLAIN/SCRAM）。
- 依赖：`github.com/Shopify/sarama v1.38.1`（go.mod:8）已内置 GSSAPI 支持（`gssapi_kerberos.go`，含 `GSSAPIConfig`、`SASLTypeGSSAPI`、`KRB5_USER_AUTH=1`、`KRB5_KEYTAB_AUTH=2`）。
- `github.com/jcmturner/gokrb5/v8 v8.4.3` 已在 go.mod/go.sum（indirect，被 sarama 引用）——**零新增依赖**。

### 5.3 关键设计点

- 增加枚举 `gssapi` 及 `saslAlgorithms`/`saslAlgorithmsList` 登记。
- 在 `Init()` 的 SASL 分支新增 gssapi 处理：

  ```go
  kafkaConfig.Net.SASL.Enable = true
  kafkaConfig.Net.SASL.Handshake = true
  kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
  kafkaConfig.Net.SASL.GSSAPI = sarama.GSSAPIConfig{
      AuthType:           sarama.KRB5_KEYTAB_AUTH,
      Username:           d.kafkaSASLKerberosUsername, // 客户端 principal（不含 realm）
      KeyTabPath:         d.kafkaSASLKerberosKeytab,
      KerberosConfigPath: d.kafkaSASLKerberosConfig,
      ServiceName:        d.kafkaSASLKerberosServiceName,
      Realm:              d.kafkaSASLKerberosRealm,
      DisablePAFXFAST:    d.kafkaSASLKerberosDisablePAFXFAST,
  }
  ```

  > 注：即使使用 keytab，sarama 的 `NewWithKeytab(username, realm, kt, ...)` 仍需要 `Username`（客户端 principal）与 `Realm`，且 `Validate()` 强制二者非空——因此 `username`、`realm`、`keytab` 均为必填。

- **必要改造**：把 kafka.go:234 的「环境变量守卫」收窄到非 gssapi 分支，否则 GSSAPI（无 user/pass）会直接报错。
- **可测性改造**：把 `Init()` 中 sarama 配置组装抽为 `buildKafkaConfig()`（返回 `*sarama.Config`），便于单测断言 GSSAPI 字段与校验错误路径，同时保证与真实拨号解耦。
- 前提条件（文档注明）：
  - broker 地址需使用可解析到服务 principal 的 **FQDN**（Kerberos SPN 为 `kafka/<fqdn>@REALM`）；
  - krb5.conf 可用（默认读取 `KRB5_CONFIG` 环境变量指定的文件，或 `/etc/krb5.conf`）；
  - keytab 包含该客户端 principal 的密钥。

### 5.4 Flags

| Flag | 默认 | 说明 |
|---|---|---|
| `-transport.kafka.sasl` | `none` | 新增可选值 `gssapi` |
| `-transport.kafka.sasl.kerberos.username` | `""` | 客户端 principal（不含 realm），**必填** |
| `-transport.kafka.sasl.kerberos.keytab` | `""` | keytab 文件路径，**必填** |
| `-transport.kafka.sasl.kerberos.realm` | `""` | 客户端 realm，**必填** |
| `-transport.kafka.sasl.kerberos.service-name` | `kafka` | 服务名（SPN 前缀） |
| `-transport.kafka.sasl.kerberos.config` | `""` | krb5.conf 路径，空则走 `KRB5_CONFIG`/系统默认 |
| `-transport.kafka.sasl.kerberos.disable-paf-x-fast` | `false` | 传输层禁用 PA-FX-FAST（老 KDC 兼容） |

## 6. 影响面与兼容性

- 新增插件均为独立注册，默认不启用；不改变现有 json/text/binary、file/kafka 的行为（kafka 仅 SASL 处多一个可选分支 + 守卫收窄）。
- `GetFormats()`/`GetTransports()`（`pkg/goflow2/config/common.go` 的 help 文本）自动包含新名称，无需改公共代码。
- `-help` 自动展示新 flag。

## 7. 风险与对策

| 风险 | 对策 |
|---|---|
| CSV 缺失字段列错位 | `CSVValues` 输出固定长度并用 `""` 占位 |
| netcat 死目标阻塞流水线 | 每目标异步 goroutine + 有界队列 + 丢新策略 |
| goflow2 与 GSSAPI 不兼容 | sarama v1.38 + gokrb5 均在依赖树，代码路径内置；单测断言配置装配 |
| 环境变量守卫误伤 gssapi | 守卫收窄至非 gssapi 分支 |