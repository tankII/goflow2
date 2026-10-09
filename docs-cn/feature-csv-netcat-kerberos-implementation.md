# GoFlow2 功能增强实施方案

> 前置：设计见 `docs/feature-csv-netcat-kerberos-design.md`
> 目标提交：`cmd/goflow2` 主程序 + `format/` + `transport/`，先实现单车机打通，再联调

## 1. 实施顺序与依赖

按「先 producer 支撑 → 再格式 → 再传输 → 再挂接 → 再验证」推进，每步可独立编译：

1. **CSV 支撑**：`producer/proto/messages.go` 抽取渲染 helper + 新增 `CSVValues()`
2. **CSV 驱动**：新建 `format/csv/csv.go` + 单测
3. **netcat 传输**：新建 `transport/netcat/netcat.go` + 集成单测
4. **kafka GSSAPI**：改 `transport/kafka/kafka.go` + 单测
5. **挂接**：`cmd/goflow2/main.go` 增加 blank import
6. **验证**：build / vet / test / 本地联调

## 2. 具体改动点

### 2.1 `producer/proto/messages.go`

- 从 `FormatMessageReflectCustom`（约 195-289 行）抽出内部 helper，例如：

  ```go
  // renderFields 返回与 formatter.Fields() 顺序一致的值列表（缺失列使用空串占位）
  func (m *ProtoProducerMessage) renderFields() []string
  ```

  复用现有 `fields / Rename / Remap / Render / mapUnknown / IsArray` 逻辑；缺失且非虚拟字段时 append `""`（区别于现有 `continue`）。`FormatMessageReflectCustom` 改为调用该 helper 保持原输出不变（防止回归）。
- 新增公开方法：

  ```go
  // CSVValues 返回按字段顺序渲染的字符串列表，供 CSV 驱动使用
  func (m *ProtoProducerMessage) CSVValues() []string
  ```

### 2.2 新建 `format/csv/csv.go`

- package `csv`，`init()` 中 `format.RegisterFormatDriver("csv", &CSVDriver{})`。
- `Prepare()` 绑定 flag `-format.csv.delimiter`（默认 `"|"`，校验为单字节）。
- `Format(data) (key, out []byte, err error)`：
  - 断言 `interface{ Key() []byte }` 与 `interface{ CSVValues() []string }`；
  - 对每个值做 RFC 4180 转义：含分隔符 / `"` / `\r` / `\n` 时包引号并把 `"` 双写；
  - 用 `strings.Join` 拼行，追加 `\n`；
  - 断言失败返回 `format.ErrNoSerializer`。
- `Prepare()/Init()/Close()` 遵循现有驱动空实现约定。

### 2.3 新建 `transport/netcat/netcat.go`

- package `netcat`，`init()` 中 `transport.RegisterTransportDriver("netcat", ...)`。
- 结构：

  ```go
  type target struct {
      addr  string
      queue chan []byte   // 有界队列
      conn  net.Conn
      w     *bufio.Writer
      done  chan struct{} // Close 信号
      wg    sync.WaitGroup
      dropped atomic.Uint64
  }

  type NetcatDriver struct {
      targets []*target
      sep     []byte
      timeout time.Duration
      reconnect time.Duration
      queueSize int
      drop bool
  }
  ```

- `Init()`：解析 `-transport.netcat.targets`（逗号分隔），为每个目标启动 writer goroutine：
  - dial（`net.DialTimeout("tcp", addr, timeout)`）成功后读队列、`data+sep` 写入并 flush；
  - 写/读错误 → 关闭连接、进入重连等待（`reconnect` 间隔）；
  - `done` 触发退出。
- `Send(key, data)`：**round-robin 选一个目标**（`tgt := targets[int(rr.Add(1)-1)%len(targets)]`），仅向该目标队列投递副本；队列满时按 `drop` 策略：`true` → 丢新并计数、`false` → 阻塞投递。
- `Close()`：关闭所有 `done`，`wg.Wait()`，关闭连接。
- `Prepare()` 绑定全部 flag（见设计表 4.3）。

### 2.4 修改 `transport/kafka/kafka.go`

- 新增常量 `KAFKA_SASL_GSSAPI KafkaSASLAlgorithm = "gssapi"`，加入 `saslAlgorithms` 与 `saslAlgorithmsList`。
- 新增字段与 flag（见设计表 5.4；含必填校验：keytab/username/realm 缺失即报错）。
- 把 `Init()` 中从 `sarama.NewConfig()` 到 SASL/TLS/压缩组装的部分（约 132-262 行）抽为 `buildKafkaConfig() (*sarama.Config, error)`，`Init()` 调用它再 `NewAsyncProducer`。
- SASL 分支新增：

  ```go
  case KAFKA_SASL_GSSAPI:
      kafkaConfig.Net.SASL.Enable = true
      kafkaConfig.Net.SASL.Handshake = true
      kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
      kafkaConfig.Net.SASL.GSSAPI = sarama.GSSAPIConfig{ ... KRB5_KEYTAB_AUTH ... }
  ```

- **守卫收窄**：仅当 `kafkaSASL` 为 `PLAIN/SCRAM*` 时执行「`KAFKA_SASL_USER/KAFKA_SASL_PASS` 必填」的环境变量校验；gssapi 分支跳过。

### 2.5 挂接 `cmd/goflow2/main.go`

在现有 blank import 块（17-21 行）后增加：

```go
_ "github.com/netsampler/goflow2/v3/format/csv"
_ "github.com/netsampler/goflow2/v3/transport/netcat"
```

（如需 enricher 也支持 csv/netcat，将两者 import 同步加入 `cmd/enricher/main.go`。）

## 3. 测试计划

### 3.1 单测

| 用例 | 文件 | 断言 |
|---|---|---|
| CSV 列序与占位：构造带 `-mapping` 的 producer 消息 | `format/csv/csv_test.go` | 输出列数与 `Mapping.Fields` 一致；缺失字段为空串；行尾 `\n` |
| CSV 转义：值含 `\|`、`"`、换行 | 同上 | 按 RFC 4180 加引号/双写 |
| netcat：本地回环收行 | `transport/netcat/netcat_test.go` | 起 `net.Listen`，`Send` 多行，对端逐行收到且以 `\n` 结尾 |
| netcat：断连自动重连 | 同上 | 先写后关监听再恢复，验证重新连上并继续收发（lb 轮询恢复到该目标） |
| netcat：多目标 LB | 同上 | 两个监听目标；N 条记录轮询分发，总条数=N、各端收到的条目互斥且尽量均分 |
| netcat：队列满丢新 | 同上 | 目标不读时满队列触发 drop 计数 |
| kafka gssapi 装配 | `transport/kafka/kafka_test.go` | `buildKafkaConfig()`：`Mechanism==SASLTypeGSSAPI`、`GSSAPI.AuthType==KRB5_KEYTAB_AUTH`、username/keytab/realm/config 透传、`Validate()` 通过 |
| kafka gssapi 校验 | 同上 | keytab/username/realm 任一为空报错；PLAIN 无 env 仍报错（守卫收窄不破坏原行为） |

### 3.2 验证命令

```bash
go build ./...                              # 编译全通过
go vet ./...                                # 静态检查
go test ./format/... ./transport/... ./producer/...   # 单测
go build -o dist/goflow2 ./cmd/goflow2      # 产物构建
```

## 4. 联调示例

### 4.1 CSV 输出

```bash
./dist/goflow2 -listen sflow://:6343,netflow://:2055 \
  -format csv \
  -format.csv.delimiter "|" \
  -transport file -loglevel debug
# 输出形如：2026-09-23T...|1.2.3.4|443|10.0.0.1|...（字段顺序=全 json 字段顺序）
```

### 4.2 netcat 对接 `nc -l`（负载分担）

```bash
# 终端1/2：对端监听（分摊方）
nc -l 9001
nc -l 9002
# 终端3：goflow2，目标列表逗号分隔，round-robin 分发（单条只进一个目标）
./dist/goflow2 -listen sflow://:6343 -format json -transport netcat \
  -transport.netcat.targets 127.0.0.1:9001,127.0.0.1:9002
```

### 4.3 netcat 对接 flume netcat source

flume 侧 `a1.sources.r1.type = netcat`（netcat source 按行接收），goflow2 参数同上（把多个 flume agent 地址列入 targets，按轮询分摊）；数据以 `\n` 分隔成逐事件投递。

### 4.4 Kafka Kerberos

```bash
./dist/goflow2 -listen sflow://:6343 -format json -transport kafka \
  -transport.kafka.brokers kafka-1.example.com:9093,kafka-2.example.com:9093 \
  -transport.kafka.tls -transport.kafka.tls.ca /etc/kafka/ca.pem \
  -transport.kafka.sasl gssapi \
  -transport.kafka.sasl.kerberos.username goflow2 \
  -transport.kafka.sasl.kerberos.keytab /etc/krb5/goflow2.keytab \
  -transport.kafka.sasl.kerberos.realm EXAMPLE.COM \
  -transport.kafka.sasl.kerberos.service-name kafka \
  -transport.kafka.sasl.kerberos.config /etc/krb5.conf
```

前提：broker 用 FQDN 可达（SPN `kafka/<fqdn>@EXAMPLE.COM`），keytab 含客户端 principal `goflow2` 的密钥；`username`/`realm`/`keytab` 三项必填。

## 5. 完成定义（DoD）

- [ ] `go build ./...`、`go vet ./...`、单测全绿
- [ ] CSV：无表头、默认 `|` 分隔、字段顺序正确、缺失列占位、RFC 4180 转义
- [ ] netcat：多目标轮询（LB）逐行、断连丢弃+退避重连、队列满丢新、`nc -l` 与 flume 联调通过
- [ ] kafka：`gssapi` 认证走 keytab，`buildKafkaConfig` 装配正确，PLAIN/SCRAM 原行为不回归
- [ ] 两份文档（本实施 + 设计）随代码提交