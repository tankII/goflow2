# GoFlow2 Mapping 配置文件完整参考

> 适用于 goflow2 v3，配置文件通过 `-mapping <file>` 加载。

## 目录

1. [整体结构](#1-整体结构)
2. [formatter: 输出格式化](#2-formatter-输出格式化)
3. [ipfix: IPFIX 字段映射](#3-ipfix-ipfix-字段映射)
4. [netflowv9: NetFlow v9 字段映射](#4-netflowv9-netflow-v9-字段映射)
5. [sflow: sFlow 报文解析映射](#5-sflow-sflow-报文解析映射)
6. [渲染器（Renderer）详细说明](#6-渲染器renderer-详细说明)
7. [内置层解析器（Layer Parser）](#7-内置层解析器layer-parser)
8. [完整示例](#8-完整示例)

---

## 1. 整体结构

```yaml
# ── 输出格式化 ──────────────────────────────────────────
formatter:
  fields:   [...]    # 输出字段列表
  key:      [...]    # 行键（Kafka 分区用）
  rename:   {...}    # 字段重命名
  render:   {...}    # 渲染器
  literal:  {...}    # 常量字段（自定义列名 + 固定值）
  protobuf: [...]    # 自定义 protobuf 字段定义

# ── 协议解码映射 ────────────────────────────────────────
ipfix:
  mapping: [...]     # IPFIX Information Element → proto 字段

netflowv9:
  mapping: [...]     # NetFlow v9 template field → proto 字段

sflow:
  ports:  [...]      # 自定义端口→协议解析器注册
  mapping: [...]     # 报文偏移提取 → proto 字段
```

> **注意**：`ipfix`、`netflowv9`、`sflow` 三个 section 分别只对对应协议生效；`formatter` 对全协议通用。

---

## 2. formatter: 输出格式化

### 2.1 `fields` — 输出字段列表

```yaml
formatter:
  fields:
    - type
    - time_received_ns
    - sampler_address
    - src_addr
    - dst_addr
    - src_port
    - dst_port
    - proto
    - bytes
    - packets
    - in_if
    - out_if
    - src_mac
    - dst_mac
    - etype
    - icmp_name    # 虚拟字段
```

- **类型**：`[]string`
- **作用**：指定输出中出现的字段顺序；不填则输出全部 50+ 个 proto 字段
- **合法值**：
  - `pb/flow.proto` 中的 json tag 名（如 `src_addr`、`bytes`）
  - `protobuf:` 中自定义的字段名（如 `csum`）
  - 虚拟字段（如 `icmp_name`）
- **排序即列序**：JSON/CSV/TEXT 输出严格按此顺序

### 2.2 `key` — 行键

```yaml
formatter:
  key:
    - sampler_address
```

- **类型**：`[]string`
- **作用**：将这些字段值拼接后做 FNV32 哈希，生成行键
- **用途**：仅 Kafka 传输配合 `-transport.kafka.hashing` 使用，相同 key 记录落同一分区
- **约束**：列出的字段必须在 protobuf schema 中存在，否则编译报错

### 2.3 `rename` — 字段重命名

```yaml
formatter:
  rename:
    src_addr: source_ip
    dst_addr: destination_ip
    src_port: src
    dst_port: dst
```

- **类型**：`map[string]string`
- **作用**：将 proto 原始字段名替换为输出名（仅影响文本输出，不影响 protobuf 存储）
- **示例**：输出中 `src_addr` 显示为 `source_ip`

### 2.4 `render` — 渲染器

```yaml
formatter:
  render:
    time_received_ns: datetimenano
    time_flow_start_ns: datetime
    src_addr: ip
    src_mac: mac
    proto: proto
    src_net: network
```

- **类型**：`map[string]RendererID`
- **作用**：将原始二进制/数值字段转为可读字符串
- **可用 RendererID 值**：

| ID | 输入类型 | 输出格式 | 典型用途 |
|---|---|---|---|
| `none` | 任意 | 原值 / hex 编码 | 不做转换 |
| `ip` | `[]byte` (4 或 16 字节) | `192.168.1.1` | 地址类字段 |
| `mac` | `uint64` | `aa:bb:cc:dd:ee:ff` | MAC 地址 |
| `etype` | `uint32` / `uint64` | `IPv4`、`IPv6`、`ARP` | EtherType |
| `proto` | `uint32` / `uint64` | `TCP`、`UDP`、`ICMP`... | IP 协议号 |
| `network` | `uint32`（前缀长度） | `10.0.0.1/24` | 配合 SrcNet/DstNet |
| `datetime` | `uint64`/`int64`（秒） | `2026-09-24T08:00:00Z` | 时间字段（秒级） |
| `datetimenano` | `uint64`/`int64`（纳秒） | `2026-09-24T08:00:00.123456789Z` | 纳秒精度时间 |
| `string` | `[]byte` / `string` | 原始字符串 | 字节流转字符串 |

- **默认渲染器**（无需显式声明，自动生效）：

  | 字段 | 默认 renderer |
  |---|---|
  | `SrcMac`, `DstMac` | `mac` |
  | `SrcAddr`, `DstAddr`, `SamplerAddress`, `NextHop`, `BgpNextHop`, `MplsLabelIp`, `MplsIp` | `ip` |
  | `Etype` | `etype` |
  | `Proto` | `proto` |
  | `SrcNet`, `DstNet` | `network` |
  | `icmp_name` | ICMP 类型名渲染（`EchoReply`、`DestinationUnreachable` 等）|
  | `Ipv6RoutingHeaderAddresses` | `ip` |

### 2.5 `literal` — 常量字段

```yaml
formatter:
  fields:
    - time_received_ns
    - src_addr
    - dst_addr
    - flows        # 自定义列，非原生字段
  literal:
    flows: 1
```

- **类型**：`map[string]interface{}`（key 为列名，value 为固定值）
- **作用**：定义**不来自报文**的常量列。把列名加入 `fields` 指定其位置，再用 `literal` 给出该列的固定值
- **典型用途**：为输出补充「流记录数」`flows: 1`（nfdump `%fl` 语义），或标记数据来源、采集点等
- **取值规则**：
  - value 一律按**字符串**输出。YAML 数字/布尔会被转换为字符串，例如 `flows: 1` → `"1"`（JSON 中也是 `"1"`）
  - 若 value 需要保留为 YAML 字符串，也可直接写 `flows: "1"`，结果相同
  - 除 `fields` 外，常量列名**不能**出现在 `key`、`rename`、`render`、`protobuf` 中
- **优先级**：常量列名与原生 proto 字段名冲突时，**常量值优先**（该列不再输出报文值）
- **合法性**：只写 `literal` 而不把列名加入 `fields` 不会输出该列；把列名加入 `fields` 但未在 `literal`/`protobuf`/`render` 中定义则编译报错

### 2.6 `protobuf` — 自定义 Protobuf 字段

```yaml
formatter:
  protobuf:
    - name: flow_direction
      index: 42
      type: varint
    - name: bi_flow_direction
      index: 41
      type: varint
    - name: csum
      index: 999
      type: varint
    - name: src_ip_encap
      index: 1006
      type: string
      array: true
```

- **类型**：`[]ProtobufFormatterConfig`
- **作用**：向 FlowMessage protobuf 扩展自定义字段（存入 unknown fields）
- **字段说明**：

  | Key | 类型 | 必填 | 说明 |
  |---|---|---|---|
  | `name` | string | ✅ | 字段名，用于 `fields` 和 `destination` 引用 |
  | `index` | int32 | ✅ | protobuf field number；自定义建议 ≥1000 |
  | `type` | string | ✅ | 编码类型：`varint` / `string` / `bytes`（bytes 等同于 string）|
  | `array` | bool | 否 | 是否数组类型，默认 false |

- **约束**：
  - `index` 不得与 `pb/flow.proto` 中已有字段冲突
  - `type` 取值：`varint`、`string`、`bytes`（三者之一）
  - 自定义字段会被存入 protobuf unknown fields，下游消费方需自行编译含该字段的 proto

---

## 3. ipfix: IPFIX 字段映射

```yaml
ipfix:
  mapping:
    - field: 61
      destination: flow_direction
    - field: 239
      destination: bi_flow_direction
    - field: 234
      destination: ingress_vrf_id
    - field: 235
      destination: egress_vrf_id
```

### 字段说明

| Key | 类型 | 必填 | 说明 |
|---|---|---|---|
| `field` | uint16 | ✅ | IPFIX Information Element ID（IANA 注册或私有 PEN）|
| `pen` | uint32 | 否 | Private Enterprise Number（私有字段时）|
| `penprovided` | bool | 否 | 是否提供 pen，默认 false |
| `destination` | string | ✅ | 目标 proto 字段名（必须是 `pb/flow.proto` 中已有字段，或 `protobuf:` 中定义的自定义字段）|
| `endianness` | string | 否 | 字节序：`big`（默认）或 `little` |

### 常用 IPFIX Field ID 速查

| ID | 名称 | 说明 |
|---|---|---|
| 1 | octetDeltaCount | 字节数 |
| 2 | packetDeltaCount | 包数 |
| 4 | protocolIdentifier | 协议 |
| 7 | sourceTransportPort | 源端口 |
| 8 | sourceIPv4Address | 源 IPv4 |
| 9 | sourceIPv4PrefixLength | 源前缀长度 |
| 10 | ingressInterface | 入接口 |
| 11 | destinationTransportPort | 目的端口 |
| 12 | destinationIPv4Address | 目的 IPv4 |
| 13 | destinationIPv4PrefixLength | 目的前缀长度 |
| 14 | egressInterface | 出接口 |
| 15 | ipNextHopIPv4Address | 下一跳 IPv4 |
| 16 | bgpSourceAsNumber | 源 AS |
| 17 | bgpDestinationAsNumber | 目的 AS |
| 18 | bgpNextHopIPv4Address | BGP 下一跳 |
| 27 | sourceIPv6Address | 源 IPv6 |
| 28 | destinationIPv6Address | 目的 IPv6 |
| 31 | flowLabelIPv6 | IPv6 flow label |
| 34 | samplingInterval | 采样率 |
| 56 | sourceMacAddress | 源 MAC |
| 57 | postDestinationMacAddress | 目的 MAC |
| 58 | vlanId | VLAN ID |
| 60 | ipVersion | IP 版本 |
| 61 | flowDirection | 流方向 |
| 234 | ingressVRFID | 入 VRF |
| 235 | egressVRFID | 出 VRF |
| 239 | biflowDirection | 双向流方向 |
| 315 | dataLinkFrameSection | 数据链路层报文（触发 sFlow 解析）|

---

## 4. netflowv9: NetFlow v9 字段映射

结构同 ipfix.mapping，仅作用于 NetFlow v9 模板字段：

```yaml
netflowv9:
  mapping:
    - field: 34
      destination: sampling_rate
      endianness: little
    - field: 61
      destination: flow_direction
```

### 字段说明（同 ipfix）

| Key | 类型 | 说明 |
|---|---|---|
| `field` | uint16 | NetFlow v9 字段 ID |
| `pen` | uint32 | 私有企业编号（可选）|
| `penprovided` | bool | 是否提供 pen（可选）|
| `destination` | string | 目标 proto 字段名 |
| `endianness` | string | 字节序 `big`/`little` |

### 常用 NetFlow v9 Field ID 速查

| ID | 名称 | 说明 |
|---|---|---|
| 1 | IN_BYTES | 输入字节 |
| 2 | IN_PKTS | 输入包数 |
| 4 | PROTOCOL | 协议 |
| 6 | TCP_FLAGS | TCP 标志 |
| 7 | L4_SRC_PORT | 源端口 |
| 8 | IPV4_SRC_ADDR | 源 IPv4 |
| 9 | SRC_MASK | 源掩码 |
| 10 | INPUT_SNMP | 入接口 |
| 11 | L4_DST_PORT | 目的端口 |
| 12 | IPV4_DST_ADDR | 目的 IPv4 |
| 13 | DST_MASK | 目的掩码 |
| 14 | OUTPUT_SNMP | 出接口 |
| 15 | IPV4_NEXT_HOP | 下一跳 |
| 16 | SRC_AS | 源 AS |
| 17 | DST_AS | 目的 AS |
| 21 | LAST_SWITCHED | 流结束时间 |
| 22 | FIRST_SWITCHED | 流开始时间 |
| 23 | OUT_BYTES | 输出字节 |
| 24 | OUT_PKTS | 输出包数 |
| 27 | IPV6_SRC_ADDR | 源 IPv6 |
| 28 | IPV6_DST_ADDR | 目的 IPv6 |
| 34 | SAMPLING_INTERVAL | 采样率（部分设备为 little-endian）|
| 56 | IN_SRC_MAC | 入接口源 MAC |
| 57 | OUT_DST_MAC | 出接口目的 MAC |
| 60 | IP_PROTOCOL_VERSION | IP 版本 |
| 61 | DIRECTION | 流方向 |
| 80 | IN_DST_MAC | 入接口目的 MAC |

---

## 5. sflow: sFlow 报文解析映射

### 5.1 `ports` — 注册自定义协议端口

```yaml
sflow:
  ports:
    - proto: "udp"
      dir: "dst"
      port: 3544
      parser: "teredo-dst"
    - proto: "udp"
      dir: "both"
      port: 4754
      parser: "gre"
    - proto: "udp"
      dir: "both"
      port: 6081
      parser: "geneve"
```

| Key | 类型 | 必填 | 说明 |
|---|---|---|---|
| `proto` | string | ✅ | 传输层协议：`udp` 或 `tcp` |
| `dir` | string | ✅ | 端口匹配方向：`src`（源端口）、`dst`（目的端口）、`both`（任一）|
| `port` | uint16 | ✅ | 端口号（0–65535）|
| `parser` | string | ✅ | 注册的解析器名称（见 §7）|

### 5.2 `mapping` — 从报文层偏移提取数据

```yaml
sflow:
  mapping:
    - layer: "udp"
      offset: 48
      length: 16
      destination: csum
    - layer: "tcp"
      offset: 128
      length: 16
      destination: csum
    - layer: "ipv6"
      encap: true
      offset: 64
      length: 128
      destination: src_ip_encap
```

| Key | 类型 | 必填 | 说明 |
|---|---|---|---|
| `layer` | string | ✅ | 起始层名称（见 §7 ConfigKeyList）|
| `offset` | int | ✅ | 位偏移（从 layer 头开始算）|
| `length` | int | ✅ | 读取位数（常用 8/16/32/128/256）|
| `destination` | string | ✅ | 目标 proto 字段名 |
| `encap` | bool | 否 | 仅在 encapsulated 模式下解析，默认 false |
| `endianness` | string | 否 | 字节序 `big`（默认）/ `little` |

---

## 6. 渲染器（Renderer）详细说明

### 6.1 内置 RendererID 一览

```go
// producer/proto/render.go
const (
    RendererNone           RendererID = "none"
    RendererIP             RendererID = "ip"
    RendererMac            RendererID = "mac"
    RendererEtype          RendererID = "etype"
    RendererProto          RendererID = "proto"
    RendererType           RendererID = "type"     // 保留，未注册
    RendererNetwork        RendererID = "network"
    RendererDateTime       RendererID = "datetime"
    RendererDateTimeNano   RendererID = "datetimenano"
    RendererString         RendererID = "string"
)
```

### 6.2 各渲染器输入输出示例

| Renderer | 输入 | 输出示例 |
|---|---|---|
| `ip` | `[192,168,1,1]`（[]byte）| `192.168.1.1` |
| `ip` | `[0xfd,0x01,...]`（16字节）| `fd01::...` |
| `mac` | `0x0011223344556677`（uint64）| `00:11:22:33:44:55` |
| `etype` | `0x0800`（uint32）| `IPv4` |
| `etype` | `0x86dd`（uint32）| `IPv6` |
| `etype` | `0x0806`（uint32）| `ARP` |
| `proto` | `6`（uint32）| `TCP` |
| `proto` | `17`（uint32）| `UDP` |
| `proto` | `1`（uint32）| `ICMP` |
| `network` | `24`（uint32，配合 SrcNet）| `10.0.0.1/24` |
| `datetime` | `1727155200`（uint64，秒）| `2024-09-24T00:00:00Z` |
| `datetimenano` | `1727155200123456789`（uint64，纳秒）| `2024-09-24T00:00:00.123456789Z` |
| `string` | `[0x74,0x65,0x73,0x74]`（[]byte）| `test` |
| `none` | 任意 | 原样输出（bytes 转 hex）|

### 6.3 虚拟字段：`icmp_name`

`icmp_name` 不是 proto 字段，而是由 `ICMPRenderer` 动态计算：

```yaml
formatter:
  fields:
    - icmp_name
  render:
    icmp_name: none   # 不需要 render，由虚拟渲染器自动处理
```

输出示例：`EchoReply`、`DestinationUnreachable`、`TimeExceeded`、`Echo` 等。

---

## 7. 内置层解析器（Layer Parser）

sFlow `mapping.layer` 和 `ports.parser` 引用的层名称如下：

### 7.1 标准网络层（ConfigKey 别名）

| Parser Name | ConfigKey 别名 | 说明 |
|---|---|---|
| `ethernet` | `ethernet`、`2` | 以太网帧 |
| `dot1q` | `dot1q` | 802.1Q VLAN |
| `mpls` | `mpls` | MPLS |
| `ipv4` | `ipv4`、`ip`、`3` | IPv4 |
| `ipv6` | `ipv6`、`ip`、`3` | IPv6 |
| `ipv6-route` | `ipv6eh_routing`、`ipv6-route`、`ipv6eh` | IPv6 路由扩展头 |
| `ipv6-frag` | `ipv6eh_fragment`、`ipv6-frag`、`ipv6eh` | IPv6 分片扩展头 |
| `tcp` | `tcp`、`4` | TCP |
| `udp` | `udp`、`4` | UDP |
| `icmp` | `icmp` | ICMP |
| `ipv6-icmp` | `icmpv6`、`ipv6-icmp` | ICMPv6 |
| `gre` | `gre` | GRE 隧道 |
| `teredo-dst` | `teredo-dst`、`teredo` | Teredo 隧道 |
| `geneve` | `geneve` | Geneve 隧道 |

### 7.2 动态注册层（通过 `ports:`）

`sflow.ports` 注册的端口会自动生成以下 ConfigKey：
- `<proto><port>` — 如 `udp3544`、`tcp4754`
- `proto<proto_num>` — 如 `proto47`（GRE）

---

## 8. 完整示例

### 8.1 基础示例（华为交换机 sFlow + 自定义 csum）

```yaml
formatter:
  fields:
    - type
    - time_received_ns
    - sampling_rate
    - sampler_address
    - bytes
    - packets
    - src_addr
    - dst_addr
    - src_port
    - dst_port
    - proto
    - in_if
    - out_if
    - src_mac
    - dst_mac
    - csum               # 自定义字段（来自 sFlow 偏移）
  key:
    - sampler_address
  protobuf:
    - name: csum
      index: 999
      type: varint
  render:
    time_received_ns: datetimenano

sflow:
  mapping:
    - layer: "udp"
      offset: 48
      length: 16
      destination: csum
    - layer: "tcp"
      offset: 128
      length: 16
      destination: csum
```

### 8.2  encapsulated 内层 IP 提取（GRE/Geneve）

```yaml
formatter:
  fields:
    - src_addr
    - dst_addr
    - src_ip_encap       # 内层源 IP（可选）
    - dst_ip_encap       # 内层目的 IP（可选）
  protobuf:
    - name: src_ip_encap
      index: 1006
      type: string
      array: true
    - name: dst_ip_encap
      index: 1007
      type: string
      array: true
  render:
    src_ip_encap: ip
    dst_ip_encap: ip

sflow:
  ports:
    - proto: "udp"
      dir: "both"
      port: 4754
      parser: "gre"
    - proto: "udp"
      dir: "both"
      port: 6081
      parser: "geneve"
  mapping:
    - layer: "ipv6"
      encap: true
      offset: 64
      length: 128
      destination: src_ip_encap
    - layer: "ipv6"
      encap: true
      offset: 192
      length: 128
      destination: dst_ip_encap
    - layer: "ipv4"
      encap: true
      offset: 96
      length: 32
      destination: src_ip_encap
    - layer: "ipv4"
      encap: true
      offset: 128
      length: 32
      destination: dst_ip_encap
```

### 8.3 IPFIX/NetFlow 扩展字段（flow_direction、VRF）

```yaml
formatter:
  fields:
    - type
    - sampler_address
    - src_addr
    - dst_addr
    - bytes
    - packets
    - flow_direction
    - bi_flow_direction
    - ingress_vrf_id
    - egress_vrf_id
  protobuf:
    - name: flow_direction
      index: 42
      type: varint
    - name: bi_flow_direction
      index: 41
      type: varint
    - name: ingress_vrf_id
      index: 39
      type: varint
    - name: egress_vrf_id
      index: 40
      type: varint

ipfix:
  mapping:
    - field: 61
      destination: flow_direction
    - field: 239
      destination: bi_flow_direction
    - field: 234
      destination: ingress_vrf_id
    - field: 235
      destination: egress_vrf_id

netflowv9:
  mapping:
    - field: 34
      destination: sampling_rate
      endianness: little
    - field: 61
      destination: flow_direction
```

---

## 9. 注意事项

1. **字段名大小写**：`formatter.fields` 和 `protobuf[].name` 使用 protobuf json tag 名（小写），如 `src_addr` 而非 `SrcAddr`。
2. **index 不冲突**：`protobuf[].index` 不能与 `pb/flow.proto` 已有字段冲突（建议自定义字段 ≥1000）。
3. **endian**：NetFlow/IPFIX/sFlow mapping 默认大端；仅 Cisco NetFlow v9 的 field 34（采样率）通常小端，需显式声明 `endianness: little`。
4. **虚拟字段**：`icmp_name` 是虚拟字段，不可在 `protobuf:` 中重复定义。
5. **encap 语义**：`sflow.mapping[].encap: true` 表示仅当报文处于 encapsulation 模式（如 GRE/Geneve 内层）时才解析该层，避免重复提取外层与内层相同偏移。
6. **sflow.ports 与 sflow.mapping 联动**：`ports` 注册的解析器使对应层出现在 parser stack 中，之后 `mapping` 才能引用该层（如 `layer: "geneve"` 必须先有 `ports` 注册 6081 端口）。
7. **常量列**：`formatter.literal` 定义的列不是原生字段，值恒为常量并按字符串输出，需同时在 `formatter.fields` 中列出才会出现（见 §2.5）。