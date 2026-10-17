# GoFlow2 指标统计优化方案

> 状态：已实现、基准验证并完成现网部署验证（整机 CPU ~60%，队列稳定）
> 影响面：`metrics/`（指标埋点）、`producer/proto`（映射与 CSV 渲染）、`format/csv`、`transport/netcat`（批量写），以及 `pkg/goflow2/config`、`app`、`collector`、`httpserver` 的开关接线
> 目标：在不改变对外语义与输出字节的前提下，消除收包热路径的无谓开销（分配、反射、系统调用），并支持按需整体关闭指标统计
> 后续可继续优化的点见 8.12

## 1. 背景

GoFlow2 的 Prometheus 指标埋点直接位于**每个 UDP 报文**和**每条 flow 记录**的处理路径上。一次收包会经历：

```
UDP 收包
  └─ metrics.PromDecoderWrapper        ← 每包：IncPacket + 3 个 Traffic/PacketSize 指标 + DecoderTime
       └─ 解码
            └─ metrics.PromProducerWrapper  ← 每包：IncFlow + NetFlow/SFlow 统计
                 └─ 每条 flow：NetFlowTimeStatsSum.With(...).Observe(...)
```

热路径上存在三类固定开销，且都与流量线性相关：

1. **每包 2 次 `time.Format`**：`RouterCounter` 需要生成小时/天两个 bucket key（`2006-01-02T15:00:00`、`2006-01-02`）。
2. **每次 `.With(...)` 的 label map 构造与哈希**：Prometheus 的 `*Vec.With(labels)` 会把标签序列化成字符串 key 再做查找，标签 map 与 hash 串都会分配内存。
3. **设备统计的全局写锁**：`RouterCounter.inc` 原先整段持有 `sync.RWMutex` 写锁，多个 worker 并发写不同设备时被强行串行化。

实测单个 `RouterCounter.IncPacket` 约 **1.2 µs、4 次分配**，`NetFlowTimeStatsSum.With().Observe()` 每条 flow ≈ **358 ns**；在 15 flow/包的场景下，指标层约占每包成本的 25%–40%。

## 2. 目标与约束

- 降低热路径的 `ns/op` 与 `allocs/op`，尤其消除每包/每流的分配。
- 提供**全局指标开关**，使高吞吐/纯净部署可完全关闭指标与设备统计。
- **对外语义不变**：指标名、标签、`/metrics`、`/device-list`、`/device-stats/*` 的行为保持不变。
- 并发安全（`-race` 通过）。

## 2.1 方案总览

| 阶段 | 内容 | 关键手段 |
|------|------|----------|
| 阶段一 | 全局指标开关 | 以 `RouterCounter == nil` 作为唯一"关闭"信号；各 wrapper 自门控 |
| 阶段二 | 热路径优化 | 缓存 Prometheus child 句柄；按秒缓存时间 key；拆分全局锁；缓存逐流 observer |

---

## 3. 阶段一：全局指标开关

### 3.1 开关语义

- 新增 flag `-metrics`（默认 `true`），env `GOFLOW2_METRICS`（部署脚本映射，见 `.deploy/run.sh`）。
- **关闭时**：`RouterCounter` 为 `nil`。这个 `nil` 是整条链路的唯一"关闭"信号，各处据此短路。

### 3.2 自门控设计

所有 wrapper 在拿到 `nil` 时**原样返回被包装对象**，从而彻底跳过埋点：

```go
func WrapPromProducer(wrapped producer.ProducerInterface, rc *RouterCounter) producer.ProducerInterface {
    if rc == nil {
        return wrapped // 关闭：直接返回，不包一层
    }
    ...
}

func PromDecoderWrapper(wrapped utils.DecoderFunc, name string, rc *RouterCounter) utils.DecoderFunc {
    if rc == nil {
        return wrapped // 关闭：直接返回
    }
    ...
}
```

接线处：

- `pkg/goflow2/app/app.go`：`rc == nil` 时不创建 `RouterCounter`，也不挂载 template/sampling-rate 的 store hooks。
- `pkg/goflow2/collector/collector.go`：`receiverCallback()` / `templateStoreOptions()` 在关闭时返回 `nil`，不设置 receiver 回调与 fallback 模板钩子。

### 3.3 端点行为（关闭时）

- `/device-list`：返回 `200` + `[]`
- `/device-stats/hourly`、`/device-stats/daily`：返回 `200` + 空的 `{router,window,buckets:[]}`（由 `writeDeviceBuckets` 统一输出）
- `/metrics`、`/queue-stats`、`/transport-stats`、`/ui`：照常提供；`/metrics` 中 goflow2 自有 series 保持注册但值为 0（"注册但不更新"）

> 端点始终注册（见 `pkg/goflow2/httpserver/httpserver.go`），只是数据为空，便于前端与采集端无需感知开关。

---

## 4. 阶段二：热路径优化

### 4.1 设备统计 `RouterCounter`（`metrics/router_counter.go`）

**优化前**：`inc` 持全局写锁，每包执行 2 次 `time.Format`，并对 3 个 Gauge 各做一次 `.With(labels).Set/Add`（每次都要重建并哈希 label map）。

**优化措施（三点）**：

1. **缓存 Prometheus child 句柄**
   每个 `deviceStats` 在首次出现时解析一次 `lastSeenG/packetsG/flowsG`（`DeviceLastSeenGauge.With(...)` 等），此后热路径直接 `Set/Add`，不再走 `.With` 的 label map 构建与哈希。

2. **按秒缓存时间 key**
   `deviceStats` 内缓存 `keySec/hourKey/dayKey`，仅当 `now.Unix()` 变化时才重新 `time.Format`，同一秒内所有报文复用字符串。

3. **锁拆分（去全局写锁串行）**
   `RouterCounter.mu`（`sync.RWMutex`）只保护 **map 成员关系**，每个 `deviceStats` 自带 `sync.Mutex` 保护自身 bucket：
   - 快路径：`RLock` 查表 → 命中则取 `ds.mu` 更新 → 不同设备可并行；
   - 慢路径：首次出现的设备在写锁内创建；
   - 快路径**全程持有 `RLock`**，从而阻止 prune/`Devices()` 在其更新期间把该设备删除。
   - 读路径（`Devices`/`GetHourly`/`GetDaily`/`sweep`）持写锁，天然与快路径互斥，故读取 bucket 时无需再取 `ds.mu`。

```go
// 快路径（metrics/router_counter.go:123）
rc.mu.RLock()
ds := rc.devices[router]
if ds != nil {
    ds.mu.Lock()
    applyCounters(ds, rc.now(), packets, flows, bytes) // metrics/router_counter.go:165
    ds.mu.Unlock()
    rc.mu.RUnlock()
    return
}
rc.mu.RUnlock()
```

> 锁顺序始终是 `RouterCounter.mu` → `deviceStats.mu`，不存在反向获取，无死锁。

### 4.2 逐流直方图 `PromProducerWrapper`（`metrics/producer.go`）

**优化前**：`for _, msg := range flowMessageSet { NetFlowTimeStatsSum.With(prometheus.Labels{"router":key,"version":versionStr}).Observe(...) }`，每条 flow 都重建并哈希一次 label map。

**优化措施**：按 `(router, version)` 缓存 `prometheus.Observer`（`sync.Map`），**每包只解析一次**，循环内直接 `Observe`：

```go
obs := p.timeStatsObserver(key, versionStr) // 每包一次
for _, msg := range flowMessageSet {
    obs.Observe(...)
}
```

- 基数上界 = （路由器数量 × NetFlow/IPFIX 版本数），可控，不会随流量增长。
- `LoadOrStore` 保证并发下同一 key 只缓存一个句柄。

---

## 5. 基准测试结果

环境：Intel Core i7-4790 @ 3.60GHz。运行：

```bash
go test ./metrics/ -run '^$' -bench 'RouterCounterIncPacket' -benchmem
go test ./metrics/ -run '^$' -bench 'NetFlowTimeStats' -benchmem
```

| 基准 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| `RouterCounterIncPacket` | 1204 ns/op, 4 allocs | **194 ns/op, 0 allocs** | 6.2× |
| `RouterCounterIncPacketParallel` | 1291 ns/op, 4 allocs | **165 ns/op, 0 allocs** | 7.8× |
| `NetFlowTimeStats`（逐流直方图） | 358.3 ns/op | **244.1 ns/op** | 1.47× |

要点：

- 单线程 6.2×、多协程 7.8×，说明**去全局写锁**同时消除了竞态串行（并行版甚至快于单线程）。
- 热路径分配归零，显著降低 GC 压力。
- 逐流直方图的收益来自"每包一次"替代"每流一次"的 label map 构建/哈希。

测试用例位于 `metrics/router_counter_test.go`、`metrics/wrapper_test.go`。

---

## 6. 正确性与兼容性

- 对外指标名、标签、端点响应语义保持不变；设备列表/桶的 JSON 结构未变。
- 关闭开关时设备端点返回 `200` 空数据，前端无需改动。
- `-race` 下 `metrics` 包全部测试通过，覆盖并发 `inc` 与 `Devices()` 读写。
- `applyCounters` 的 bucket key 由每设备缓存，测试仍可通过 `rc.now` 注入任意时间（秒变化即重算），行为与旧实现一致。

---

## 6.1 相关代码索引

| 位置 | 说明 |
|------|------|
| `metrics/router_counter.go:76` | `RouterCounter`（map RWMutex + 每设备 mutex） |
| `metrics/router_counter.go:123` | `inc` 快/慢路径 |
| `metrics/router_counter.go:165` | `applyCounters`（更新 bucket + 缓存句柄/时间 key） |
| `metrics/router_counter.go:242` / `:264` | `GetHourly` / `GetDaily` |
| `metrics/decoder.go:17` | `PromDecoderWrapper`（`nil` 时自门控） |
| `metrics/producer.go:177` | `WrapPromProducer`（`nil` 时自门控） |
| `metrics/producer.go:136` | 逐流 observer 缓存使用点 |
| `pkg/goflow2/config/config.go:64` | `-metrics` flag |
| `pkg/goflow2/app/app.go` | 创建/接线 `RouterCounter` 与 store hooks |
| `pkg/goflow2/collector/collector.go` | receiver 回调 / 模板钩子门控 |
| `pkg/goflow2/httpserver/httpserver.go` | 设备端点注册与空数据处理 |

---

## 7. 诊断：pprof 性能剖析

> 定位"接收队列（dispatch）持续上涨"等吞吐/CPU 问题的首选工具。默认关闭，仅在排查时临时开启。

### 7.1 启用

GoFlow2 内置 `net/http/pprof`，挂在 HTTP 状态端口（`-addr`，部署默认 `:18080`）。两种启用方式：

| 方式 | 做法 |
|------|------|
| 环境变量（run.sh） | `GOFLOW2_PPROF=true bash run.sh restart` |
| 命令行 flag | `-pprof`（默认 `false`） |

启动摘要中出现 `pprof    : true` 即生效。

### 7.2 端点

| 端点 | 用途 |
|------|------|
| `/debug/pprof/` | 总览索引 |
| `/debug/pprof/profile?seconds=30` | CPU 采样（默认 30s） |
| `/debug/pprof/heap` | 当前/累计堆分配 |
| `/debug/pprof/goroutine?debug=1` | 全量协程栈（判断阻塞点） |
| `/debug/pprof/cmdline`、`/symbol`、`/trace` | 命令行、符号表、执行 trace |

### 7.3 采集

```bash
# 状态端口默认 :18080
BASE=http://127.0.0.1:18080

# CPU：在队列上涨时采 30s
curl -s "$BASE/debug/pprof/profile?seconds=30" -o /tmp/cpu.pb.gz

# 堆（分配热点）
curl -s "$BASE/debug/pprof/heap" -o /tmp/heap.pb.gz

# 协程栈（判断是否被阻塞）
curl -s "$BASE/debug/pprof/goroutine?debug=1" > /tmp/goro.txt
```

> 也可以不落盘，直接用 Go 环境在线分析：`go tool pprof -top http://127.0.0.1:18080/debug/pprof/profile?seconds=30`（仍需端点已启用）。

### 7.4 分析

```bash
BIN=/home/ipms/goflow2/bin/goflow2-linux-amd64

# CPU 热点（flat = 函数自身耗时）
go tool pprof -top -nodecount=40 "$BIN" /tmp/cpu.pb.gz

# 调用链累计耗时（cum）
go tool pprof -top -cum -nodecount=40 "$BIN" /tmp/cpu.pb.gz

# 分配热点：累计分配字节
go tool pprof -top -sample_index=alloc_space "$BIN" /tmp/heap.pb.gz
```

### 7.5 判读

| 热点（top/cum 靠前） | 根因指向 | 相关位置 |
|------|----------|----------|
| `flowstore.(*Store).get` / `fireHooks` / `metrics...TemplateStoreHooks` | 每次模板查询走全局写锁 + 每次分配（每数据集一次），worker 间串行 | `pkg/flowstore/store.go:302` |
| `prometheus.(*MetricVec).With` / `hashLabels` | 每包/每流重建并哈希 label map | `metrics/decoder.go`、`metrics/producer.go` |
| `runtime.gc*` / `mallocgc` | 分配过剩导致 GC 压力 | 各热路径分配 |
| `producer/proto` 渲染、`format/csv` | 格式化/渲染耗时 | `producer/proto/messages.go`、`format/csv` |
| `netcat` / `bufio` 写 | 传输侧堆积（默认 `drop=true` 只丢不阻塞） | `transport/netcat/netcat.go` |

- 若 **CPU 已打满**（`top -H -p <pid>` 接近核数）→ 瓶颈在 CPU，对照上表找热点函数。
- 若 **CPU 未打满**但队列仍涨 → worker 被阻塞，查看 `/tmp/goro.txt`：统计卡在 channel/lock 上的 worker 栈即可定位。

### 7.6 安全

`/debug/pprof/` 无鉴权，且会暴露进程内部信息，只应在排查期间临时开启，用完立即关闭：

```bash
GOFLOW2_PPROF=false bash run.sh restart
```

---

## 8. 现网剖析与热路径优化（阶段三 / 阶段四）

> 起点：用第 7 节的 pprof 对现网抓取 30s CPU / 堆 / 协程，定位到**机器 CPU 打满（采样 181.7s / 30s wall ≈ 607%，约 6 核）**，瓶颈不在收发阻塞，而在 **producer 映射与 CSV 渲染**。
>
> 终点：四批优化落地并部署验证（见 8.8），**整机 CPU 降至约 60%，接收队列多数时间 < 1000，吞吐与消费基本持平**。
>
> 本节记录逐轮剖析证据、已落地优化与基准，另含排障记录（8.9）、工具（8.10）与**可继续优化的点（8.12）**。

| 批次 | 内容 | 小节 |
|---|---|---|
| 阶段三 | 映射键去 `Sprintf`、缓存字段下标、跳过注定失败的 `FieldByName` | 8.3 – 8.6 |
| 阶段四 | netcat 批量写、CSV 追加式渲染（B1）、原生标量列直写（B2） | 8.7 |

### 8.1 现场结论

- `goroutine`：20 个 netcat writer 正常等待、4 个 UDP 收包协程，**无锁阻塞/死锁**；`netcat` 默认 `drop=true` 只丢不阻塞。
- CPU 总采样 **181.72s / 30s wall ≈ 605%** → 收包速率 > 流水线消费速率 → 输入（dispatch）队列上涨。
- 流水线占比（cum）：`NetFlowPipe.DecodeFlow` 67%、`PromProducerWrapper.Produce` 31%、`ConvertNetFlowDataSet` 25%、`flowpipe.formatSend`（CSV）23%、`netcat` 写 13%。

### 8.2 热点排名（CPU / 分配）

| 热点 | CPU | 分配 | 位置 |
|------|-----|------|------|
| 映射键 `fmt.Sprintf("%v-%d-%d", ...)`（每个 data field 一次） | **24.3s (13.4%)** | 5.1GB | `producer/proto/config_impl.go` `NetFlowMapper.Map` |
| CSV 渲染 `renderFieldParts` / `CSVValues` | 22.2s | **30.8GB (35%)** | `producer/proto/messages.go` |
| netcat 每行 `Flush`→`write` 系统调用 | 21.0s (11.6%) | — | `transport/netcat/netcat.go` |
| 反射 `vfm.FieldByName(dest)`（每字段一次） | 9.0s (4.9%) | 3.3GB | `producer/proto/reflect.go` `MapCustom` |
| 解码 `DecodeDataSetUsingFields` | — | 13.7GB (16%) | `decoders/netflow` |
| `flowstore.Store.get`（模板查询） | — | 2.96GB (3.4%) | `pkg/flowstore/store.go:302` |

> 注：早先"flowstore 写锁 + 每次分配""decoder 每包 `.With`"的假设经剖析为**次要**（3.4% 分配 / 约 8%），主因是上表前两项。

### 8.3 已落地优化（阶段三）

**#1 映射键去 `Sprintf`（`producer/proto/config_impl.go`）**

`NetFlowMapper.data` 的键由 `fmt.Sprintf("%v-%d-%d", penProvided, pen, type)` 字符串改为结构体键 `netFlowMapKey{penProvided, pen, fieldType}`，构建侧（`mapFieldsNetFlow`）与查找侧（`Map`）一致修改，热路径零格式化、零分配。

**#2 缓存目标字段索引（`producer/proto/reflect.go` + `config_impl.go`）**

在配置构建期用 `protoMessageType.FieldByName(destination).Index` 解析一次字段下标，存入 `MapConfigBase.fieldIndex`（新增 `GetFieldIndex()`）；`MapCustom` 优先 `vfm.FieldByIndex(idx)`，仅对未解析的临时配置回退 `FieldByName`。复用既有 `compiledPlan` 的 `fieldIndex` / `FieldByIndex` 模式（`producer/proto/messages.go`）。

```go
// config_impl.go：构建期解析一次
func resolveFieldIndex(v *MapConfigBase) {
    if v.Destination == "" {
        return
    }
    if sf, ok := protoMessageType.FieldByName(v.Destination); ok {
        v.fieldIndex = sf.Index
    }
}

// reflect.go：热路径直接按下标取值，未解析才回退 FieldByName
if fi, ok := cfg.(interface{ GetFieldIndex() []int }); ok {
    if idx := fi.GetFieldIndex(); len(idx) > 0 {
        fieldValue = vfm.FieldByIndex(idx)
    }
}
if !fieldValue.IsValid() {
    fieldValue = vfm.FieldByName(cfg.GetDestination())
}
```

### 8.4 基准结果

```bash
go test ./producer/proto/ -run '^$' -bench 'NetFlowMapperMap|MapCustom|CustomMappingIPFIX' -benchmem
```

| 基准 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| `BenchmarkNetFlowMapperMap` | 185.7 ns/op, 2 allocs | **31.9 ns/op, 0 allocs** | 5.8× |
| `BenchmarkMapCustom` | 1307 ns/op, 6 allocs | **250 ns/op, 1 alloc** | 5.2× |
| `BenchmarkCustomMappingIPFIX/struct-field` | 1220 ns/op, 7 allocs | **83 ns/op, 0 allocs** | 14.7× |
| `BenchmarkCustomMappingIPFIX/unknown-proto` | 302 ns/op, 4 allocs | 296 ns/op, 4 allocs | 持平（未回归） |

要点：映射查找与逐字段取值两条热路径的分配**全部归零**；`unknown-proto`（动态 protobuf，不走映射键）保持不变。回归测试见 `producer/proto/mapper_test.go`（键区分 `PenProvided`、构建期字段下标解析、`FieldByName` 回退）。

### 8.5 相关代码索引（阶段三）

| 位置 | 说明 |
|------|------|
| `producer/proto/config_impl.go` `netFlowMapKey` | 映射键结构体（替代 `Sprintf` 字符串键） |
| `producer/proto/config_impl.go` `mapFieldsNetFlow` | 构建期写入结构体键 + 解析字段下标 |
| `producer/proto/config_impl.go` `resolveFieldIndex` | 解析一次目标字段下标（并标记"已解析"） |
| `producer/proto/config_impl.go` `MapConfigBase.fieldIndex` / `GetFieldIndex` | 缓存字段下标 + 访问器 |
| `producer/proto/reflect.go` `MapCustom` | 优先 `FieldByIndex`，回退 `FieldByName` |
| `producer/proto/mapper_test.go` | 回归测试 |

### 8.6 第二轮剖析：残余 `FieldByName` 根因与修复

重建部署后再抓 30s profile（Build ID 变化，`fmt.Sprintf` 已消失 → **#1 已生效**；`unknown-proto` 基准不变），但 `reflect.FieldByName` 仍占 **15s (8.2%)**，且 100% 来自 `MapCustom` 回退分支。

根因：现网 `mapping.yaml` 的 `destination` 为 **snake_case 非 Go 字段名**（`flow_direction`、`sampling_rate` 等，属"手动 protobuf 字段"或解码器原生处理项）。`resolveFieldIndex` 对这类目的地解析不到 Go 字段，`fieldIndex` 保持 `nil`，于是 `MapCustom` 对**每条记录**仍回退到 `reflect.FieldByName`（~100 字段线性扫描 + 分配，查找必然失败）——纯浪费。

修复（**#2b**，`producer/proto`）：

- `resolveFieldIndex` 把"已解析"状态显式化：编译期构建的 mapping 一律置 `fieldIndex` 为**非 nil**（解析不到 Go 字段则为空切片 `[]int{}`）。
- `MapCustom` 改为三态：`resolvedFieldIndex(cfg)` 返回 `(idx, resolved)`；已解析且 `len(idx)>0` → `FieldByIndex`；已解析但空 → **直接走 proto-unknown 路径，跳过 `FieldByName`**；仅临时（未解析）配置才回退 `FieldByName`。

行为不变：跳过 `FieldByName` 的那类目的地本来也解析不到（构建期与运行期类型一致，结果确定相同）。

基准：

| 基准 | 修复前 | 修复后 | 提升 |
|------|--------|--------|------|
| `BenchmarkMapCustomNonFieldDestination`（`flow_direction` 类目的地） | 1125 ns/op, 4 allocs | **188 ns/op, 1 alloc** | 6.0× |
| `BenchmarkMapCustom`（`SrcAddr` 类 Go 字段） | 238 ns/op, 1 alloc | 持平 | — |
| `BenchmarkNetFlowMapperMap` | 31.8 ns/op, 0 alloc | 持平 | — |

> 剩余 alloc 为基准内 `&ProtoProducerMessage{}`（生产路径走 `sync.Pool`）。回归测试 `TestMapFieldsNetFlowMarksNonFieldDestinationResolved` + 原有 `producer/proto` 全套（`-race`）保障。

### 8.7 第三批：netcat 批量写 + CSV 追加式渲染（阶段四）

针对第二轮 profile 剩下的两项（netcat `Syscall6` 22.2s、CSV 渲染 ~52s / 30.8GB）。

#### A. netcat 批量写（`transport/netcat`）

**优化前**：每条 flow 一次 `writeLine` → `bufio.Writer.Flush()` → **一次 `write` 系统调用**。

**优化措施**：`serve()` 改用 64KiB `bufio.Writer` 累积记录，仅在**缓冲写满**或**200ms ticker 到期**时 `Flush`：

```go
case data := <-t.queue:
    if err := d.writeLine(w, data); err != nil { return err }   // 只写缓冲，不 Flush
    if w.Buffered() == 0 { continue }                            // 写满已自动下发给内核
case <-ticker.C:
    if err := w.Flush(); err != nil { return err }
case <-t.done:
    return drain()                                               // 关闭前排空并 Flush
```

- **语义不变**：行内容、分隔符、每目标独立连接、队列满 `drop=true` 丢弃全部保持原样；关闭时仍排空队列并 Flush。
- 下游为 flume sink（写入 HDFS），本身就有 channel + 大批量落盘，200ms 延迟完全无感。
- 新增可调项（run.sh 已接线）：

| 环境变量 | flag | 默认 |
|---|---|---|
| `GOFLOW2_NETCAT_FLUSH_INTERVAL` | `-transport.netcat.flush.interval` | `200ms` |
| `GOFLOW2_NETCAT_WRITE_BUFFER` | `-transport.netcat.write.buffer` | `65536` |

回归测试 `TestServeBatchesWrites`（200 条记录合并为远少于 200 次写，且全部按序送达）、`TestServeFlushesWhenIdle`（空闲时残余数据仍会被 flush）。

#### B. CSV 追加式渲染（`producer/proto` + `format/csv`）

**优化前**（每条 flow 一行 14 列）：`make([]fieldPart, 14)`≈1.1KB → 逐列 `Interface()` 装箱 → renderer/格式化字符串 → `CSVValues` 返回 `[]string` → `strings.Join`（整行拷贝 1）→ `[]byte(...)`（整行拷贝 2）。实测 **33 allocs / 1128 B**。

**优化措施**：

1. `[]fieldPart` 改为 `sync.Pool` 复用（`acquireFieldParts` / `releaseFieldParts`，归还前清空 `values`/`indices` 引用，避免池内滞留消息数据）。
2. 新增 `AppendCSVRow(dst []byte, delim string) []byte`：直接往字节缓冲追加，**绕过 `[]string` 与 `strings.Join`**；转义规则由 `appendCSVField` 原地完成（`csvNeedsQuote` 与原 `csvEscape` 判定等价）。
3. `appendCSVValues(dst []string)` 提供可复用列切片；`CSVValues()` 退化为 `appendCSVValues(nil)`，行为不变。
4. `format/csv` 增加可选接口 `csvRowAppender`：命中则走追加路径，缓冲从池取；因 transport（netcat 队列）会持有返回切片，仍需**一次**拷贝交给调用方。未实现该接口的消息（如仅 `CSVValues()`）走原路径，兼容性不变。

**输出逐字节一致**由测试保证：`TestAppendCSVRowMatchesLegacy` 对 3 种分隔符逐字节比对旧的 `CSVValues + csvEscape + strings.Join` 管线；另有转义、空列、数组列、缓冲复用、重复渲染稳定性等用例；`format/csv` 侧 `TestFormatAppendPath` 断言快慢两条路径字节一致。

基准（14 列，贴近现网 formatter）：

| 基准 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| `BenchmarkCSVRowLegacy` | 2824 ns/op, 1128 B, 33 allocs | — | — |
| `BenchmarkCSVRowAppend` | — | **2060 ns/op, 296 B, 26 allocs** | 27% / **3.8× 内存** |
| `BenchmarkCSVRowAppendCopy`（含 driver 的 1 次拷贝） | 2824 ns/op, 1128 B, 33 allocs | **2175 ns/op, 408 B, 27 allocs** | 23% / **2.8× 内存** |

剩余分配主要来自逐列 `reflect.Value.Interface()` 装箱与 renderer 返回的字符串 —— 属于 **B2**，见下节。

#### C. B2：原生标量列直写（`producer/proto`）

**思路**：`compiledField` 新增 `directScalar`，在 `compilePlan` 阶段判定"该列是 protobuf 原生标量（uint/int/bool）、非数组、且用户未自定义 renderer"。这类列无需装箱成 `interface{}` 再交给 renderer。

```go
// 判定在构建期完成，热路径零成本
cf.directScalar = cf.hasField && !cf.isArray && !cf.hasRenderer && scalarDirect(fieldKind)
```

- `renderFieldParts` 对 `directScalar` 列**跳过** `fieldValue.Interface()` 与 renderer 调用；text/JSON 路径仍用 `renderScalar` 得到字符串，**输出与旧实现完全一致**。
- `AppendCSVRow` 调用 `renderFieldParts(sp, /*skipDirectScalar=*/true)`，直接 `strconv.AppendUint/AppendInt/AppendBool` 写进目标缓冲，**连中间字符串都不产生**。
- **安全性**：只有未配置 renderer 的列才走直写。`mapFormat` 会预置 `defaultRenderers`（`Proto`→ProtoRenderer、`SrcNet`→NetworkRenderer、`Etype`→EtypeRenderer、`SrcAddr`→IPRenderer 等），这些列 `hasRenderer=true`，不会被直写覆盖 —— 回归测试 `TestAppendCSVRowRendererOnNumericColumn` 专门锁定这一点。
- 判定与 `renderValue` 的标量分支严格一致（`scalarDirect` 与之对齐），因此格式结果逐字节相同。

基准（14 列、贴近现网 formatter）：

| 基准 | 旧管线（Legacy） | B1 | **B2（含 driver 拷贝）** |
|---|---|---|---|
| ns/op | 2475 | 2175 | **1415** |
| B/op | 1064 | 408 | **272** |
| allocs/op | 23 | 27→ | **10** |

即相对旧管线 **1.75× 提速、3.9× 内存下降、23→10 次分配**；原生标量行本身已降至 **0 次分配**（`TestAppendCSVRowScalarNoAlloc`）。

剩余 10 次分配来自仍走 renderer 的列（`SrcAddr`/`DstAddr`/`SamplerAddress` 的 `IPRenderer`），属于下一步候选。

### 8.8 现网验证结果

新包 `dist/goflow2-deploy-baseline-20261017-114611.tar.gz` 部署后的实测：

| 指标 | 优化前 | 优化后 |
|---|---|---|
| goflow2 进程 CPU | 采样 181.7s/30s ≈ **607%**（约 6 核打满） | **整机 ~60%** |
| 接收（dispatch）队列 | 持续上涨 | **多数时间 < 1000** |
| 吞吐与消费关系 | 消费 < 收包，队列无界增长 | **基本持平** |

即：瓶颈已从 goflow2 自身移走，流水线可与收包速率持平，CPU 获得约 40% 的整机余量用于流量增长。

### 8.9 排障记录：CSV 时间字段「丢失毫秒」（非缺陷）

**现象**：现网多数记录输出 `2026-10-17 12:07:19`，少数带毫秒（`.8`、`.83`）。

**结论：不是缺陷，也不是本轮优化引入的回归。** 用 HEAD（改动前）与当前二进制对同一构造包做 A/B，输出**逐字节一致**（见 8.3 回归测试）。

两个叠加原因：

1. **`.999` 是 Go 的"省略末尾零"布局**。`2006-01-02 15:04:05.999` 在毫秒为 0 时整段不输出；`.000` 才是固定 3 位。旧的 `datetimenano`（RFC3339Nano）同样会省略末尾零。
2. **亚秒精度取决于设备上报的字段与粒度**。关键算式（`producer/proto/producer_nf.go:521`）：

   ```go
   timeDiff := (uptimeNs - uint64(timeFirstSwitched)*1e6) // 整数"毫秒"之差
   flowMessage.TimeFlowStartNs = baseTimeNs - timeDiff    // baseTimeNs = UnixSeconds*1e9（整秒）
   ```

   输出毫秒位 = `(uptime − firstSwitched) mod 1000`。

| 设备类型 | 模板含 FIRST/LAST_SWITCHED | switched 粒度 | 输出毫秒 |
|---|---|---|---|
| A | 否 | — | 无（用报文头 `unixSeconds`，整秒） |
| B | 是 | **秒级**（值 %1000 == 0） | 无 |
| C | 是 | **毫秒级**（值 %1000 ≠ 0） | 有 |

三者**都是 NetFlow v9**；版本相同不代表模板字段相同。判读特征：`start` 比 `end` 早**整数秒**（如 63s/73s）→ B 型。

**处理建议**：

- 格式对齐（立即可做）：把 layout 由 `.999` 改为 `.000`，列宽固定为 3 位，下游解析稳定。**注意这只是格式对齐，不会给 B 型设备补出真实毫秒**。
- 数值精度（需设备侧）：B 型设备的亚秒信息在源数据中即不存在，需网络侧确认设备上报配置。

**数据质量旁证**：观察到 `time_flow_start_ns > time_flow_end_ns` 的记录（如 `12:12:01 | 12:11:57.83`），属设备侧时钟/上报异常，与 goflow2 无关，建议单独排查。

### 8.10 工具：`cmd/nfdump`（模板诊断）

用于逐设备核实上表 A/B/C 型。解析 NetFlow v9 报文头与模板，打印字段清单并判定是否含时间字段：

```bash
# 抓某设备的原始包（模板非每包都带，需抓够）
tcpdump -i any -nn -s0 -c20 'udp dst port 2055 and src host 120.204.194.2' -X > /tmp/dev1.hex
dist/bin/nfdump < /tmp/dev1.hex
```

> ⚠️ 命名与 NetFlow 领域经典的 C 工具 `nfdump` **同名但无关**，本工具仅用于解码模板，不做流量统计。

### 8.11 代码注释约定

本文件涉及的**性能关键路径**（`producer/proto`、`format/csv`、`transport/netcat` 中的反射映射、CSV 渲染、netcat 批量写）统一使用**中文注释**，说明优化动机与行为约束；其余公共 API 的文档注释保持英文。

### 8.12 可继续优化的点（按预估收益排序）

> 下列均为**未实施**项，基于优化前 profile 的剩余热点。收益估算基于当时占比，实际需重新剖析确认。

| 优先级 | 候选 | 现状证据 | 建议做法 | 预期收益 |
|---|---|---|---|---|
| **P0** | **解码侧分配** | `DecodeDataSetUsingFields`、`DecodeMessageCommonFlowSet` 堆分配 **13.7GB（16%）**，是优化后最大的单块 | 逐字段 `switch` 解码改为按模板缓存的解码函数指针；`GetBytes` 结果用 `sync.Pool` 复用；避免 `[]byte` 装箱 | 预计再降 10–15% 整机 CPU |
| **P1** | **IP 列直写** | `IPRenderer` 走 `netip.Addr.String()`，分配 0.97GB | 为 `compiledField` 增加 `appendTo(dst []byte) []byte` 可选实现；`netip.Addr` 本身提供 `AppendTo` | 消除 3 个 IP 列的分配/消息 |
| **P1** | **时间列直写** | `time.Time.appendFormat` 5.2s CPU；`datetimenano` renderer 返回字符串 | 同上，`time.Time.AppendFormat` 直写缓冲 | 省 2 个时间列的字符串分配 |
| **P2** | `flowstore.get` | 模板查询持**全局写锁** + 每次 `make([]hookEvent,…)`，2.96GB（3.4%） | 改为 `RWMutex` 读锁 + 同步.Once/无锁 hook 快照；模板加读写比优化 | 解除 worker 间串行 |
| **P2** | 逐流直方图 | `NetFlowTimeStatsSum` 仍每条 flow 观测一次 | 改为按包聚合或抽样 | 高基数场景下可观 |
| **P3** | `RouterCounter` 分片 | `devices` map 读端 cache line 争用 | 按 router hash 分片，每片独立 `RWMutex` | 极高核数/设备数时才显现 |
| **P3** | 指标路径地址字符串 | `PromDecoderWrapper`/`Produce` 每包 `netip.Addr.String()` | 单包内复用已可比较的 `Unmap()` 地址 | 与 P1 IP 列部分重叠 |

**实施建议顺序**：P0 解码分配收益最大且与已完成的渲染优化解耦，风险低；P1 两项可合并为"renderer 直写旁路"一次改动；P2/P3 收益递减，按现场压力择机。

**验证前提**：任何一项动手前，先用第 7 节的 pprof 重新采集**当前部署版本**的 profile（确认 Build ID 已变化），避免在已解决的热点上重复投入。

### 8.13 已定位的累积型问题：模板 TTL 默认为 0（永不过期）

**现象**：部署后短时间内 CPU 正常，但**运行数小时后 CPU 持续爬升**。

**根因**：

```
pkg/goflow2/config/config.go:79
  fs.DurationVar(&cfg.TemplatesTTL, "templates.ttl", 0, "NetFlow/IPFIX templates TTL (0 disables expiry)")
utils/store/templates/store.go:137
  if s.ttl > 0 { storeOpts = append(storeOpts, flowstore.WithDefaultTTL(...)) }
pkg/flowstore/store.go:521
  if !existing && s.defaultTTL > 0 { ent.expiresAt = now.Add(s.defaultTTL) }
  return time.Time{}   // ← expiresAt 零值 = 永不过期
```

清理机制本身是**存在**的（`TemplateFlowStore` 每 60s 跑一次后台 sweep，`TestTemplateFlowStoreExpiresAndPrunes` 覆盖），但默认 `ttl=0` → 不设 `defaultTTL` → `expiresAt` 恒为零值 → **sweep 每分钟醒来却发现没有任何条目过期**。`sampling.ttl` 同理。

后果：模板 key 为 `{RouterKey, Version, ObsDomainID, TemplateID}`，条目随「设备数 × 观测域 × 模板 ID」**只增不减**；而 `pkg/flowstore/store.go:302` 的模板查询取**全局写锁**并每次 `make([]hookEvent, …)`，基数增长会持续放大 worker 间串行化。

> 这也修正了本文 8.2 的判断：该处把 `flowstore` 评为"次要（3.4%）"，但那是**刚部署、模板存量很少时**的 30s 切片，无法反映稳态下的基数增长。

**处置（仅配置，无需改代码）**：`app.go:97` 已把 `cfg.TemplatesTTL` 传入 store，加环境变量即生效。

```bash
# setenv.sh 中配置（建议 1h；模板标准刷新周期 60s，留足余量）
export GOFLOW2_TEMPLATES_TTL=1h
export GOFLOW2_SAMPLING_TTL=1h
```

**验证方式**：

```bash
# 启动日志应显示 templates_ttl 1h0m0s，而非 0s
curl -s localhost:18080/metrics | egrep 'template_entries|sampling_rate_entries'
# 间隔数小时再采：条目数应稳定在"在线设备数 × 模板数"量级，不再单调上涨
```

> 注意：TTL 不得小于模板刷新周期，否则会反复丢模板并触发 fallback 解码。

---

## 9. 相关文档

| 文档 | 内容 |
|---|---|
| `docs-cn/socket-tuning.md` | 队列/缓冲调参、内核丢包、接收队列上涨的处置 |
| `docs-cn/mapping-reference.md` | `mapping.yaml` 字段与 renderer 配置参考（时间列 layout 见 8.9） |
| `docs-cn/logging.md` | 日志轮转与排障 |

> 后续优化项已统一收敛到 **8.12**，避免多处维护。
