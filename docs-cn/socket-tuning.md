# UDP Socket 容量与调优指南

## 1. 单一 Socket 的理论上限

UDP 收包的瓶颈不在 socket 本身，而在**网卡带宽**和**内核 ring buffer**：

```
网卡带宽 → 内核 ring buffer (rmem_max) → socket receive → dispatch channel → workers 解码
```

### 单 socket 承载能力估算

| 参数 | 典型值 | 说明 |
|------|--------|------|
| 标准 IPv4 报文 | ~576 B（最小）– 1500 B（MTU） | IPFIX 含模板时可达 3000–4000 B |
| 1 Gbps 网卡理论上限 | ≈ 83,000 pps | 1 Gbps ÷ (1500 × 8 bits) |
| 10 Gbps 网卡理论上限 | ≈ 830,000 pps | 大报文场景 ~500K pps |
| Linux `rmem_max` 默认 | 2–8 MB | 可通过 `sysctl net.core.rmem_max` 调大 |

**核心结论**：单 socket 承载能力 ≈ **网卡带宽 ÷ 报文大小**。GoFlow2 官方宣称"hundreds of thousands of flows per second"是指**整体吞吐**（所有 workers 合计），非单 socket 指标。

---

## 2. 配置建议

### 2.1 基础公式

```
所需 count = ceil(总入站 pps / 单 socket 承载 pps)
所需 workers = max(count × 2, ceil(总入站 pps / 单核解码 pps))
```

其中：
- **单 socket 承载 pps** = 网卡带宽 (Mbps) × 125,000 ÷ 报文平均字节数
- **单核解码 pps** = 5万 – 50万（视 template/record 复杂度）

### 2.2 场景对照表

| 场景 | 入站带宽 | 单 socket 承载 | 建议 count | 建议 workers | 备注 |
|------|---------|---------------|-----------|-------------|------|
| 小型 ISP / 边缘 | 1 Gbps | ~80K pps | 2 | 4–8 | 默认即可 |
| 中型运营商 / 城域 | 10 Gbps | ~800K pps | 4 | 8–16 | 需确认 CPU 核数 |
| 大型数据中心 | 100 Gbps | ~8M pps | 8–16 | 32–64 | 关注 NIC 多队列 |
| DDoS 突发检测 | 峰值 > 带宽 | — | 按峰值算 | workers 需预留余量 | blocking=false 兜底 |

### 2.3 调优优先级

**当出现丢包（packets dropped > 0）时**：
1. 先检查内核 ring buffer：`sysctl net.core.rmem_max`（建议 ≥ 16MB）
2. 增加 `count`（更多 socket = 更多 NIC 队列亲和）
3. 最后考虑增加 `workers`（CPU 解码能力）

**当 CPU 空闲但吞吐不达标时**：
- 通常是 `count` 不足导致单 socket 成为瓶颈
- 或报文过大（IPFIX options template 较耗时）

**当延迟过高时**：
- 降低 `queue_size`（缓冲过大会造成 buffer bloat）
- 或启用 `blocking=true`（实时模式，无缓冲）

---

## 3. 验证方法

### 3.1 压测脚本

使用 `socat` 或 `iperf` 生成密集 NetFlow 报文：

```bash
# 方法一：socat 单发模式（简单但较慢）
for i in $(seq 1 10000); do
  echo "dummy" | socat - UDP-DATAGRAM:127.0.0.1:2055
done

# 方法二：iperf3 UDP 模式（高速，接近真实）
iperf3 -c 127.0.0.1 -u -b 900M -l 1400 -t 10
```

### 3.2 关键指标监控

启动 collector 后观察以下 Prometheus 指标：

| 指标 | 含义 | 阈值 |
|------|------|------|
| `goflow2_netflow_packets_received_total` | 总接收包数 | 持续增长 |
| `goflow2_netflow_packets_dropped_total` | 丢弃包数 | **应保持为 0** |
| `goflow2_netflow_decode_errors_total` | 解码错误 | 应保持为 0 |
| `process_cpu_seconds_total` | CPU 使用 | 未打满说明有余量 |

### 3.3 快速验证步骤

```bash
# 步骤 1：记录基线（默认配置）
GOFLOW2_LISTEN="sflow://:6343,netflow://:2055" bash run.sh start

# 步骤 2：发送压测流量（另一个终端）
iperf3 -c <collector-ip> -u -b 500M -l 1400 -t 30

# 步骤 3：查看 collector 日志（run.sh 不重定向，日志走 stderr；
# 直接命令行可 `2>&1 | grep`，或设置 GOFLOW2_LOG_PATH 落盘轮转后 tail 该文件）
tail -f /var/log/goflow2.log | grep -E "drop|error|worker"

# 步骤 4：检查 Prometheus 指标（若有部署）
curl http://localhost:18080/metrics | grep goflow2_netflow

# 步骤 5：若 drop > 0，增加 count 并重试
GOFLOW2_COUNT=4 GOFLOW2_WORKERS=8 bash run.sh restart
```

### 3.4 调优决策树

```
┌─────────────────────────────────────────────────────────┐
│ 是否出现 packets_dropped > 0？                          │
└───────────────────────┬─────────────────────────────────┘
                        │
           ┌────────────┴────────────┐
           ↓                         ↓
         是                         否
           │                         │
   ┌───────┴───────┐          ✅ 当前配置满足需求
   ↓               ↓
sysctl            增加 count
rmem_max ≥ 16M？   （每个 socket 分担 NIC 队列）
   │               │
   ├─ 否 ──→ 调整 sysctl
   └─ 是 ──→ 增加 count
           │
   ┌───────┴───────┐
   ↓               ↓
CPU 是否打满？    其他问题
   │               │
   ├─ 否 ──→ 已解决
   └─ 是 ──→ 减少 count 或升级 CPU
```

---

## 4. Kubernetes 注意事项

```yaml
resources:
  requests:
    cpu: "4"      # 建议 request = limit
    memory: "8Gi" # 必须固定，否则峰值时可能被 OOM Kill
  limits:
    cpu: "4"
    memory: "8Gi"
```

- **CPU**：`request = limit` 避免被其他 Pod 抢占
- **内存**：必须设固定值，否则 GoFlow2 可能因 buffer 膨胀被 OOM Kill
- **HPA 不推荐**：NetFlow/IPFIX 冷启动时模板缓存丢失会导致短暂断流

---

## 5. 常见陷阱

| 现象 | 原因 | 解决 |
|------|------|------|
| 高频丢包 | `rmem_max` 过小 | `sysctl -w net.core.rmem_max=16777216` |
| 单 socket 拥塞 | `count` 小于 NIC 队列数 | `count >= NIC 队列数` |
| workers 空转 | `count` 过大导致调度开销 | 减少 `count`，增加 `workers` |
| 延迟突增 | `queue_size` 过大引发 buffer bloat | 减小 `queue_size` 或启用 `blocking` |
| 模板未命中 | 重启后缓存未恢复 | 配置 `-store.json.path` 持久化 |

---

## 6. 一句话总结

> **先摸清流量特征（R），再按 CPU 核数配 workers（C），最后用 queue_size/buffer 兜底突发（L）——三者平衡，才是稳定采集的关键。**

具体到 socket 层面：单 socket 承载 ≈ 带宽 ÷ 报文大小；多 socket (`count`) 用于分散 NIC 队列压力；多 worker (`workers`) 用于消化解码计算量。
