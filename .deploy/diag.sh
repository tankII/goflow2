#!/usr/bin/env bash
# GoFlow2 采集器诊断脚本（只读，不修改任何配置）
#
# 用途：定位「数据丢失」发生在哪一层。NetFlow 数据可能丢在四个位置，
#       现象相似但修复方向完全不同，必须逐层区分：
#
#   ① 内核 socket 缓冲溢出   → RcvbufErrors 增长，ss 的 r0 逼近 rb
#   ② 内核软中断处理不过来   → softnet_stat 的 time_squeeze 增长
#   ③ 网卡 ring buffer 溢出   → rx_dropped 增长
#   ④ 应用层模板缺失导致丢弃   → template_not_found 增长（解码不了，不是内核丢）
#
# 用法：
#   bash diag.sh              # 默认采样 60 秒
#   bash diag.sh 120          # 自定义采样间隔（秒）
#
# 结果输出到 stdout，同时保存到 /tmp/goflow2-diag-<时间戳>.txt

set -uo pipefail

INTERVAL="${1:-60}"
OUT="/tmp/goflow2-diag-$(date +%Y%m%d-%H%M%S).txt"
DEPLOY_DIR="${GOFLOW2_DEPLOY_DIR:-$(cd "$(dirname "$0")" && pwd)}"
HTTP_ADDR="${GOFLOW2_ADDR:-127.0.0.1:18080}"
BASE="http://${HTTP_ADDR}"

# 采集器进程：优先按部署目录下的二进制名匹配
PID=$(pgrep -f "goflow2-linux" 2>/dev/null | head -1)
if [[ -z "$PID" ]]; then
    PID=$(pgrep -x goflow2 2>/dev/null | head -1)
fi

# 从采集器启动参数里解析监听端口（用于筛 ss 输出）
LISTEN_PORT=""
if [[ -n "$PID" ]]; then
    LISTEN_PORT=$(tr '\0' '\n' < "/proc/$PID/cmdline" 2>/dev/null \
        | grep -A1 -E '^-listen$' | tail -1 \
        | sed -n 's|.*:\([0-9]\{1,5\}\).*|\1|p')
fi
[[ -z "$LISTEN_PORT" ]] && LISTEN_PORT="2055"

hr() { printf '────────────────────────────────────────────────────────────\n'; }
sec() { hr; echo "== $* =="; hr; }

# ── 内核 UDP 累计计数（取快照，稍后做差）────────────────────────
udp_snap() {
    # /proc/net/snmp 的 Udp 段：
    # InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti
    awk '/^Udp:/{if(!h){h=$0; next} print h; print $0; exit}' /proc/net/snmp 2>/dev/null
}

# ── softnet 累计计数 ────────────────────────────────────────────
# /proc/net/softnet_stat 每行以空格分隔，字段为 16 进制：
# processed dropped time_squeeze ...
# 注意：必须按 16 进制解析，直接按十进制求和会溢出成 0。
softnet_snap() {
    # 注意：/proc/net/softnet_stat 的字段是 16 进制，而 mawk/busybox awk
    # 没有 strtonum()（gawk 专有），直接按十进制求和会溢出成 0。
    # 这里手写十六进制转换，只用 POSIX awk 特性。
    awk '
    function hex2dec(s,   i, c, n, d) {
        n = 0
        for (i = 1; i <= length(s); i++) {
            c = tolower(substr(s, i, 1))
            d = index("0123456789abcdef", c) - 1
            if (d < 0) d = 0
            n = n * 16 + d
        }
        return n
    }
    {
        p += hex2dec($1); d += hex2dec($2); t += hex2dec($3);
    }
    END { printf "%d %d %d\n", p, d, t }' /proc/net/softnet_stat 2>/dev/null
}

# ── 网卡接收丢包累计计数 ────────────────────────────────────────
nic_snap() {
    local d n
    for d in /sys/class/net/*; do
        [[ -d "$d" ]] || continue
        n=$(basename "$d")
        # 跳过虚拟网卡：容器网桥、veth、隧道等不承载采集流量，
        # 列出来只会淹没真正有意义的物理网卡。
        case "$n" in
            lo|docker*|veth*|virbr*|br-*|tun*|tap*|bond*.*|dummy*|ifb*|sit*|gre*|ip6tnl*|erspan*) continue ;;
        esac
        printf '%s %s %s %s\n' "$n" \
            "$(cat "$d/statistics/rx_dropped" 2>/dev/null || echo 0)" \
            "$(cat "$d/statistics/rx_errors" 2>/dev/null || echo 0)" \
            "$(cat "$d/statistics/rx_missed_errors" 2>/dev/null || echo 0)"
    done
}

# ── 应用层队列深度 ─────────────────────────────────────────────
app_snap() {
    curl -s --max-time 3 "${BASE}/queue-stats" 2>/dev/null
}

# ── 应用层解码错误（template_not_found 等）────────────────────────
app_errors() {
    curl -s --max-time 3 "${BASE}/metrics" 2>/dev/null \
        | grep -E '^goflow2_flow_process_nf_errors_total' || echo "（无）"
}

{
echo "############################################################"
echo "# GoFlow2 诊断报告"
echo "# 生成时间: $(date '+%F %T')   采样间隔: ${INTERVAL}s"
echo "# 采集器 PID: ${PID:-未找到}   部署目录: ${DEPLOY_DIR}"
echo "############################################################"

sec "1. 主机与内核参数"
echo "核数: $(nproc)   内核: $(uname -r)   负载: $(cat /proc/loadavg)"
echo "--- 内核缓冲与队列参数（关键）---"
sysctl net.core.rmem_default net.core.rmem_max \
       net.core.netdev_max_backlog net.core.netdev_budget \
       net.core.netdev_budget_usecs 2>/dev/null
echo "提示: rmem_default 是 socket 默认接收缓冲；rmem_max 是上限。"
echo "      上限小于期望值时，内核会静默把 SO_RCVBUF 截断到上限。"
if [[ -n "$PID" ]]; then
    echo "--- 采集器线程数: $(ls /proc/$PID/task 2>/dev/null | wc -l) ---"
    echo "--- 启动参数 ---"
    tr '\0' ' ' < "/proc/$PID/cmdline" 2>/dev/null; echo
    echo "--- 生效的模板/采样率 TTL ---"
    tr '\0' '\n' < "/proc/$PID/environ" 2>/dev/null \
        | grep -E '^GOFLOW2_(TEMPLATES|SAMPLING)' || echo "（环境变量中未显式设置）"
fi
echo "--- setenv.sh 中的相关配置 ---"
if [[ -r "${DEPLOY_DIR}/setenv.sh" ]]; then
    grep -nE 'TEMPLATES_TTL|SAMPLING_TTL|UDP_RECV_BUFFER|METRICS' "${DEPLOY_DIR}/setenv.sh" \
        | sed 's/^/  /' || echo "  （无）"
else
    echo "  （找不到 ${DEPLOY_DIR}/setenv.sh）"
fi

sec "2. UDP socket 缓冲区占用"
echo "Recv-Q 是当前已占用字节；用 ss -m 可看到 skmem 的 (已占用,上限)。"
echo "若上限远小于期望值，说明 SO_RCVBUF 未生效（受 rmem_max 限制）。"
# 注意：ss -m 的 skmem 行不含端口号也不含表头文字，必须用 -A1 把
# 它跟在上方的 socket 行后面，否则最关键的缓冲上限（rb）会被过滤掉。
ss -uanm 2>/dev/null | grep -A1 -E "Recv-Q|${LISTEN_PORT}" || echo "（ss 不可用或无相关监听）"

sec "2b. 到达报文的长度分布（用于确定接收缓冲区该配多大）"
# 报文缓冲按 9000 字节分配，若实际报文远小于此则每包都在浪费内存；
# 反之若实际报文超过缓冲则会被内核静默截断、数据损坏。
# 这里实测分布，不依赖任何历史抓包样本。
if ! command -v tcpdump >/dev/null 2>&1; then
    echo "（无 tcpdump，跳过）"
elif [[ -z "$PID" ]]; then
    echo "（未找到采集器进程，跳过）"
else
    CAP=/tmp/goflow2-diag-pkt.pcap
    echo "采样 200 个报文（约 10 秒）…"
    if timeout 20 tcpdump -i any -nn -s 0 -c 200 -w "$CAP" \
            "udp dst port ${LISTEN_PORT}" 2>/dev/null; then
        if [[ -s "$CAP" ]]; then
            # tcpdump -r 输出里的 "length N" 是 UDP 负载长度
            tcpdump -r "$CAP" -n 2>/dev/null \
                | grep -oE 'length [0-9]+' | awk '{print $2}' | sort -n > /tmp/goflow2-diag-pkt.len
            CNT=$(wc -l < /tmp/goflow2-diag-pkt.len)
            if [[ "$CNT" -gt 0 ]]; then
                echo "采样报文数: $CNT"
                echo "  最小: $(head -1 /tmp/goflow2-diag-pkt.len) 字节"
                echo "  中位数: $(awk 'NR==int(NR/2)+1' /tmp/goflow2-diag-pkt.len 2>/dev/null || sed -n "$((CNT/2))p" /tmp/goflow2-diag-pkt.len) 字节"
                echo "  最大: $(tail -1 /tmp/goflow2-diag-pkt.len) 字节"
                echo "  P95: $(sed -n "$((CNT*95/100))p" /tmp/goflow2-diag-pkt.len) 字节"
                echo "  P99: $(sed -n "$((CNT*99/100))p" /tmp/goflow2-diag-pkt.len) 字节"
                MAX=$(tail -1 /tmp/goflow2-diag-pkt.len)
                echo
                echo "对照：当前接收缓冲区 9000 字节（-udp.recv.packet.size 可调）"
                if [[ "$MAX" -ge 9000 ]]; then
                    echo "  ⚠ 最大报文 $MAX 字节 ≥ 缓冲区 9000，可能发生截断！"
                    echo "    请调大 -udp.recv.packet.size，并观察 /queue-stats 的 truncated_packets"
                else
                    echo "  ✓ 最大报文 $MAX 字节 < 缓冲区 9000，无截断风险"
                    echo "    若想省内存，可将 -udp.recv.packet.size 设为略大于 $MAX（如 $(( (MAX/256+1)*256 ))）"
                    echo "    注意：调小后若 truncated_packets 增长即为配小了"
                fi
            else
                echo "（未捕获到报文，可能该端口此刻无流量）"
            fi
            rm -f "$CAP" /tmp/goflow2-diag-pkt.len
        else
            echo "（未捕获到报文）"
        fi
    else
        echo "（tcpdump 采样失败，可能需要 root 或 CAP_NET_RAW 权限）"
        echo "  可改用：sudo tcpdump -i any -nn -s 0 -c 200 'udp dst port ${LISTEN_PORT}'"
    fi
fi

sec "3. 采样开始前的累计计数"
UDP1=$(udp_snap); SOFT1=$(softnet_snap); NIC1=$(nic_snap)
echo "$UDP1"
echo "softnet (processed dropped time_squeeze): $SOFT1"
echo "$NIC1" | sed 's/^/nic rx_dropped rx_errors rx_missed: /'

sec "4. 应用层队列深度采样"
echo "depth 接近 capacity 说明应用层在积压；长期接近 0 说明包没进到这里"
echo "（可能已在内核层丢失）。"
for i in 1 2 3 4 5; do
    printf '%s  %s\n' "$(date +%T)" "$(app_snap)"
    sleep 2
done

sec "5. 应用层解码错误累计值"
echo "template_not_found 表示模板缺失导致数据流集无法解码（数据被丢弃，"
echo "与内核丢包是两回事）。数值增长即在丢数据。"
app_errors

sec "6. 等待 ${INTERVAL} 秒后再次采样"
sleep "$INTERVAL"

sec "7. 采样后的累计计数（与第 3 节做差）"
UDP2=$(udp_snap); SOFT2=$(softnet_snap); NIC2=$(nic_snap)
echo "$UDP2"
echo "softnet (processed dropped time_squeeze): $SOFT2"
echo "$NIC2" | sed 's/^/nic rx_dropped rx_errors rx_missed: /'

sec "8. 增量分析"
python3 - "$UDP1" "$UDP2" "$SOFT1" "$SOFT2" "$NIC1" "$NIC2" <<'PYEOF'
import sys

def parse_udp(text):
    lines = [l for l in text.strip().splitlines() if l.startswith('Udp:')]
    if len(lines) < 2:
        return {}
    hdr = lines[0].split()[1:]
    val = lines[1].split()[1:]
    return {k: int(v) for k, v in zip(hdr, val) if v.isdigit()}

def parse_soft(text):
    try:
        return [int(x) for x in text.split()]
    except Exception:
        return [0, 0, 0]

def parse_nic(text):
    out = {}
    for l in text.strip().splitlines():
        p = l.split()
        if len(p) == 4:
            try:
                out[p[0]] = [int(x) for x in p[1:]]
            except ValueError:
                pass
    return out

u1, u2 = parse_udp(sys.argv[1]), parse_udp(sys.argv[2])
s1, s2 = parse_soft(sys.argv[3]), parse_soft(sys.argv[4])
n1, n2 = parse_nic(sys.argv[5]), parse_nic(sys.argv[6])

print("── 内核 UDP 层 ──")
if u1 and u2:
    d_in = u2.get('InDatagrams', 0) - u1.get('InDatagrams', 0)
    d_err = u2.get('InErrors', 0) - u1.get('InErrors', 0)
    d_rcv = u2.get('RcvbufErrors', 0) - u1.get('RcvbufErrors', 0)
    total = d_in + d_err
    print(f"  收到报文 InDatagrams : {d_in}")
    print(f"  InErrors             : {d_err}")
    print(f"  RcvbufErrors         : {d_rcv}   ← socket 接收缓冲溢出")
    if total > 0:
        print(f"  内核层丢包率         : {d_err * 100.0 / total:.3f}%")
    if d_err and d_err != d_rcv:
        print(f"  ⚠ InErrors 与 RcvbufErrors 不相等，还有 {d_err - d_rcv} 次其它原因")
    if d_err == 0:
        print(f"  ✓ 本采样窗口内核层无丢包")
    if u2.get('NoPorts', 0) != u1.get('NoPorts', 0):
        print(f"  NoPorts(端口无监听)  : {u2['NoPorts'] - u1['NoPorts']}")
else:
    print("  （无法解析 /proc/net/snmp）")

print("\n── 内核软中断层 ──")
if len(s1) == 3 and len(s2) == 3:
    d_proc = s2[0] - s1[0]
    d_drop = s2[1] - s1[1]
    d_sq = s2[2] - s1[2]
    print(f"  processed            : {d_proc}")
    print(f"  dropped              : {d_drop}   ← 内核丢弃（网卡队列满等）")
    print(f"  time_squeeze         : {d_sq}   ← 软中断处理不过来（CPU 不够）")
    if d_sq > 0:
        print(f"  ⚠ time_squeeze 增长：CPU/软中断是瓶颈，调大缓冲解决不了")
    else:
        print(f"  ✓ 软中断未出现 time_squeeze")
else:
    print("  （无法解析 softnet_stat）")

print("\n── 网卡层 ──")
for name in sorted(set(list(n1.keys()) + list(n2.keys()))):
    a = n1.get(name, [0, 0, 0])
    b = n2.get(name, [0, 0, 0])
    dd = [b[i] - a[i] for i in range(3)]
    if any(dd):
        print(f"  {name}: rx_dropped+{dd[0]}  rx_errors+{dd[1]}  rx_missed+{dd[2]}")
        print(f"     ⚠ 网卡层丢包，需调 ring buffer / RSS 队列")
    else:
        print(f"  {name}: 无新增丢包")
PYEOF

sec "9. 应用层队列与错误（结束时再采一次）"
for i in 1 2 3; do
    printf '%s  %s\n' "$(date +%T)" "$(app_snap)"
    sleep 2
done
app_errors

sec "10. 进程线程 CPU（找热点线程）"
if [[ -n "$PID" ]]; then
    top -H -bn1 -p "$PID" 2>/dev/null | head -20
else
    echo "（未找到进程）"
fi

sec "11. CPU / 内存总览"
top -bn1 | head -5
free -m | head -3

sec "12. 网卡统计"
ip -s link show 2>/dev/null | head -40

echo
echo "############################################################"
echo "# 报告已保存: ${OUT}"
echo "############################################################"
} 2>&1 | tee "$OUT"
