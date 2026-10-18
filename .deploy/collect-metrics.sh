#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# GoFlow2 现网指标采集脚本
#
# 用法:
#   ./collect-metrics.sh              采集一次快照
#   ./collect-metrics.sh 600          采集两次(间隔 600 秒)并计算速率
#   ./collect-metrics.sh 600 pprof    同时抓取 heap/cpu/goroutine 快照
#
# 环境变量:
#   GF_BASE       状态端口地址，默认 http://127.0.0.1:18080
#   GF_PID_FILE   pid 文件路径，默认 /home/ipms/goflow2/logs/goflow2.pid
#   GF_OUT        输出目录，默认 /tmp
#   GF_INPUT=file 从文件读取 /metrics（离线复现用，配合 GF_FILE）
# ─────────────────────────────────────────────────────────────
set -uo pipefail

BASE="${GF_BASE:-http://127.0.0.1:18080}"
PID_FILE="${GF_PID_FILE:-/home/ipms/goflow2/logs/goflow2.pid}"
OUT="${GF_OUT:-/tmp}"

INTERVAL="${1:-0}"
WITH_PPROF="${2:-}"

# ── 采集一次 /metrics，输出 "名称 值" ──────────────────────
# 注意：多数指标带标签，形如 name{label="v"} value，
#       因此不能用 grep "^name " 匹配，必须按前缀聚合 $NF。
collect_metrics() {
    # 先取 /metrics 文本，再交给 awk 聚合（必须是管道，不能是两个独立命令）
    if [[ "${GF_INPUT:-}" == "file" ]]; then
        cat "${GF_FILE:-/dev/null}"
    else
        curl -s --max-time 10 "${BASE}/metrics" || true
    fi | awk '
    # ── 流量 ──
    /^goflow2_flow_traffic_packets_total/        { pkts += $NF }
    /^goflow2_flow_traffic_bytes_total/          { byts += $NF }
    /^goflow2_flow_dropped_packets_total/         { dpkt += $NF }
    /^goflow2_flow_dropped_bytes_total/           { dbyt += $NF }
    /^goflow2_device_packets_total/               { dpkts += $NF }
    /^goflow2_device_flows_total/                 { dflws += $NF }
    /^goflow2_device_active_count/                { dcount = $NF }
    # ── 解码 / 模板 ──
    /^goflow2_flow_process_nf_total/              { nftot += $NF }
    /^goflow2_flow_process_nf_flowset_total/      { nfsets += $NF }
    /^goflow2_flow_process_nf_template_entries/   { tplent += $NF }
    /^goflow2_flow_sampling_rate_entries/         { srent += $NF }
    /^goflow2_flow_decoder_error_total/           { derr += $NF }
    /^goflow2_flow_decoding_time_seconds_sum/     { decsum += $NF }
    /^goflow2_flow_decoding_time_seconds_count/   { deccnt += $NF }
    /^goflow2_flow_process_nf_delay_seconds_sum/  { dlysum += $NF }
    /^goflow2_flow_process_nf_delay_seconds_count/{ dlycnt += $NF }
    # ── 运行时内存 / GC（判断是"留存"还是"分配churn"的关键）──
    /^go_memstats_alloc_bytes_total/              { galloc += $NF }
    /^go_memstats_heap_inuse_bytes/               { ginuse = $NF }
    /^go_memstats_heap_sys_bytes/                 { gsys = $NF }
    /^go_memstats_heap_released_bytes_total/      { grel += $NF }
    /^go_memstats_sys_bytes/                      { gsysall = $NF }
    /^go_gc_duration_seconds_count/               { gccnt += $NF }
    /^process_resident_memory_bytes/              { rss = $NF }
    /^process_start_time_seconds/                 { pstart = $NF }
    /^go_goroutines/                              { goro = $NF }
    /^go_threads/                                 { gthr = $NF }
    /^promhttp_metric_handler_requests_total/     { preq += $NF }
    END {
        printf "pkts %s\n",            pkts+0
        printf "byts %s\n",            byts+0
        printf "dpkt %s\n",            dpkt+0
        printf "dbyt %s\n",            dbyt+0
        printf "dpkts %s\n",           dpkts+0
        printf "dflws %s\n",           dflws+0
        printf "dcount %s\n",          dcount+0
        printf "nftot %s\n",           nftot+0
        printf "nfsets %s\n",          nfsets+0
        printf "tplent %s\n",          tplent+0
        printf "srent %s\n",           srent+0
        printf "derr %s\n",            derr+0
        printf "decsum %s\n",          decsum+0
        printf "deccnt %s\n",          deccnt+0
        printf "dlysum %s\n",          dlysum+0
        printf "dlycnt %s\n",          dlycnt+0
        printf "galloc %s\n",          galloc+0
        printf "ginuse %s\n",          ginuse+0
        printf "gsys %s\n",            gsys+0
        printf "grel %s\n",            grel+0
        printf "gsysall %s\n",         gsysall+0
        printf "gccnt %s\n",           gccnt+0
        printf "rss %s\n",             rss+0
        printf "pstart %s\n",          pstart+0
        printf "goro %s\n",            goro+0
        printf "gthr %s\n",            gthr+0
        printf "preq %s\n",            preq+0
    }'
}

# 从多行 "名称 值" 快照中取某字段的值（按行精确匹配，避免多行输入串扰）
get_val() { printf '%s\n' "$1" | grep -m1 -E "^$2 " | awk '{print $2}'; }

human() { awk -v v="${1:-0}" 'BEGIN{
    if (v >= 1073741824) printf "%.2f GiB", v/1073741824
    else if (v >= 1048576) printf "%.1f MiB", v/1048576
    else if (v >= 1024) printf "%.1f KiB", v/1024
    else printf "%d B", v }'; }

hr() { printf '%s\n' "────────────────────────────────────────────────────────────"; }

snapshot() {
    local snap; snap="$(collect_metrics)"
    hr; echo "快照 @ $(date '+%F %T')"; hr

    local v
    v=$(get_val "$snap" rss);        printf "进程 RSS        : %s\n" "$(human "$v")"
    v=$(get_val "$snap" ginuse);     printf "Go 堆 inuse     : %s\n" "$(human "$v")"
    v=$(get_val "$snap" gsys);       printf "Go 堆 sys       : %s\n" "$(human "$v")"
    v=$(get_val "$snap" gsysall);    printf "Go 运行时总内存 : %s\n" "$(human "$v")"
    v=$(get_val "$snap" goro);       printf "协程数 / 线程数 : %s / %s\n" "$(get_val "$snap" goro)" "$(get_val "$snap" gthr)"
    v=$(get_val "$snap" dcount);     printf "活跃设备数      : %s\n" "${v:-?}"
    v=$(get_val "$snap" tplent);     printf "模板条目数      : %s\n" "${v:-?}"
    v=$(get_val "$snap" srent);      printf "采样率条目数    : %s\n" "${v:-?}"

    # 队列
    echo
    if [[ "${GF_INPUT:-}" == "file" ]]; then
        echo "队列: 离线模式跳过"
    else
        jq_q='if length==0 then "n/a" else (map("depth=\(.depth)/\(.capacity)") | join("  ")) end'
        jq_t='if length==0 then "n/a" else (map("\(.addr): \(.depth)/\(.capacity) drop=\(.dropped)") | join("  ")) end'
        printf "接收队列  : "
        curl -s --max-time 5 "${BASE}/queue-stats" | jq -r "$jq_q" 2>/dev/null || echo "获取失败"
        printf "发送队列  : "
        curl -s --max-time 5 "${BASE}/transport-stats" | jq -r "$jq_t" 2>/dev/null || echo "获取失败"
    fi

    # OS 层
    echo
    if [[ -r "$PID_FILE" ]]; then
        local pid; pid="$(cat "$PID_FILE" 2>/dev/null)"
        if [[ -n "$pid" ]] && [[ -d "/proc/$pid" ]]; then
            local rss_kb cpu_pct
            rss_kb=$(awk '/VmRSS/{print $2}' "/proc/$pid/status" 2>/dev/null)
            cpu_pct=$(ps -o %cpu= -p "$pid" 2>/dev/null | tr -d ' ')
            local thr; thr=$(ls "/proc/$pid/task" 2>/dev/null | wc -l)
            printf "OS: pid=%s  RSS=%s MiB  CPU=%s%%  线程=%s\n" \
                   "$pid" "${rss_kb:-?}" "${cpu_pct:-?}" "$thr"
        fi
    fi
    free -m | awk 'NR==1||NR==2{printf "内存: %s\n", $0}'
    uptime | sed 's/^/负载: /'

}

capture_pprof() {
    echo; hr; echo "抓取 pprof 快照 -> $OUT"; hr
    local stamp; stamp="$(date +%Y%m%d-%H%M%S)"
    curl -s --max-time 60 "${BASE}/debug/pprof/heap"                  -o "${OUT}/heap-${stamp}.pb.gz"     && echo "  heap      ${OUT}/heap-${stamp}.pb.gz"
    curl -s --max-time 90 "${BASE}/debug/pprof/profile?seconds=30"    -o "${OUT}/cpu-${stamp}.pb.gz"      && echo "  cpu(30s)  ${OUT}/cpu-${stamp}.pb.gz"
    curl -s --max-time 30 "${BASE}/debug/pprof/goroutine?debug=1"     -o "${OUT}/goro-${stamp}.txt"       && echo "  goroutine ${OUT}/goro-${stamp}.txt"
    echo "  分析: go tool pprof -top -sample_index=inuse_space ${OUT}/heap-${stamp}.pb.gz"
    echo "        go tool pprof -top -sample_index=alloc_space ${OUT}/heap-${stamp}.pb.gz"
}

# ── 主流程 ────────────────────────────────────────────────
if [[ "$INTERVAL" =~ ^[0-9]+$ ]] && [[ "$INTERVAL" -gt 0 ]]; then
    S1="$(collect_metrics)"; snapshot >/dev/null
    [[ "$WITH_PPROF" == "pprof" ]] && capture_pprof
    echo; echo "等待 ${INTERVAL} 秒后采集第二次…"
    sleep "$INTERVAL"
    S2="$(collect_metrics)"; snapshot
    [[ "$WITH_PPROF" == "pprof" ]] && capture_pprof

    hr; echo "区间增量（${INTERVAL}s）"; hr
    awk -v s1="$S1" -v s2="$S2" -v dt="$INTERVAL" '
    BEGIN {
        split(s1, A, "\n"); split(s2, B, "\n")
        for (i in A) { split(A[i], kv, " "); v1[kv[1]] = kv[2]+0 }
        for (i in B) { split(B[i], kv, " "); v2[kv[1]] = kv[2]+0 }
        # 累计类指标求速率
        printf "入包速率     : %.0f pps\n",   (v2["pkts"]-v1["pkts"])/dt
        printf "入流量速率   : %.2f Mbps\n",  (v2["byts"]-v1["byts"])*8/dt/1000000
        printf "接收丢包     : %.0f pps (%s)\n", (v2["dpkt"]-v1["dpkt"])/dt, \
               sprintf("%.4f%%", (v2["pkts"]>v1["pkts"] ? (v2["dpkt"]-v1["dpkt"])*100.0/(v2["pkts"]-v1["pkts"]) : 0))
        printf "设备流量速率 : %.0f pps\n",   (v2["dpkts"]-v1["dpkts"])/dt
        printf "流记录速率   : %.0f flow/s\n", (v2["dflws"]-v1["dflws"])/dt
        # 分配 churn（判断是否分配过快导致 RSS 膨胀）
        printf "内存分配速率 : %.2f MiB/s\n", (v2["galloc"]-v1["galloc"])/dt/1048576
        printf "GC 次数      : %d 次 (%.1f 次/分钟)\n", (v2["gccnt"]-v1["gccnt"]), (v2["gccnt"]-v1["gccnt"])*60/dt
        printf "堆归还 OS    : %s\n", (v2["grel"]-v1["grel"] > 0 ? "有(健康)" : "无(内存只涨不降)")
        printf "平均解码耗时 : %.2f us\n", (v2["deccnt"]>v1["deccnt"] ? (v2["decsum"]-v1["decsum"])/(v2["deccnt"]-v1["deccnt"])*1e6 : 0)
        printf "模板/采样增长: +%d / +%d\n", (v2["tplent"]-v1["tplent"]), (v2["srent"]-v1["srent"])
    }'
else
    snapshot
    [[ "$WITH_PPROF" == "pprof" ]] && capture_pprof
    cat <<'EOF'

提示：加时间间隔可自动计算速率与丢包率
  ./collect-metrics.sh 600 pprof
EOF
fi