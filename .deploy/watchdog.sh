#!/usr/bin/env bash
# GoFlow2 看门狗（供 crontab 调用）
#
# 语义：采集器存活 → 什么都不做且不输出；不存活 → 自动拉起。
# 正常情况下完全静默，避免 cron 每次触发都发邮件；只有拉起动作和失败
# 才会记录到日志文件。
#
# 存活判定：直接用 run.sh status 的退出码
#   0 = 存活，1 = 未运行。判定逻辑因此只有一处定义（run.sh），
#   看门狗不再自己解析 pid 文件或遍历 /proc。
#
# 用法：
#   bash watchdog.sh              # 检查并在必要时拉起
#   bash watchdog.sh --status     # 只查看状态，不拉起（退出码：0=存活 1=未存活）
#
# crontab 示例（每 2 分钟检查一次）：
#   */2 * * * * /home/ipms/goflow2/watchdog.sh
#
# 环境变量：
#   GOFLOW2_DEPLOY_DIR   部署目录（run.sh 所在目录），默认取本脚本所在目录
#   GOFLOW2_WATCHDOG_LOG 看门狗日志目录，默认 ${DEPLOY_DIR}/logs/watchdog
#
# 退出码：
#   0  采集器存活（未做任何动作），或拉起成功
#   1  采集器未存活且拉起失败
#   2  用法错误 / 环境不满足

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="${GOFLOW2_DEPLOY_DIR:-$SCRIPT_DIR}"
RUN_SH="${DEPLOY_DIR}/run.sh"
LOG_DIR="${DEPLOY_DIR}/logs"
WATCHDOG_LOG_DIR="${GOFLOW2_WATCHDOG_LOG:-${LOG_DIR}/watchdog}"

MODE="watch"

usage() {
    cat <<EOF
用法: bash $(basename "$0") [--status]

  （无参数）  看门狗：存活则静默退出，不存活则拉起
  --status    只查看存活状态，不做任何拉起动作

环境变量:
  GOFLOW2_DEPLOY_DIR    部署目录（run.sh 所在目录），默认本脚本所在目录
  GOFLOW2_WATCHDOG_LOG  看门狗日志目录，默认 \${DEPLOY_DIR}/logs/watchdog
EOF
}

case "${1:-}" in
    "")         ;;
    --status)   MODE="status" ;;
    -h|--help)  usage; exit 0 ;;
    *)          echo "unknown argument: $1" >&2; usage; exit 2 ;;
esac

if [[ ! -r "$RUN_SH" ]]; then
    echo "watchdog: run.sh not found: $RUN_SH" >&2
    exit 2
fi

# ── 存活判定 ────────────────────────────────────────────────
#
# 委托给 run.sh status：退出码 0 表示采集器在跑，1 表示未运行。
# 判定逻辑只保留一份，避免看门狗与启停脚本对「是否在跑」的理解不一致。
is_running() {
    bash "$RUN_SH" status >/dev/null 2>&1
}

# ── 日志 ────────────────────────────────────────────────────
# 记录拉起动作与失败原因，按天切分。
watchdog_log() {
    local msg="$1"
    mkdir -p "$WATCHDOG_LOG_DIR" 2>/dev/null || return 0
    local file="${WATCHDOG_LOG_DIR}/check-$(date +%Y%m%d).log"
    printf '%s %s\n' "$(date '+%F %T')" "$msg" >> "$file" 2>/dev/null || true
}

if [[ "$MODE" == "status" ]]; then
    if is_running; then
        echo "running"
        exit 0
    fi
    echo "not running"
    exit 1
fi

# ── 看门狗主体 ──────────────────────────────────────────────

# 存活 → 静默退出
if is_running; then
    exit 0
fi

# 未存活 → 拉起
watchdog_log "goflow2 not running, restarting..."

# run.sh start 会打印启动细节；全部转到临时文件后写入看门狗日志，
# 保持 cron 静默。用文件而非 $(...) 捕获，是因为 run.sh 派生的后台进程
# 继承了 stdout，管道要等它退出才关闭，会把看门狗拖住。
tmp_out="$(mktemp "${TMPDIR:-/tmp}/goflow2-watchdog.XXXXXX")" || tmp_out="/tmp/goflow2-watchdog.$$"

bash "$RUN_SH" start >"$tmp_out" 2>&1
rc=$?

out=$(tr '\n' '|' < "$tmp_out" 2>/dev/null)
rm -f "$tmp_out" 2>/dev/null || true

if [[ $rc -ne 0 ]]; then
    watchdog_log "start FAILED (rc=$rc): ${out}"
    # 失败要让人看见：写 stderr，cron 会把 stderr 寄到邮箱
    printf 'watchdog: goflow2 start failed (rc=%s)\n%s\n' "$rc" "$out" >&2
    exit 1
fi

watchdog_log "started ok: ${out}"
exit 0
