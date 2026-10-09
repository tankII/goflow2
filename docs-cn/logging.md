# 日志分级、输出与文件轮转

> goflow2 / enricher 的日志统一走 `log/slog`（结构化日志），由 `pkg/goflow2/logging` 单一入口构建。本文介绍级别、格式、目标（stderr / 文件）、轮转规则与调试选项。

## 1. 日志级别与格式

| Flag | 取值 | 默认 | 说明 |
|---|---|---|---|
| `-loglevel` | `debug` / `info` / `warn` / `error` | `info` | 输出阈值，低于该级别的不输出 |
| `-logfmt` | `normal` / `json` | `normal` | `normal` 为 key=value 文本；`json` 每行一条 JSON |

两者在 goflow2 与 enricher 上保持一致，均由 `logging.RegisterFlags` 注册，避免两处参数漂移。

## 2. 输出目标

默认（不设 `-log.file`）日志写入 **stderr**，与走 **stdout** 的 flow 记录天然分离：

- 直接命令行运行时，`| jq` 等管道拿 flow 记录，`2>/dev/null` 可丢弃日志；
- 部署脚本 `run.sh` 不做重定向，把 stdout/stderr 原样交给调用方（systemd/journald、管道等）接管。

设置 `-log.file <path>` 后，日志改由应用内写入指定文件，并获得**按大小轮转**能力。goflow2 与 enricher 行为一致。

| Flag | 默认 | 说明 |
|---|---|---|
| `-log.file` | 空（stderr） | 日志文件路径；为空则输出 stderr |
| `-log.also-stderr` | `false` | 设了 `-log.file` 时，是否仍同时写 stderr |

## 3. 文件轮转（应用内实现）

轮转由 `gopkg.in/natefinch/lumberjack` 实现，配置均在 `-log.file` 生效时起作用：

| Flag | 默认 | 说明 |
|---|---|---|
| `-log.file.max-size` | `100` | 单文件大小上限，单位 **MB**；`0` 表示关闭大小轮转 |
| `-log.file.max-backups` | `3` | 保留的旧文件（备份）个数 |
| `-log.file.max-age` | `28` | 备份保留天数 |
| `-log.file.compress` | `true` | 备份以 gzip 压缩（`.gz`） |

轮转语义（lumberjack）：

- 写入后若超过 `max-size`，当前文件改名为 `<name>-<本地时间戳>.log[.gz]` 并新建空文件；
- `max-backups` / `max-age` 由后台线程（mill）异步清理，删除时机 `min(超份数, 超天数)`；
- 备份时间戳使用**本地时区**（`LocalTime: true`）；
- 压缩为异步操作，`.gz` 文件可能稍晚出现；
- 并发安全：多处 goroutine 同时写同一 logger 是安全的（可配合 `-race` 验证）。

与部署脚本的关系：

- `run.sh start` 不重定向，stdout（flow 记录）与 stderr（日志）原样流入调用方环境；
- 需要独立落盘 + 轮转时设置 `GOFLOW2_LOG_PATH`（对应 `-log.file`），脚本自动追加 `-log.file.max-size/-backups/-age/-compress` 等参数；需要保留一份 stderr 日志时再设 `GOFLOW2_LOG_ALSO_STDERR=1`；

## 4. 调试选项

| Flag | 默认 | 说明 |
|---|---|---|
| `-log.source` | `false` | 每条记录附加 `source=文件:行`，便于定位代码 |
| `-log.http` | `false` | 开启 HTTP 访问日志 |

### 4.1 启动配置摘要

goflow2 启动时输出一行 `goflow2 startup` 的 `INFO` 记录，包含：监听地址、HTTP 端口、格式、传输、produce、设备统计（前缀/保留时长）、模板/采样 TTL、持久化与 mapping 路径。用于核对当前生效配置是否与预期一致。

### 4.2 HTTP 访问日志

`-log.http` 会为 HTTP 状态端口（默认 `:18080`）的每个请求输出一条记录：

```
msg="http request" method=GET path=/device-list query="" remote=127.0.0.1:xxxxx user_agent=curl/... status=200 bytes=123 duration_ms=0
```

中间件位于 `httpserver.New` 内部，使用 `slog.Default()`（app 启动时已初始化），因此同样遵守 `-loglevel` 与 `-logfmt` 设置，也可写入 `-log.file`。仅对已注册端点生效（promhttp 的 `/metrics`、`/__health`、store、device-stats）。

## 5. 部署脚本接线（run.sh / setenv.sh）

| 环境变量 | 对应 flag | 默认 |
|---|---|---|
| `GOFLOW2_LOG_PATH` | `-log.file` | 无（stderr） |
| `GOFLOW2_LOG_ALSO_STDERR` | `-log.also-stderr` | `0` |
| `GOFLOW2_LOG_SOURCE` | `-log.source` | `0` |
| `GOFLOW2_LOG_HTTP` | `-log.http` | `0` |
| `GOFLOW2_LOG_ROT_SIZE` | `-log.file.max-size` | `100` |
| `GOFLOW2_LOG_ROT_BACKUPS` | `-log.file.max-backups` | `3` |
| `GOFLOW2_LOG_ROT_AGE` | `-log.file.max-age` | `28` |
| `GOFLOW2_LOG_ROT_COMPRESS` | `-log.file.compress` | `true` |

布尔型环境变量传 `1` / `true` 时以“裸 flag”传入，否则以 `flag 值` 传入（可用于关闭默认开启项，如 `GOFLOW2_LOG_ROT_COMPRESS=0`）。`run.sh start` 的回显中会展示 `log args` 以便核对。

## 6. 设计要点

- 单一入口：`logging.NewLogger(Options)` 决定 writer（stderr / lumberjack / MultiWriter）与 `HandlerOptions`；
- `logging.RegisterFlags(fs, *Options)` 统一注册全部 `-log.*` flags，goflow2 与 enricher 共用，杜绝参数不一致；
- HTTP 访问日志不引入额外依赖，复用既有 slog。