package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/netsampler/goflow2/v3/metrics"
	"github.com/netsampler/goflow2/v3/pkg/goflow2/builder"
	"github.com/netsampler/goflow2/v3/pkg/goflow2/collector"
	"github.com/netsampler/goflow2/v3/pkg/goflow2/config"
	"github.com/netsampler/goflow2/v3/pkg/goflow2/httpserver"
	"github.com/netsampler/goflow2/v3/pkg/goflow2/listen"
	"github.com/netsampler/goflow2/v3/pkg/goflow2/logging"
	protoproducer "github.com/netsampler/goflow2/v3/producer/proto"
	"github.com/netsampler/goflow2/v3/utils/debug"
	"github.com/netsampler/goflow2/v3/utils/store/persistence"
	"github.com/netsampler/goflow2/v3/utils/store/samplingrate"
	"github.com/netsampler/goflow2/v3/utils/store/templates"
)

// App wires and runs the GoFlow2 application.
type App struct {
	cfg           *config.Config
	logger        *slog.Logger
	collector     *collector.Collector
	persistence   *persistence.Manager
	transport     interface{ Close() error }
	producer      interface{ Close() }
	routerCounter *metrics.RouterCounter
	server        *http.Server
	serverErr     chan error
	collecting    atomic.Bool
}

// buildDashboardParams 汇总控制台「运行参数」卡片要展示的配置。
//
// 只挑与收发路径直接相关的项：接收侧看监听参数与 socket 缓冲，发送侧看
// 传输与输出格式，运行时看影响性能的关键开关。无关配置不堆到界面上。
// 每项都带一句说明，运维无需翻文档即可区分相近的开关。
func buildDashboardParams(cfg *config.Config, listeners []listen.ListenerConfig) []httpserver.ParamGroup {
	groups := []httpserver.ParamGroup{}

	// ── 接收 ──
	recv := []httpserver.ParamItem{}
	for _, l := range listeners {
		host := l.Hostname
		if host == "" {
			host = "*"
		}
		recv = append(recv,
			httpserver.ParamItem{
				Label: "监听 " + l.Scheme,
				Value: fmt.Sprintf("%s:%d", host, l.Port),
				Desc:  "接收 NetFlow / sFlow 报文的 UDP 监听地址与端口。",
			},
			httpserver.ParamItem{
				Label: "  socket / worker",
				Value: fmt.Sprintf("%d / %d", l.NumSockets, l.NumWorkers),
				Desc:  "socket 数为复用端口的接收套接字个数（分流入口流量）；worker 数为解码协程数（处理能力）。",
			},
			httpserver.ParamItem{
				Label: "  队列容量",
				Value: fmt.Sprintf("%d（阻塞=%s）", l.QueueSize, onOff(l.Blocking)),
				Desc:  "应用层接收队列长度。非阻塞模式下队列满即丢弃（应用层丢包）；阻塞模式不丢包但接收协程会卡住。",
			},
		)
	}
	if cfg.UDPRecvBuffer > 0 {
		recv = append(recv, httpserver.ParamItem{
			Label: "socket 接收缓冲",
			Value: humanBytes(uint64(cfg.UDPRecvBuffer)),
			Desc:  "内核 socket 缓冲区（SO_RCVBUF）。它是应用层队列之前的一级缓冲，默认仅 256KB，突发流量会在内核层就溢出（内核层丢包）。受 net.core.rmem_max 限制，需同步调大 sysctl 并重启才生效。",
		})
	}
	if len(recv) > 0 {
		groups = append(groups, httpserver.ParamGroup{Group: "接收", Items: recv})
	}

	// ── 发送 ──
	send := []httpserver.ParamItem{
		{Label: "传输方式", Value: cfg.Transport,
			Desc: "流记录的下游去向：file 落文件、netcat 发 TCP、Kafka 入队。"},
		{Label: "输出格式", Value: cfg.Format,
			Desc: "流记录的序列化格式，决定下游字段形态与解析方式。"},
		{Label: "产出方式", Value: cfg.Produce,
			Desc: "sample 按采样规则产出流记录；raw 不做映射，原样输出原始字段。"},
	}
	if cfg.MappingFile != "" {
		send = append(send, httpserver.ParamItem{Label: "字段映射", Value: cfg.MappingFile,
			Desc: "字段映射配置文件，决定原始字段如何重命名、置常量或组合为输出列。"})
	}
	groups = append(groups, httpserver.ParamGroup{Group: "发送", Items: send})

	// ── 运行时 ──
	run := []httpserver.ParamItem{
		{Label: "HTTP 面板", Value: cfg.Addr,
			Desc: "控制台与监控端点的监听地址，留空则不启用 HTTP。"},
		{Label: "指标 / pprof", Value: onOff(cfg.Metrics) + " / " + onOff(cfg.Pprof),
			Desc: "指标开关控制 Prometheus 指标与设备统计；pprof 开启 CPU/内存剖析端点。"},
		{Label: "模板 TTL", Value: cfg.TemplatesTTL.String() + "（访问续期=" + onOff(cfg.TemplatesExtendOnAccess) + "）",
			Desc: "模板过期时间。过期后设备未重发模板将导致数据无法解码而丢弃；开启访问续期后，正在收流量的模板不会过期。"},
		{Label: "采样率 TTL", Value: cfg.SamplingRatesTTL.String() + "（访问续期=" + onOff(cfg.SamplingRatesExtendOnAccess) + "）",
			Desc: "采样率记录的过期时间，机制同上。"},
	}
	// GOGC / GOMEMLIMIT 是影响 CPU 与内存的关键开关，从环境变量读取
	if v := os.Getenv("GOGC"); v != "" {
		run = append(run, httpserver.ParamItem{Label: "GOGC", Value: v,
			Desc: "GC 目标堆增长百分比。调低 → GC 更积极 → 内存下降、CPU 上升（用 CPU 换内存）。"})
	}
	if v := os.Getenv("GOMEMLIMIT"); v != "" {
		run = append(run, httpserver.ParamItem{Label: "GOMEMLIMIT", Value: v,
			Desc: "Go 运行时软内存上限。接近时自动加大 GC 力度，避免堆持续膨胀被内核 OOM kill。"})
	}
	groups = append(groups, httpserver.ParamGroup{Group: "运行时", Items: run})

	return groups
}

// onOff 把布尔值渲染为「开 / 关」。
func onOff(b bool) string {
	if b {
		return "开"
	}
	return "关"
}

// humanBytes 把字节数渲染为便于阅读的字符串。
func humanBytes(n uint64) string {
	switch {
	case n >= 1024*1024*1024:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1024*1024*1024))
	case n == 0:
		return "0 B"
	case n >= 1024*1024:
		return fmt.Sprintf("%.0f MiB", float64(n)/(1024*1024))
	case n >= 1024:
		return fmt.Sprintf("%.0f KiB", float64(n)/1024)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// New constructs a new App from config.
func New(cfg *config.Config) (*App, error) {
	logger, err := logging.NewLogger(cfg.Logging)
	if err != nil {
		return nil, fmt.Errorf("app: init logger: %w", err)
	}
	slog.SetDefault(logger)

	slog.Info("goflow2 startup",
		"log_level", cfg.Logging.Level,
		"log_format", cfg.Logging.Format,
		"log_file", cfg.Logging.File,
		"listen", cfg.ListenAddresses,
		"udp_recv_buffer", cfg.UDPRecvBuffer,
		"http_addr", cfg.Addr,
		"format", cfg.Format,
		"transport", cfg.Transport,
		"produce", cfg.Produce,
		"metrics", cfg.Metrics,
		"device_stats_path", cfg.DeviceStatsPath,
		"transport_stats_path", cfg.TransportStatsPath,
		"web_path", cfg.WebPath,
		"device_retention_hourly", cfg.DeviceHourlyTTL.String(),
		"device_retention_daily", cfg.DeviceDailyTTL.String(),
		"templates_ttl", cfg.TemplatesTTL.String(),
		"sampling_ttl", cfg.SamplingRatesTTL.String(),
		"store_json_path", cfg.StoreJSONPath,
		"store_json_interval", cfg.StoreJSONInterval.String(),
		"mapping", cfg.MappingFile)

	formatter, err := builder.BuildFormatter(cfg.Format)
	if err != nil {
		return nil, fmt.Errorf("app: build formatter: %w", err)
	}
	transporter, err := builder.BuildTransport(cfg.Transport)
	if err != nil {
		return nil, fmt.Errorf("app: build transport: %w", err)
	}

	persist := persistence.New(persistence.Config{
		Path:     cfg.StoreJSONPath,
		Interval: cfg.StoreJSONInterval,
	})

	samplingOpts := []samplingrate.FlowStoreOption{
		samplingrate.WithTTL(cfg.SamplingRatesTTL),
		samplingrate.WithExtendOnAccess(cfg.SamplingRatesExtendOnAccess),
		samplingrate.WithSweepInterval(cfg.SamplingRatesSweepInterval),
	}
	if cfg.Metrics {
		samplingOpts = append(samplingOpts, samplingrate.WithHooks(metrics.SamplingRateStoreHooks()))
	}
	samplingStore, err := persist.NewSamplingRateStore(samplingOpts...)
	if err != nil {
		return nil, fmt.Errorf("app: init sampling persistence: %w", err)
	}

	templateOpts := []templates.FlowStoreOption{
		templates.WithTTL(cfg.TemplatesTTL),
		templates.WithExtendOnAccess(cfg.TemplatesExtendOnAccess),
		templates.WithSweepInterval(cfg.TemplatesSweepInterval),
	}
	if cfg.Metrics {
		templateOpts = append(templateOpts, templates.WithHooks(metrics.TemplateStoreHooks()))
	}
	templateStore, err := persist.NewTemplateStore(templateOpts...)
	if err != nil {
		return nil, fmt.Errorf("app: init template persistence: %w", err)
	}

	flowProducer, err := builder.BuildProducer(cfg, samplingStore)
	if err != nil {
		return nil, fmt.Errorf("app: build producer: %w", err)
	}

	var flowFields []protoproducer.FlowField
	if fp, ok := flowProducer.(interface {
		Fields() []protoproducer.FlowField
	}); ok {
		flowFields = fp.Fields()
		// 补上输出字段的中文释义：释义表在 httpserver 包，这里按需填充
		for i := range flowFields {
			flowFields[i].Desc = httpserver.FlowFieldDescription(flowFields[i].Name)
		}
	}

	// deviceRC is nil when metrics are disabled, which turns the Prometheus
	// wrappers into no-ops and keeps the device endpoints empty.
	var deviceRC *metrics.RouterCounter
	if cfg.Metrics {
		deviceRC = metrics.NewRouterCounter(cfg.DeviceHourlyTTL, cfg.DeviceDailyTTL)
	}

	flowProducer = debug.WrapPanicProducer(flowProducer)
	flowProducer = metrics.WrapPromProducer(flowProducer, deviceRC)

	listeners, err := listen.ParseListenAddresses(cfg.ListenAddresses)
	if err != nil {
		return nil, fmt.Errorf("app: parse listen addresses: %w", err)
	}

	coll, err := collector.New(collector.Config{
		Listeners:     listeners,
		Formatter:     formatter,
		Transport:     transporter,
		Producer:      flowProducer,
		TemplateStore: templateStore,
		RouterCounter: deviceRC,
		ErrCnt:        cfg.ErrCnt,
		ErrInt:        cfg.ErrInt,
		RecvBuffer:    cfg.UDPRecvBuffer,
		Logger:        logger,
	})
	if err != nil {
		return nil, fmt.Errorf("app: init collector: %w", err)
	}

	app := &App{
		cfg:           cfg,
		logger:        logger,
		collector:     coll,
		persistence:   persist,
		transport:     transporter,
		producer:      flowProducer,
		routerCounter: deviceRC,
		serverErr:     make(chan error, 1),
	}

	if cfg.Addr != "" {
		mux := httpserver.New(httpserver.Config{
			Addr:                 cfg.Addr,
			StoreHTTPPath:        cfg.StoreHTTPPath,
			TemplatesPath:        cfg.TemplatesHTTPPath,
			TemplateSource:       persist.TemplateStore(),
			DeviceStatsPath:      cfg.DeviceStatsPath,
			QueueStatsPath:       cfg.QueueStatsPath,
			TransportStatsPath:   cfg.TransportStatsPath,
			WebPath:              cfg.WebPath,
			Transport:            cfg.Transport,
			Format:               cfg.Format,
			FlowFields:           flowFields,
			Params:               buildDashboardParams(cfg, listeners),
			RouterCounter:        deviceRC,
			QueueStatsSource:     coll.QueueStats,
			TransportStatsSource: transporter.StatsSource(),
			LogHTTP:              cfg.Logging.HTTP,
			Pprof:                cfg.Pprof,
		}, persist.Document, app.collecting.Load)
		app.server = &http.Server{
			Addr:              cfg.Addr,
			Handler:           mux,
			ReadHeaderTimeout: time.Second * 5,
		}
	}

	return app, nil
}

// Start starts the collector and HTTP server.
func (a *App) Start() error {
	a.logger.Info("starting GoFlow2")

	a.persistence.Start()
	go func() {
		for err := range a.persistence.Errors() {
			a.logger.Error("flowstore persistence error", slog.String("error", err.Error()))
		}
	}()

	if err := a.collector.Start(); err != nil {
		a.persistence.Close()
		return fmt.Errorf("app: start collector: %w", err)
	}
	a.collecting.Store(true)

	if a.server == nil {
		return nil
	}

	go func() {
		err := a.server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.serverErr <- fmt.Errorf("http server: %w", err)
			return
		}
		a.logger.With(slog.String("http", a.cfg.Addr)).Info("closed HTTP server")
	}()

	return nil
}

// Run starts the app and blocks until context cancellation or server error.
func (a *App) Run(ctx context.Context) error {
	if err := a.Start(); err != nil {
		return fmt.Errorf("app run start: %w", err)
	}

	if a.server == nil {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		a.Shutdown(shutdownCtx)
		cancel()
		return nil
	}

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		a.Shutdown(shutdownCtx)
		cancel()
		return nil
	case err := <-a.Wait():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		a.Shutdown(shutdownCtx)
		cancel()
		return fmt.Errorf("app run wait: %w", err)
	}
}

// Wait returns a channel that receives HTTP server errors.
func (a *App) Wait() <-chan error {
	return a.serverErr
}

// Shutdown stops receivers, closes transports, and shuts down the HTTP server.
func (a *App) Shutdown(ctx context.Context) {
	a.collecting.Store(false)

	a.collector.Stop()
	if a.routerCounter != nil {
		a.routerCounter.Stop()
	}
	a.producer.Close()
	a.persistence.Close()
	if err := a.transport.Close(); err != nil {
		a.logger.Error("error closing transport", slog.String("error", err.Error()))
	}
	a.logger.Info("transporter closed")

	if a.server == nil {
		return
	}
	if err := a.server.Shutdown(ctx); err != nil {
		a.logger.Error("error shutting-down HTTP server", slog.String("error", err.Error()))
	}
}
