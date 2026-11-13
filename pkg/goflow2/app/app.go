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
			},
			httpserver.ParamItem{
				Label: "  socket / worker",
				Value: fmt.Sprintf("%d / %d", l.NumSockets, l.NumWorkers),
			},
			httpserver.ParamItem{
				Label: "  队列容量",
				Value: fmt.Sprintf("%d（阻塞=%s）", l.QueueSize, onOff(l.Blocking)),
			},
		)
	}
	if cfg.UDPRecvBuffer > 0 {
		recv = append(recv, httpserver.ParamItem{
			Label: "socket 接收缓冲",
			Value: humanBytes(uint64(cfg.UDPRecvBuffer)),
		})
	}
	if len(recv) > 0 {
		groups = append(groups, httpserver.ParamGroup{Group: "接收", Items: recv})
	}

	// ── 发送 ──
	send := []httpserver.ParamItem{
		{Label: "传输方式", Value: cfg.Transport},
		{Label: "输出格式", Value: cfg.Format},
		{Label: "产出方式", Value: cfg.Produce},
	}
	if cfg.MappingFile != "" {
		send = append(send, httpserver.ParamItem{Label: "字段映射", Value: cfg.MappingFile})
	}
	groups = append(groups, httpserver.ParamGroup{Group: "发送", Items: send})

	// ── 运行时 ──
	run := []httpserver.ParamItem{
		{Label: "HTTP 面板", Value: cfg.Addr},
		{Label: "指标 / pprof", Value: onOff(cfg.Metrics) + " / " + onOff(cfg.Pprof)},
		{Label: "模板 TTL", Value: cfg.TemplatesTTL.String() + "（访问续期=" + onOff(cfg.TemplatesExtendOnAccess) + "）"},
		{Label: "采样率 TTL", Value: cfg.SamplingRatesTTL.String() + "（访问续期=" + onOff(cfg.SamplingRatesExtendOnAccess) + "）"},
	}
	// GOGC / GOMEMLIMIT 是影响 CPU 与内存的关键开关，从环境变量读取
	if v := os.Getenv("GOGC"); v != "" {
		run = append(run, httpserver.ParamItem{Label: "GOGC", Value: v})
	}
	if v := os.Getenv("GOMEMLIMIT"); v != "" {
		run = append(run, httpserver.ParamItem{Label: "GOMEMLIMIT", Value: v})
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
