package httpserver

import (
	_ "embed"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/netsampler/goflow2/v3/metrics"
	"github.com/netsampler/goflow2/v3/pkg/goflow2/collector"
	protoproducer "github.com/netsampler/goflow2/v3/producer/proto"
	"github.com/netsampler/goflow2/v3/transport"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Config configures the HTTP server.
type Config struct {
	Addr               string
	StoreHTTPPath      string
	TemplatesPath      string
	TemplateSource     TemplateSource
	DeviceStatsPath    string
	QueueStatsPath     string
	TransportStatsPath string
	WebPath            string
	Transport          string
	Format             string
	FlowFields         []protoproducer.FlowField
	// Params is the effective runtime configuration shown in the dashboard.
	Params               []ParamGroup
	RouterCounter        *metrics.RouterCounter
	QueueStatsSource     func() []collector.QueueStat
	TransportStatsSource func() []transport.TargetStat
	LogHTTP              bool
	Pprof                bool
}

// StoreSource returns flowstore data for HTTP rendering.
type StoreSource func() []byte

// HealthHandler returns a handler for the health endpoint.
func HealthHandler(isCollecting func() bool) http.HandlerFunc {
	return func(wr http.ResponseWriter, r *http.Request) {
		if !isCollecting() {
			wr.WriteHeader(http.StatusServiceUnavailable)
			if _, err := wr.Write([]byte("Not OK\n")); err != nil {
				slog.Error("error writing HTTP", slog.String("error", err.Error()))
			}
			return
		}
		wr.WriteHeader(http.StatusOK)
		if _, err := wr.Write([]byte("OK\n")); err != nil {
			slog.Error("error writing HTTP", slog.String("error", err.Error()))
		}
	}
}

// QueueStatsHandler returns a handler serving receiver queue usage as JSON.
func QueueStatsHandler(source func() []collector.QueueStat) http.HandlerFunc {
	return func(wr http.ResponseWriter, r *http.Request) {
		stats := source()
		if stats == nil {
			stats = []collector.QueueStat{}
		}
		body, err := json.Marshal(stats)
		if err != nil {
			wr.WriteHeader(http.StatusInternalServerError)
			if _, werr := wr.Write([]byte("Internal Server Error\n")); werr != nil {
				slog.Error("error writing HTTP", slog.String("error", werr.Error()))
			}
			return
		}
		wr.Header().Add("Content-Type", "application/json")
		wr.WriteHeader(http.StatusOK)
		if _, err := wr.Write(body); err != nil {
			slog.Error("error writing HTTP", slog.String("error", err.Error()))
		}
	}
}

// TransportStatsHandler returns a handler serving per-target send buffer queue
// usage as JSON.
func TransportStatsHandler(source func() []transport.TargetStat) http.HandlerFunc {
	return func(wr http.ResponseWriter, r *http.Request) {
		stats := source()
		if stats == nil {
			stats = []transport.TargetStat{}
		}
		body, err := json.Marshal(stats)
		if err != nil {
			wr.WriteHeader(http.StatusInternalServerError)
			if _, werr := wr.Write([]byte("Internal Server Error\n")); werr != nil {
				slog.Error("error writing HTTP", slog.String("error", werr.Error()))
			}
			return
		}
		wr.Header().Add("Content-Type", "application/json")
		wr.WriteHeader(http.StatusOK)
		if _, err := wr.Write(body); err != nil {
			slog.Error("error writing HTTP", slog.String("error", err.Error()))
		}
	}
}

// StoreHandler returns a handler for the store endpoint.
func StoreHandler(store StoreSource) http.HandlerFunc {
	return func(wr http.ResponseWriter, r *http.Request) {
		body := store()
		if body == nil {
			wr.WriteHeader(http.StatusNotFound)
			if _, err := wr.Write([]byte("Not Found\n")); err != nil {
				slog.Error("error writing HTTP", slog.String("error", err.Error()))
			}
			return
		}
		wr.Header().Add("Content-Type", "application/json")
		wr.WriteHeader(http.StatusOK)
		if _, err := wr.Write(body); err != nil {
			slog.Error("error writing HTTP", slog.String("error", err.Error()))
		}
	}
}

// ParamItem is a single configuration entry displayed in the dashboard's
// "运行参数" card.
type ParamItem struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

// ParamGroup groups related configuration entries under one heading.
type ParamGroup struct {
	Group string      `json:"group"`
	Items []ParamItem `json:"items"`
}

// dashboardConfig is injected into the dashboard page as JSON so the frontend
// can discover the endpoints that are actually enabled.
type dashboardConfig struct {
	HealthPath         string                    `json:"healthPath"`
	MetricsPath        string                    `json:"metricsPath"`
	StorePath          string                    `json:"storePath"`
	TemplatesPath      string                    `json:"templatesPath"`
	QueueStatsPath     string                    `json:"queueStatsPath"`
	TransportStatsPath string                    `json:"transportStatsPath"`
	DeviceStatsPath    string                    `json:"deviceStatsPath"`
	DeviceListPath     string                    `json:"deviceListPath"`
	Transport          string                    `json:"transport"`
	Format             string                    `json:"format"`
	FlowFields         []protoproducer.FlowField `json:"flowFields"`
	// Params holds the effective runtime configuration, grouped for display.
	Params []ParamGroup `json:"params"`
}

const dashboardConfigPlaceholder = "__GOFLOW2_CONFIG__"

//go:embed web/index.html
var dashboardHTML string

// DashboardHandler returns a handler serving the embedded web dashboard, with
// the endpoint configuration injected into the page.
func dashboardHandler(cfg dashboardConfig) http.HandlerFunc {
	body, err := json.Marshal(cfg)
	if err != nil {
		slog.Error("error marshalling dashboard config", slog.String("error", err.Error()))
	}
	page := strings.Replace(dashboardHTML, dashboardConfigPlaceholder, string(body), 1)
	return func(wr http.ResponseWriter, r *http.Request) {
		wr.Header().Add("Content-Type", "text/html; charset=utf-8")
		wr.WriteHeader(http.StatusOK)
		if _, err := wr.Write([]byte(page)); err != nil {
			slog.Error("error writing HTTP", slog.String("error", err.Error()))
		}
	}
}

// statusWriter captures the response status and written bytes for access logs.
type statusWriter struct {
	http.ResponseWriter
	code int
	n    int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.n += n
	return n, err
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// withAccessLog logs every request through the wrapped handler.
func withAccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(wr http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: wr, code: http.StatusOK}
		next.ServeHTTP(sw, r)
		slog.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"remote", r.RemoteAddr,
			"user_agent", r.UserAgent(),
			"status", sw.code,
			"bytes", sw.n,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// New constructs a mux with metrics, health, store, and device-stats endpoints.
func New(cfg Config, store StoreSource, isCollecting func() bool) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/__health", HealthHandler(isCollecting))

	storePath := ""
	if cfg.StoreHTTPPath != "" && store != nil {
		storePath = cfg.StoreHTTPPath
		mux.HandleFunc(storePath, StoreHandler(store))
	}

	// Device statistics endpoints stay available even when metrics are
	// disabled: the handlers then respond with empty data (200 OK).
	deviceStatsPath := orDefault(cfg.DeviceStatsPath, "/device-stats")
	deviceListPath := "/device-list"
	deviceBase := strings.TrimSuffix(deviceStatsPath, "/")
	mux.HandleFunc(deviceBase+"/hourly", metrics.DeviceHourlyHandler(cfg.RouterCounter))
	mux.HandleFunc(deviceBase+"/daily", metrics.DeviceDailyHandler(cfg.RouterCounter))
	mux.HandleFunc(deviceListPath, metrics.DeviceListHandler(cfg.RouterCounter))

	// 模板浏览端点与 /store 相互独立：/store 输出的同时也是落盘的持久化
	// 文档，其结构必须保持稳定，因此带字段释义的展示视图单独走一个接口。
	templatesPath := ""
	if cfg.TemplateSource != nil {
		templatesPath = orDefault(cfg.TemplatesPath, "/templates")
		mux.HandleFunc(templatesPath, TemplateHandler(cfg.TemplateSource))
	}

	queueStatsPath := ""
	if cfg.QueueStatsSource != nil {
		queueStatsPath = orDefault(cfg.QueueStatsPath, "/queue-stats")
		mux.HandleFunc(queueStatsPath, QueueStatsHandler(cfg.QueueStatsSource))
	}

	transportStatsPath := ""
	if cfg.TransportStatsSource != nil {
		transportStatsPath = orDefault(cfg.TransportStatsPath, "/transport-stats")
		mux.HandleFunc(transportStatsPath, TransportStatsHandler(cfg.TransportStatsSource))
	}

	if cfg.WebPath != "" {
		mux.HandleFunc(cfg.WebPath, dashboardHandler(dashboardConfig{
			HealthPath:         "/__health",
			MetricsPath:        "/metrics",
			StorePath:          storePath,
			TemplatesPath:      templatesPath,
			QueueStatsPath:     queueStatsPath,
			TransportStatsPath: transportStatsPath,
			DeviceStatsPath:    deviceStatsPath,
			DeviceListPath:     deviceListPath,
			Transport:          cfg.Transport,
			Format:             cfg.Format,
			FlowFields:         cfg.FlowFields,
			Params:             cfg.Params,
		}))
	}

	if cfg.Pprof {
		registerPprof(mux)
	}

	if cfg.LogHTTP {
		return withAccessLog(mux)
	}
	return mux
}

// registerPprof exposes the standard Go profiling endpoints under
// /debug/pprof/. It is opt-in because it can leak process internals and is
// expensive to serve; enable it only when profiling a running collector.
func registerPprof(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
