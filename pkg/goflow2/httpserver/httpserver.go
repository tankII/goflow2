package httpserver

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/netsampler/goflow2/v3/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Config configures the HTTP server.
type Config struct {
	Addr            string
	StoreHTTPPath   string
	DeviceStatsPath string
	RouterCounter   *metrics.RouterCounter
	LogHTTP         bool
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
	if cfg.StoreHTTPPath != "" && store != nil {
		mux.HandleFunc(cfg.StoreHTTPPath, StoreHandler(store))
	}
	if cfg.RouterCounter != nil {
		basePath := cfg.DeviceStatsPath
		if basePath == "" {
			basePath = "/device-stats"
		}
		if !strings.HasSuffix(basePath, "/") {
			basePath += "/"
		}
		mux.HandleFunc(basePath+"hourly", metrics.DeviceHourlyHandler(cfg.RouterCounter))
		mux.HandleFunc(basePath+"daily", metrics.DeviceDailyHandler(cfg.RouterCounter))
		mux.HandleFunc("/device-list", metrics.DeviceListHandler(cfg.RouterCounter))
	}

	if cfg.LogHTTP {
		return withAccessLog(mux)
	}
	return mux
}
