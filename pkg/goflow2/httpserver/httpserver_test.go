package httpserver

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func bufferDefaultLogger() *bytes.Buffer {
	buf := &bytes.Buffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	return buf
}

func restoreDefaultLogger() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

func TestNewLogsHTTPAccess(t *testing.T) {
	buf := bufferDefaultLogger()
	defer restoreDefaultLogger()

	h := New(Config{LogHTTP: true}, nil, func() bool { return true })
	req := httptest.NewRequest(http.MethodGet, "/__health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	out := buf.String()
	if !strings.Contains(out, "http request") {
		t.Fatalf("expected access log line, got %q", out)
	}
	for _, want := range []string{"method=GET", "path=/__health", "status=200"} {
		if !strings.Contains(out, want) {
			t.Fatalf("access log missing %q, got %q", want, out)
		}
	}
}

func TestNewLogsHTTPAccessNon200(t *testing.T) {
	buf := bufferDefaultLogger()
	defer restoreDefaultLogger()

	h := New(Config{LogHTTP: true}, nil, func() bool { return false })
	req := httptest.NewRequest(http.MethodGet, "/__health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}

	if !strings.Contains(buf.String(), "status=503") {
		t.Fatalf("expected status=503 in access log, got %q", buf.String())
	}
}

func TestNewNoHTTPAccessLogByDefault(t *testing.T) {
	buf := bufferDefaultLogger()
	defer restoreDefaultLogger()

	h := New(Config{}, nil, func() bool { return true })
	req := httptest.NewRequest(http.MethodGet, "/__health", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	if bytes.Contains([]byte(buf.String()), []byte("http request")) {
		t.Fatalf("access log should be disabled by default, got %q", buf.String())
	}
}
