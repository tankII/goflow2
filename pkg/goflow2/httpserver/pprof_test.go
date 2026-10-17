package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPprofDisabledByDefault(t *testing.T) {
	h := New(Config{}, nil, func() bool { return true })

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestPprofEnabled(t *testing.T) {
	h := New(Config{Pprof: true}, nil, func() bool { return true })

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline", "/debug/pprof/goroutine"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

			require.Equal(t, http.StatusOK, rec.Code)
		})
	}
}
