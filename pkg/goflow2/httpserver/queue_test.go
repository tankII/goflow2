package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/netsampler/goflow2/v3/pkg/goflow2/collector"
	"github.com/stretchr/testify/require"
)

func TestQueueStatsEndpoint(t *testing.T) {
	h := New(Config{
		QueueStatsPath: "/queue-stats",
		QueueStatsSource: func() []collector.QueueStat {
			return []collector.QueueStat{
				{Scheme: "netflow", Hostname: "", Port: 2055, Depth: 2, Capacity: 4, Utilization: 0.5},
			}
		},
	}, nil, func() bool { return true })

	req := httptest.NewRequest(http.MethodGet, "/queue-stats", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.JSONEq(t, `[{"scheme":"netflow","hostname":"","port":2055,"depth":2,"capacity":4,"utilization":0.5}]`, rec.Body.String())
}

func TestQueueStatsEndpointDefaultPath(t *testing.T) {
	h := New(Config{
		QueueStatsSource: func() []collector.QueueStat { return nil },
	}, nil, func() bool { return true })

	req := httptest.NewRequest(http.MethodGet, "/queue-stats", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `[]`, rec.Body.String())
}

func TestQueueStatsEndpointDisabledWithoutSource(t *testing.T) {
	h := New(Config{}, nil, func() bool { return true })

	req := httptest.NewRequest(http.MethodGet, "/queue-stats", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}
