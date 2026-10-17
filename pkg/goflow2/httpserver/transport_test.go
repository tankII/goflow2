package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/netsampler/goflow2/v3/transport"
	"github.com/stretchr/testify/require"
)

func TestTransportStatsEndpoint(t *testing.T) {
	h := New(Config{
		TransportStatsPath: "/transport-stats",
		TransportStatsSource: func() []transport.TargetStat {
			return []transport.TargetStat{
				{
					Addr:        "10.0.0.1:9995",
					Depth:       3,
					Capacity:    1024,
					Utilization: 0.0029296875,
					Dropped:     7,
					Connected:   true,
					Drop:        true,
				},
			}
		},
	}, nil, func() bool { return true })

	req := httptest.NewRequest(http.MethodGet, "/transport-stats", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.JSONEq(t, `[{"addr":"10.0.0.1:9995","depth":3,"capacity":1024,"utilization":0.0029296875,"dropped":7,"connected":true,"drop":true}]`, rec.Body.String())
}

func TestTransportStatsEndpointDefaultPath(t *testing.T) {
	h := New(Config{
		TransportStatsSource: func() []transport.TargetStat { return nil },
	}, nil, func() bool { return true })

	req := httptest.NewRequest(http.MethodGet, "/transport-stats", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `[]`, rec.Body.String())
}

func TestTransportStatsEndpointDisabledWithoutSource(t *testing.T) {
	h := New(Config{}, nil, func() bool { return true })

	req := httptest.NewRequest(http.MethodGet, "/transport-stats", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}
