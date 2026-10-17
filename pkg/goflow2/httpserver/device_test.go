package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeviceEndpointsWithoutRouterCounter(t *testing.T) {
	h := New(Config{}, nil, func() bool { return true })

	cases := []struct {
		path string
		body string
	}{
		{"/device-list", `[]`},
		{"/device-stats/hourly?router=10.0.0.1", `{"router":"10.0.0.1","window":"hourly","buckets":[]}`},
		{"/device-stats/daily?router=10.0.0.1", `{"router":"10.0.0.1","window":"daily","buckets":[]}`},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))

			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.JSONEq(t, tc.body, rec.Body.String())
		})
	}
}
