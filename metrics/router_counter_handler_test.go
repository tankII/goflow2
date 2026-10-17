package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeviceListHandlerDisabled(t *testing.T) {
	rec := httptest.NewRecorder()
	DeviceListHandler(nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/device-list", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.JSONEq(t, `[]`, rec.Body.String())
}

func TestDeviceStatsHandlersDisabled(t *testing.T) {
	cases := []struct {
		name string
		h    http.HandlerFunc
	}{
		{"hourly", DeviceHourlyHandler(nil)},
		{"daily", DeviceDailyHandler(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/device-stats/"+tc.name+"?router=10.0.0.1", nil)
			tc.h.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			require.JSONEq(t,
				`{"router":"10.0.0.1","window":"`+tc.name+`","buckets":[]}`,
				rec.Body.String())
		})
	}
}
