package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/netsampler/goflow2/v3/pkg/goflow2/collector"
	protoproducer "github.com/netsampler/goflow2/v3/producer/proto"
	"github.com/netsampler/goflow2/v3/transport"
	"github.com/stretchr/testify/require"
)

func TestWebDashboardEndpoint(t *testing.T) {
	h := New(Config{
		WebPath: "/ui",
		QueueStatsSource: func() []collector.QueueStat {
			return nil
		},
		TransportStatsSource: func() []transport.TargetStat {
			return nil
		},
		Transport: "netcat",
		Format:    "json",
		FlowFields: []protoproducer.FlowField{
			{Name: "bytes", Kind: protoproducer.FlowFieldNative},
			{Name: "sampler", Kind: protoproducer.FlowFieldRenamed, Source: "SamplerAddress"},
		},
	}, nil, func() bool { return true })

	req := httptest.NewRequest(http.MethodGet, "/ui", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))
	body := rec.Body.String()
	require.NotContains(t, body, dashboardConfigPlaceholder)
	require.Contains(t, body, `"queueStatsPath":"/queue-stats"`)
	require.Contains(t, body, `"transportStatsPath":"/transport-stats"`)
	require.Contains(t, body, `"healthPath":"/__health"`)
	require.Contains(t, body, `"metricsPath":"/metrics"`)
	require.Contains(t, body, `"transport":"netcat"`)
	require.Contains(t, body, `"format":"json"`)
	require.Contains(t, body, `"flowFields":[{"name":"bytes","kind":"native"},{"name":"sampler","kind":"renamed","source":"SamplerAddress"}]`)

	// The send-queue section and the drill-down drawer are always part of the
	// page, regardless of which stats sources are available.
	require.Contains(t, body, "发送队列")
	require.Contains(t, body, "设备统计")
	require.Contains(t, body, `id="drawer"`)
	require.Contains(t, body, "/hourly?router=")
	require.Contains(t, body, "/daily?router=")

	// The overview row always exposes transport, format, and the field list.
	require.Contains(t, body, `id="transport"`)
	require.Contains(t, body, `id="format"`)
	require.Contains(t, body, `id="flowfields"`)
	// 顶部的「字段」按钮已移除：输出字段改为配置页的常驻表格卡片
	require.NotContains(t, body, `id="fields-toggle"`)
	require.Contains(t, body, "输出字段名")

	// Four mutually exclusive views: 收发 / 设备 / 配置 / 模板.
	// The 配置 view holds the runtime-parameter card and the output-field card.
	require.Contains(t, body, `id="tab-io"`)
	require.Contains(t, body, `id="tab-devices"`)
	require.Contains(t, body, `id="tab-config"`)
	require.Contains(t, body, `id="tab-templates"`)
	require.Contains(t, body, `id="view-io"`)
	require.Contains(t, body, `id="view-devices"`)
	require.Contains(t, body, `id="view-config"`)
	require.Contains(t, body, `id="view-templates"`)
	require.Contains(t, body, `id="params"`)
	require.Contains(t, body, "运行参数")
	require.Contains(t, body, "输出字段")

	// The drill-down drawer offers hourly/daily tabs and a line/bar style
	// selector, and plots packets and flows as separate single-series rows
	// (each with its own scale) so proportional series cannot coincide.
	require.Contains(t, body, "按小时")
	require.Contains(t, body, "按日")
	require.Contains(t, body, "柱图")
	require.Contains(t, body, "折线")
	require.Contains(t, body, "单序列图")
}

func TestWebDashboardEndpointDisabled(t *testing.T) {
	h := New(Config{}, nil, func() bool { return true })

	req := httptest.NewRequest(http.MethodGet, "/ui", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestWebDashboardCustomPath(t *testing.T) {
	h := New(Config{
		WebPath: "/dashboard",
	}, nil, func() bool { return true })

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/ui", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestWebDashboardEmptyPathsWhenUnavailable(t *testing.T) {
	h := New(Config{
		WebPath: "/ui",
	}, nil, func() bool { return true })

	req := httptest.NewRequest(http.MethodGet, "/ui", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.NotContains(t, body, dashboardConfigPlaceholder)
	require.Contains(t, body, `"queueStatsPath":""`)
	require.Contains(t, body, `"transportStatsPath":""`)
	require.Contains(t, body, `"deviceStatsPath":"/device-stats"`)
	require.Contains(t, body, `"deviceListPath":"/device-list"`)
	require.Contains(t, body, `"storePath":""`)

	// Even with no stats source, the sections stay present (the frontend shows
	// an explanatory message instead of hiding them, so nothing looks missing).
	require.Contains(t, body, "发送队列")
	require.Contains(t, body, "当前传输不提供发送队列")
}
