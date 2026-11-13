package app

import (
	"flag"
	"testing"
	"time"

	"github.com/netsampler/goflow2/v3/metrics"
	"github.com/netsampler/goflow2/v3/pkg/goflow2/config"
	"github.com/netsampler/goflow2/v3/pkg/goflow2/listen"

	_ "github.com/netsampler/goflow2/v3/format/json"
	_ "github.com/netsampler/goflow2/v3/transport/file"

	"github.com/stretchr/testify/require"
)

func newTestApp(t *testing.T, metricsEnabled bool) *App {
	t.Helper()

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := config.BindFlags(fs)
	args := []string{"-produce=raw", "-format=json", "-transport=file", "-addr=", "-listen=sflow://:6343"}
	if !metricsEnabled {
		args = append(args, "-metrics=false")
	}
	require.NoError(t, fs.Parse(args))

	a, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		if a.routerCounter != nil {
			a.routerCounter.Stop()
		}
	})
	return a
}

func TestRouterCounterDisabledWithMetricsOff(t *testing.T) {
	a := newTestApp(t, false)

	require.Nil(t, a.routerCounter, "no device aggregation must be created when metrics are off")

	_, wrapped := a.producer.(*metrics.PromProducerWrapper)
	require.False(t, wrapped, "producer must not be wrapped in Prometheus metrics when metrics are off")
}

func TestRouterCounterEnabledWithMetricsOn(t *testing.T) {
	a := newTestApp(t, true)

	require.NotNil(t, a.routerCounter)
	_, wrapped := a.producer.(*metrics.PromProducerWrapper)
	require.True(t, wrapped)
}

func TestBuildDashboardParams(t *testing.T) {
	cfg := &config.Config{
		ListenAddresses:         "netflow://:2055?count=4&workers=8&queue_size=50000",
		Transport:               "netcat",
		Format:                  "csv",
		Produce:                 "sample",
		Addr:                    ":18080",
		Metrics:                 true,
		UDPRecvBuffer:           4 * 1024 * 1024,
		TemplatesTTL:            time.Hour,
		TemplatesExtendOnAccess: true,
		SamplingRatesTTL:        time.Hour,
	}
	listeners, err := listen.ParseListenAddresses(cfg.ListenAddresses)
	require.NoError(t, err)

	groups := buildDashboardParams(cfg, listeners)
	require.Len(t, groups, 3)
	require.Equal(t, "接收", groups[0].Group)
	require.Equal(t, "发送", groups[1].Group)
	require.Equal(t, "运行时", groups[2].Group)

	// 接收组：应含监听地址、socket/worker、队列容量、socket 接收缓冲
	recv := groups[0].Items
	require.NotEmpty(t, recv)
	joined := ""
	for _, it := range recv {
		joined += it.Label + "=" + it.Value + ";"
	}
	require.Contains(t, joined, "*:2055")
	require.Contains(t, joined, "4 / 8")
	require.Contains(t, joined, "50000")
	require.Contains(t, joined, "4 MiB")

	// 运行时组：模板 TTL 与访问续期应一并体现
	run := ""
	for _, it := range groups[2].Items {
		run += it.Label + "=" + it.Value + ";"
	}
	require.Contains(t, run, "1h0m0s")
	require.Contains(t, run, "访问续期=开")
}

func TestHumanBytes(t *testing.T) {
	require.Equal(t, "512 B", humanBytes(512))
	require.Equal(t, "4 KiB", humanBytes(4*1024))
	require.Equal(t, "4 MiB", humanBytes(4*1024*1024))
	require.Equal(t, "2.0 GiB", humanBytes(2*1024*1024*1024))
	require.Equal(t, "0 B", humanBytes(0))
}

func TestOnOff(t *testing.T) {
	require.Equal(t, "开", onOff(true))
	require.Equal(t, "关", onOff(false))
}
