package app

import (
	"flag"
	"testing"

	"github.com/netsampler/goflow2/v3/metrics"
	"github.com/netsampler/goflow2/v3/pkg/goflow2/config"

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
