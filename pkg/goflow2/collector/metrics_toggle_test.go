package collector

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/netsampler/goflow2/v3/metrics"

	"github.com/stretchr/testify/require"
)

func newTestCollector(t *testing.T, rc *metrics.RouterCounter) *Collector {
	t.Helper()
	c, err := New(Config{
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		RouterCounter: rc,
	})
	require.NoError(t, err)
	return c
}

func TestMetricsDisabledWithoutRouterCounter(t *testing.T) {
	disabled := newTestCollector(t, nil)
	require.Nil(t, disabled.receiverCallback())
	require.Empty(t, disabled.templateStoreOptions())

	rc := metrics.NewRouterCounter(time.Hour, 24*time.Hour)
	defer rc.Stop()

	enabled := newTestCollector(t, rc)
	require.NotNil(t, enabled.receiverCallback())
	require.NotEmpty(t, enabled.templateStoreOptions())
}
