package metrics

import (
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/netsampler/goflow2/v3/producer"
	"github.com/netsampler/goflow2/v3/utils"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// counterValue reads the current value of a counter without pulling in the
// Prometheus testutil package (which would require go.mod changes).
func counterValue(c prometheus.Counter) float64 {
	var dm dto.Metric
	if err := c.Write(&dm); err != nil {
		panic(err)
	}
	return dm.GetCounter().GetValue()
}

type stubProducer struct{}

func (s *stubProducer) Produce(msg interface{}, args *producer.ProduceArgs) ([]producer.ProducerMessage, error) {
	return nil, nil
}
func (s *stubProducer) Close()                            {}
func (s *stubProducer) Commit([]producer.ProducerMessage) {}

func TestWrapPromProducerDisabledWithoutRouterCounter(t *testing.T) {
	p := &stubProducer{}

	gotDisabled := WrapPromProducer(p, nil)
	_, wrapped := gotDisabled.(*PromProducerWrapper)
	require.False(t, wrapped, "producer must not be wrapped when metrics are disabled")

	rc := NewRouterCounter(time.Hour, 24*time.Hour)
	defer rc.Stop()

	gotEnabled := WrapPromProducer(p, rc)
	_, wrapped = gotEnabled.(*PromProducerWrapper)
	require.True(t, wrapped, "producer must be wrapped when metrics are enabled")
}

// BenchmarkNetFlowTimeStatsCached measures the per-flow cost after the
// (router,version) observer handle has been cached.
func BenchmarkNetFlowTimeStatsCached(b *testing.B) {
	p := &PromProducerWrapper{timeStats: sync.Map{}}
	obs := p.timeStatsObserver("10.0.0.1", "9")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obs.Observe(0.001)
	}
}

// BenchmarkNetFlowTimeStatsUncached measures the previous per-flow cost of
// rebuilding and hashing the label map on every record.
func BenchmarkNetFlowTimeStatsUncached(b *testing.B) {
	labels := prometheus.Labels{"router": "10.0.0.1", "version": "9"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NetFlowTimeStatsSum.With(labels).Observe(0.001)
	}
}

func TestPromDecoderWrapperDisabledWithoutRouterCounter(t *testing.T) {
	src := netip.MustParseAddrPort("203.0.113.7:4000")
	dst := netip.MustParseAddrPort("203.0.113.9:2055")
	labels := prometheus.Labels{
		"remote_ip":  src.Addr().Unmap().String(),
		"local_ip":   dst.Addr().Unmap().String(),
		"local_port": "2055",
		"type":       "netflow",
	}
	count := func() float64 {
		return counterValue(MetricTrafficPackets.With(labels))
	}

	before := count()
	called := 0
	inner := func(msg interface{}) error { called++; return nil }

	dec := PromDecoderWrapper(inner, "netflow", nil)
	require.NoError(t, dec(&utils.Message{Src: src, Dst: dst, Payload: []byte("abcd")}))
	require.Equal(t, 1, called, "wrapped decoder must still run when metrics are disabled")
	require.Equal(t, before, count(), "no packet metric must be emitted when metrics are disabled")

	rc := NewRouterCounter(time.Hour, 24*time.Hour)
	defer rc.Stop()

	decEnabled := PromDecoderWrapper(inner, "netflow", rc)
	require.NoError(t, decEnabled(&utils.Message{Src: src, Dst: dst, Payload: []byte("abcd")}))
	require.Greater(t, count(), before, "packet metric must be emitted when metrics are enabled")
}
