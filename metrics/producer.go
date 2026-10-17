package metrics

import (
	"fmt"
	"net/netip"
	"sync"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
	"github.com/netsampler/goflow2/v3/decoders/sflow"
	"github.com/netsampler/goflow2/v3/producer"
	"github.com/netsampler/goflow2/v3/producer/proto"

	"github.com/prometheus/client_golang/prometheus"
)

// timeStatsKey identifies a cached per-(router,version) histogram handle.
type timeStatsKey struct {
	router  string
	version string
}

// PromProducerWrapper wraps a producer to emit Prometheus metrics.
type PromProducerWrapper struct {
	wrapped       producer.ProducerInterface
	routerCounter *RouterCounter

	// timeStats caches NetFlowTimeStatsSum observer handles so the per-flow
	// hot path avoids rebuilding a label map and hashing it on every record.
	// Cardinality is bounded by (number of routers x IPFIX/NetFlow versions).
	timeStats sync.Map // timeStatsKey -> prometheus.Observer
}

// Produce forwards to the wrapped producer and updates metrics.
func (p *PromProducerWrapper) Produce(msg interface{}, args *producer.ProduceArgs) ([]producer.ProducerMessage, error) {
	flowMessageSet, err := p.wrapped.Produce(msg, args)
	if err != nil {
		return flowMessageSet, fmt.Errorf("metrics producer: %w", err)
	}
	key := args.Src.Addr().Unmap().String()
	if p.routerCounter != nil {
		p.routerCounter.IncFlow(key, int64(len(flowMessageSet)))
	}
	var nfvariant bool
	var versionStr string
	switch packet := msg.(type) {
	case *sflow.Packet:
		agentStr := "unk"
		agentIp, ok := netip.AddrFromSlice(packet.AgentIP)
		if ok {
			agentStr = agentIp.String()
		}

		SFlowStats.With(
			prometheus.Labels{
				"router":  key,
				"agent":   agentStr,
				"version": "5",
			}).
			Inc()

		for _, samples := range packet.Samples {
			typeStr := "unknown"
			countRec := 0
			switch samplesConv := samples.(type) {
			case sflow.FlowSample:
				typeStr = "FlowSample"
				countRec = len(samplesConv.Records)
			case sflow.CounterSample:
				typeStr = "CounterSample"
				if samplesConv.Header.Format == 4 {
					typeStr = "Expanded" + typeStr
				}
				countRec = len(samplesConv.Records)
			case sflow.ExpandedFlowSample:
				typeStr = "ExpandedFlowSample"
				countRec = len(samplesConv.Records)
			}
			SFlowSampleStatsSum.With(
				prometheus.Labels{
					"router":  key,
					"agent":   agentStr,
					"version": "5",
					"type":    typeStr,
				}).
				Inc()

			SFlowSampleRecordsStatsSum.With(
				prometheus.Labels{
					"router":  key,
					"agent":   agentStr,
					"version": "5",
					"type":    typeStr,
				}).
				Add(float64(countRec))
		}

	case *netflowlegacy.PacketNetFlowV5:
		NetFlowStats.With(
			prometheus.Labels{
				"router":  key,
				"version": "5",
			}).
			Inc()
		NetFlowSetStatsSum.With(
			prometheus.Labels{
				"router":  key,
				"version": "5",
				"type":    "DataFlowSet",
			}).
			Add(float64(packet.Count))

	case *netflow.NFv9Packet:
		NetFlowStats.With(
			prometheus.Labels{
				"router":  key,
				"version": "9",
			}).
			Inc()
		recordCommonNetFlowMetrics(9, key, packet.FlowSets)
		nfvariant = true
		versionStr = "9"

	case *netflow.IPFIXPacket:
		NetFlowStats.With(
			prometheus.Labels{
				"router":  key,
				"version": "10",
			}).
			Inc()
		recordCommonNetFlowMetrics(10, key, packet.FlowSets)
		nfvariant = true
		versionStr = "10"
	}

	if nfvariant {
		// resolve the observer once per packet instead of once per flow, so
		// the per-record hot path avoids rebuilding and hashing a label map
		obs := p.timeStatsObserver(key, versionStr)
		for _, msg := range flowMessageSet {
			fmsg, ok := msg.(*protoproducer.ProtoProducerMessage)
			if !ok {
				continue
			}
			timeDiff := fmsg.TimeReceivedNs - fmsg.TimeFlowEndNs
			obs.Observe(float64(timeDiff) / 1e9)
		}
	}

	return flowMessageSet, nil
}

// timeStatsObserver returns the cached NetFlowTimeStatsSum observer for the
// given router and version, creating and caching it on first use.
func (p *PromProducerWrapper) timeStatsObserver(router, version string) prometheus.Observer {
	k := timeStatsKey{router: router, version: version}
	if obs, ok := p.timeStats.Load(k); ok {
		return obs.(prometheus.Observer)
	}
	obs := NetFlowTimeStatsSum.With(prometheus.Labels{"router": router, "version": version})
	actual, _ := p.timeStats.LoadOrStore(k, obs)
	return actual.(prometheus.Observer)
}

// Close forwards Close to the wrapped producer.
func (p *PromProducerWrapper) Close() {
	p.wrapped.Close()
}

// Commit forwards Commit to the wrapped producer.
func (p *PromProducerWrapper) Commit(flowMessageSet []producer.ProducerMessage) {
	p.wrapped.Commit(flowMessageSet)
}

// WrapPromProducer wraps a producer with metrics reporting. When rc is nil
// metrics are disabled and the producer is returned unchanged.
func WrapPromProducer(wrapped producer.ProducerInterface, rc *RouterCounter) producer.ProducerInterface {
	if rc == nil {
		return wrapped
	}
	return &PromProducerWrapper{
		wrapped:       wrapped,
		routerCounter: rc,
	}
}

// metrics template system
