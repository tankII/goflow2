// Package protoproducer encodes flow data into protobuf messages.
package protoproducer

import (
	"fmt"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
	"github.com/netsampler/goflow2/v3/decoders/sflow"
	"github.com/netsampler/goflow2/v3/producer"
	"github.com/netsampler/goflow2/v3/utils/store/samplingrate"
)

// ProtoProducer converts decoded packets into protobuf flow messages.
type ProtoProducer struct {
	cfg           ProtoProducerConfig
	samplingStore samplingrate.Store
}

// FlowField describes one output column produced by the mapping formatter.
type FlowField struct {
	// Name is the output name (post-rename) used in the emitted record.
	Name string `json:"name"`
	// Kind classifies the origin of the field.
	Kind string `json:"kind"`
	// Source is the original mapping key when Name was renamed.
	Source string `json:"source,omitempty"`
}

// FlowField kinds.
const (
	FlowFieldNative  = "native"
	FlowFieldRenamed = "renamed"
	FlowFieldLiteral = "literal"
	FlowFieldVirtual = "virtual"
)

// Fields returns the output record fields in formatter order, tagged with
// their origin, mirroring what the JSON/CSV/text formatters emit.
func (p *ProtoProducer) Fields() []FlowField {
	if p == nil || p.cfg == nil {
		return nil
	}
	fm := p.cfg.GetFormatter()
	if fm == nil {
		return nil
	}
	fields := fm.Fields()
	out := make([]FlowField, 0, len(fields))
	for _, name := range fields {
		f := FlowField{Name: name, Kind: FlowFieldNative}

		renamed := false
		if disp, ok := fm.Rename(name); ok && disp != "" {
			f.Name = disp
			f.Source = name
			renamed = true
		}

		if _, ok := fm.Literal(name); ok {
			f.Kind = FlowFieldLiteral
		} else if renamed {
			f.Kind = FlowFieldRenamed
		} else if _, ok := fm.Remap(name); !ok {
			f.Kind = FlowFieldVirtual
		}

		out = append(out, f)
	}
	return out
}

func (p *ProtoProducer) enrich(flowMessageSet []producer.ProducerMessage, cb func(msg *ProtoProducerMessage)) {
	for _, msg := range flowMessageSet {
		fmsg, ok := msg.(*ProtoProducerMessage)
		if !ok {
			continue
		}
		cb(fmsg)
	}
}

func (p *ProtoProducer) Produce(msg interface{}, args *producer.ProduceArgs) (flowMessageSet []producer.ProducerMessage, err error) {
	tr := uint64(args.TimeReceived.UnixNano())
	sa, _ := args.SamplerAddress.Unmap().MarshalBinary()
	ctx := netflow.FlowContext{RouterKey: args.Src.String()}
	if args.FlowContext != nil {
		ctx = *args.FlowContext
	}
	switch msgConv := msg.(type) {
	case *netflowlegacy.PacketNetFlowV5:
		flowMessageSet, err = ProcessMessageNetFlowLegacy(msgConv)

		p.enrich(flowMessageSet, func(fmsg *ProtoProducerMessage) {
			fmsg.TimeReceivedNs = tr
			fmsg.SamplerAddress = sa
		})
	case *netflow.NFv9Packet:
		flowMessageSet, err = ProcessMessageNetFlowV9Config(msgConv, ctx, p.samplingStore, p.cfg)

		p.enrich(flowMessageSet, func(fmsg *ProtoProducerMessage) {
			fmsg.TimeReceivedNs = tr
			fmsg.SamplerAddress = sa
		})
	case *netflow.IPFIXPacket:
		flowMessageSet, err = ProcessMessageIPFIXConfig(msgConv, ctx, p.samplingStore, p.cfg)

		p.enrich(flowMessageSet, func(fmsg *ProtoProducerMessage) {
			fmsg.TimeReceivedNs = tr
			fmsg.SamplerAddress = sa
		})
	case *sflow.Packet:
		flowMessageSet, err = ProcessMessageSFlowConfig(msgConv, p.cfg)

		p.enrich(flowMessageSet, func(fmsg *ProtoProducerMessage) {
			fmsg.TimeReceivedNs = tr
			fmsg.TimeFlowStartNs = tr
			fmsg.TimeFlowEndNs = tr
		})
	default:
		return flowMessageSet, fmt.Errorf("flow not recognized")
	}

	p.enrich(flowMessageSet, func(fmsg *ProtoProducerMessage) {
		fmsg.formatter = p.cfg.GetFormatter()
	})
	if err != nil {
		return flowMessageSet, fmt.Errorf("proto producer %T: %w", msg, err)
	}
	return flowMessageSet, nil
}

// Commit returns messages to the pool.
func (p *ProtoProducer) Commit(flowMessageSet []producer.ProducerMessage) {
	for _, fmsg := range flowMessageSet {
		protoMessagePool.Put(fmsg)
	}
}

// Close is a no-op for ProtoProducer.
func (p *ProtoProducer) Close() {
	if p.samplingStore != nil {
		p.samplingStore.Close()
	}
}

// CreateProtoProducer creates a ProtoProducer with config and sampling system.
func CreateProtoProducer(cfg ProtoProducerConfig, samplingStore samplingrate.Store) (producer.ProducerInterface, error) {
	if samplingStore == nil {
		samplingStore = samplingrate.NewSamplingRateFlowStore()
	}
	samplingStore.Start()

	p := &ProtoProducer{
		cfg:           cfg,
		samplingStore: samplingStore,
	}

	return p, nil
}
