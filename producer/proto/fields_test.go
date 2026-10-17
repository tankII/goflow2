package protoproducer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtoProducerFieldsDefaultNative(t *testing.T) {
	fm, err := mapFormat(&ProducerConfig{})
	require.Nil(t, err)

	p := &ProtoProducer{cfg: &producerConfigMapped{Formatter: fm}}

	fields := p.Fields()
	require.NotEmpty(t, fields)

	names := make([]string, 0, len(fields))
	for _, f := range fields {
		assert.Equal(t, FlowFieldNative, f.Kind)
		assert.Empty(t, f.Source)
		names = append(names, f.Name)
	}
	assert.Contains(t, names, "bytes")
	assert.Contains(t, names, "packets")
}

func TestProtoProducerFieldsKinds(t *testing.T) {
	cfg := &ProducerConfig{}
	cfg.Formatter.Fields = []string{"bytes", "cluster", "custom_virtual", "SamplerAddress"}
	cfg.Formatter.Rename = map[string]string{"SamplerAddress": "sampler"}
	cfg.Formatter.Literal = map[string]interface{}{"cluster": "dc1"}
	cfg.Formatter.Render = map[string]RendererID{"custom_virtual": RendererNone}

	fm, err := mapFormat(cfg)
	require.Nil(t, err)

	p := &ProtoProducer{cfg: &producerConfigMapped{Formatter: fm}}

	fields := p.Fields()
	require.Len(t, fields, 4)

	assert.Equal(t, FlowField{Name: "bytes", Kind: FlowFieldNative}, fields[0])
	assert.Equal(t, FlowField{Name: "cluster", Kind: FlowFieldLiteral}, fields[1])
	assert.Equal(t, FlowField{Name: "custom_virtual", Kind: FlowFieldVirtual}, fields[2])
	assert.Equal(t, FlowField{Name: "sampler", Kind: FlowFieldRenamed, Source: "SamplerAddress"}, fields[3])
}
