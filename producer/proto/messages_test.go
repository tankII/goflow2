package protoproducer

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"gopkg.in/yaml.v3"

	"github.com/stretchr/testify/assert"
)

func TestMarshalJSON(t *testing.T) {
	var m ProtoProducerMessage

	m.formatter = &FormatterConfigMapper{
		fields: []string{"Etype", "test1", "test2", "test3"},
		rename: map[string]string{
			"Etype": "etype",
		},
		numToPb: map[int32]ProtobufFormatterConfig{
			100: ProtobufFormatterConfig{
				Name:  "test1",
				Index: 100,
				Type:  "varint",
				Array: false,
			},
			101: ProtobufFormatterConfig{
				Name:  "test2",
				Index: 101,
				Type:  "string",
				Array: false,
			},
			102: ProtobufFormatterConfig{
				Name:  "test3",
				Index: 102,
				Type:  "bytes",
				Array: false,
			},
		},
		render: map[string]RenderFunc{
			"Etype": EtypeRenderer,
			"test1": EtypeRenderer,
			"test2": NilRenderer,
			"test3": StringRenderer,
		},
	}

	m.Etype = 0x86dd

	fmr := m.ProtoReflect()
	unk := fmr.GetUnknown()

	unk = protowire.AppendTag(unk, protowire.Number(100), protowire.VarintType)
	unk = protowire.AppendVarint(unk, 0x86dd)

	unk = protowire.AppendTag(unk, protowire.Number(101), protowire.BytesType)
	unk = protowire.AppendString(unk, string("testing"))

	unk = protowire.AppendTag(unk, protowire.Number(102), protowire.BytesType)
	unk = protowire.AppendString(unk, string([]byte{0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67}))

	fmr.SetUnknown(unk)

	out, err := m.MarshalJSON()
	assert.Nil(t, err)
	t.Log(string(out))
	assert.Equal(t, "{\"etype\":\"IPv6\",\"test1\":\"IPv6\",\"test2\":\"74657374696e67\",\"test3\":\"testing\"}", string(out))
}

func TestCSVValues(t *testing.T) {
	var m ProtoProducerMessage

	m.formatter = &FormatterConfigMapper{
		fields: []string{"Etype", "missing", "test3", "Etype"},
		rename: map[string]string{
			"Etype": "etype",
		},
		numToPb: map[int32]ProtobufFormatterConfig{
			102: ProtobufFormatterConfig{
				Name:  "test3",
				Index: 102,
				Type:  "bytes",
				Array: false,
			},
		},
		render: map[string]RenderFunc{
			"Etype": EtypeRenderer,
			"test3": StringRenderer,
		},
	}

	m.Etype = 0x0800

	fmr := m.ProtoReflect()
	unk := fmr.GetUnknown()
	unk = protowire.AppendTag(unk, protowire.Number(102), protowire.BytesType)
	unk = protowire.AppendString(unk, string([]byte{0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67}))
	fmr.SetUnknown(unk)

	// "missing" is not a virtual field nor present on the message:
	// it must map to an empty string so CSV columns stay aligned.
	assert.Equal(t, []string{"IPv4", "", "testing", "IPv4"}, m.CSVValues())
}

func TestCSVValuesLiteral(t *testing.T) {
	var m ProtoProducerMessage

	m.formatter = &FormatterConfigMapper{
		fields: []string{"Etype", "flows", "missing", "test3"},
		rename: map[string]string{
			"Etype": "etype",
		},
		numToPb: map[int32]ProtobufFormatterConfig{
			102: ProtobufFormatterConfig{
				Name:  "test3",
				Index: 102,
				Type:  "bytes",
				Array: false,
			},
		},
		render: map[string]RenderFunc{
			"Etype": EtypeRenderer,
			"test3": StringRenderer,
		},
		literal: map[string]string{
			"flows": "1",
		},
	}

	m.Etype = 0x0800

	fmr := m.ProtoReflect()
	unk := fmr.GetUnknown()
	unk = protowire.AppendTag(unk, protowire.Number(102), protowire.BytesType)
	unk = protowire.AppendString(unk, string([]byte{0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67}))
	fmr.SetUnknown(unk)

	// "flows" is a literal column: constant "1" in its configured position,
	// while the genuinely missing field stays empty for column alignment.
	assert.Equal(t, []string{"IPv4", "1", "", "testing"}, m.CSVValues())
}

func TestMarshalJSONLiteral(t *testing.T) {
	var m ProtoProducerMessage

	m.formatter = &FormatterConfigMapper{
		fields: []string{"Etype", "flows"},
		rename: map[string]string{
			"Etype": "etype",
		},
		render: map[string]RenderFunc{
			"Etype": EtypeRenderer,
		},
		literal: map[string]string{
			"flows": "1",
		},
	}

	m.Etype = 0x0800

	out, err := m.MarshalJSON()
	assert.Nil(t, err)
	assert.Equal(t, "{\"etype\":\"IPv4\",\"flows\":\"1\"}", string(out))
}

func TestLiteralMappingConfig(t *testing.T) {
	var cfg ProducerConfig

	err := yaml.Unmarshal([]byte(`
formatter:
  fields:
    - Etype
    - flows
  literal:
    flows: 1
`), &cfg)
	assert.Nil(t, err)

	// "flows" is neither a protobuf field nor a virtual renderer; the
	// literal section must make it accepted and expose the constant value.
	mapped, err := cfg.Compile()
	assert.Nil(t, err)

	fm := mapped.GetFormatter()
	assert.Equal(t, []string{"Etype", "flows"}, fm.Fields())

	val, ok := fm.Literal("flows")
	assert.True(t, ok)
	assert.Equal(t, "1", val)
}
