package protoproducer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppendCSVRowScalarDirect pins the fast path for scalar columns: a message
// whose columns are plain protobuf fields (no custom renderer) must render the
// exact same bytes as the legacy values-based pipeline.
func TestAppendCSVRowScalarDirect(t *testing.T) {
	var m ProtoProducerMessage
	m.formatter = &FormatterConfigMapper{
		fields: []string{
			"Proto", "SrcPort", "DstPort", "SrcAs", "DstAs",
			"Packets", "Bytes", "IpTos", "ForwardingStatus", "TcpFlags",
		},
		render: map[string]RenderFunc{},
	}

	m.Proto = 6
	m.SrcPort = 54321
	m.DstPort = 443
	m.SrcAs = 65001
	m.DstAs = 65002
	m.Packets = 17
	m.Bytes = 4096
	m.IpTos = 128
	m.ForwardingStatus = 2
	m.TcpFlags = 0x1b

	want := legacyRowRef(&m, "|")
	got := string(m.AppendCSVRow(nil, "|"))
	assert.Equal(t, want, got)
}

// TestAppendCSVRowScalarEdgeValues covers zero, maximum and large-magnitude
// values, where strconv formatting differs subtly from boxing.
func TestAppendCSVRowScalarEdgeValues(t *testing.T) {
	var m ProtoProducerMessage
	m.formatter = &FormatterConfigMapper{
		fields: []string{"IpTtl", "ZeroValue", "SequenceNum", "SamplingRate", "IcmpCode"},
		render: map[string]RenderFunc{},
	}

	m.IpTtl = 255
	m.SequenceNum = 4294967295
	m.SamplingRate = 18446744073709551615
	// ZeroValue / IcmpCode are left at their zero values.

	want := legacyRowRef(&m, "|")
	got := string(m.AppendCSVRow(nil, "|"))
	assert.Equal(t, want, got)
}

// TestAppendCSVRowScalarMixed guards that the direct scalar path and the
// renderer path coexist correctly in a single row.
func TestAppendCSVRowScalarMixed(t *testing.T) {
	var m ProtoProducerMessage
	m.formatter = &FormatterConfigMapper{
		fields: []string{
			"Proto", "SrcAddr", "SrcPort", "Bytes", "SamplerAddress", "Flows",
		},
		literal: map[string]string{"Flows": "1"},
		render: map[string]RenderFunc{
			"SrcAddr":        IPRenderer,
			"SamplerAddress": IPRenderer,
		},
	}
	m.Proto = 17
	m.SrcAddr = []byte{10, 0, 0, 1}
	m.SrcPort = 80
	m.Bytes = 12345
	m.SamplerAddress = []byte{192, 168, 0, 1}

	want := legacyRowRef(&m, "|")
	got := string(m.AppendCSVRow(nil, "|"))
	assert.Equal(t, want, got)
}

// TestAppendCSVRowScalarEscaping guards that a scalar column needing escaping
// still routes through the escaping rules.
func TestAppendCSVRowScalarEscaping(t *testing.T) {
	var m ProtoProducerMessage
	m.formatter = &FormatterConfigMapper{
		fields: []string{"Proto", "Leading", "Trailing", "Quote"},
		literal: map[string]string{
			"Leading":  " x",
			"Trailing": "y ",
			"Quote":    `a"b`,
		},
		render: map[string]RenderFunc{},
	}
	m.Proto = 6

	want := legacyRowRef(&m, "|")
	got := string(m.AppendCSVRow(nil, "|"))
	assert.Equal(t, want, got)
}

// TestAppendCSVRowScalarProtoBytes guards that a byte-array column (SrcAddr)
// still goes through the renderer path rather than the numeric fast path.
func TestAppendCSVRowScalarProtoBytes(t *testing.T) {
	var m ProtoProducerMessage
	m.formatter = &FormatterConfigMapper{
		fields: []string{"SrcAddr", "SrcNet", "DstAddr"},
		render: map[string]RenderFunc{
			"SrcAddr": IPRenderer,
			"DstAddr": IPRenderer,
		},
	}
	m.SrcAddr = []byte{192, 168, 1, 1}
	m.SrcNet = 24
	m.DstAddr = []byte{8, 8, 8, 8}

	want := legacyRowRef(&m, "|")
	got := string(m.AppendCSVRow(nil, "|"))
	assert.Equal(t, want, got)
}

// TestAppendCSVRowScalarOnlyUnknown guards a row whose columns all come from the
// protobuf unknown-field map (dynamic fields), which must not be mistaken for
// native scalars.
func TestAppendCSVRowScalarOnlyUnknown(t *testing.T) {
	var m ProtoProducerMessage
	m.formatter = &FormatterConfigMapper{
		fields: []string{"Proto", "dyn"},
		render: map[string]RenderFunc{},
		numToPb: map[int32]ProtobufFormatterConfig{
			900: {Name: "dyn", Index: 900, Type: "string"},
		},
	}
	m.Proto = 6

	require.NotNil(t, m.fieldPlan())
	want := legacyRowRef(&m, "|")
	got := string(m.AppendCSVRow(nil, "|"))
	assert.Equal(t, want, got)
}

// TestAppendCSVRowScalarRepeated guards the pooled fieldPart reuse across many
// renders of differently shaped rows.
func TestAppendCSVRowScalarRepeated(t *testing.T) {
	for i := 0; i < 50; i++ {
		var a ProtoProducerMessage
		a.formatter = &FormatterConfigMapper{
			fields: []string{"Proto", "SrcAddr", "Bytes"},
			render: map[string]RenderFunc{"SrcAddr": IPRenderer},
		}
		a.Proto = uint32(i)
		a.SrcAddr = []byte{10, 0, byte(i), 1}
		a.Bytes = uint64(i * 1000)

		var b ProtoProducerMessage
		b.formatter = &FormatterConfigMapper{
			fields: []string{"DstPort", "Packets", "missing"},
			render: map[string]RenderFunc{},
		}
		b.DstPort = uint32(i)
		b.Packets = uint64(i)

		assert.Equal(t, legacyRowRef(&a, "|"), string(a.AppendCSVRow(nil, "|")))
		assert.Equal(t, legacyRowRef(&b, ","), string(b.AppendCSVRow(nil, ",")))
	}
}

// TestAppendCSVRowScalarNoAlloc is the B2 contract: a row made only of plain
// numeric protobuf columns must be rendered without boxing each value into an
// interface and without building an intermediate string, i.e. zero allocations
// once the pools are warm.
func TestAppendCSVRowScalarNoAlloc(t *testing.T) {
	var m ProtoProducerMessage
	m.formatter = &FormatterConfigMapper{
		fields: []string{
			"Proto", "SrcPort", "DstPort", "SrcAs", "DstAs",
			"Packets", "Bytes", "IpTos", "ForwardingStatus", "TcpFlags",
		},
		render: map[string]RenderFunc{},
	}
	m.Proto = 6
	m.SrcPort = 54321
	m.DstPort = 443
	m.SrcAs = 65001
	m.DstAs = 65002
	m.Packets = 17
	m.Bytes = 4096
	m.IpTos = 128
	m.ForwardingStatus = 2
	m.TcpFlags = 0x1b

	buf := make([]byte, 0, 1024)
	// warm the fieldPart pool
	m.AppendCSVRow(buf[:0], "|")

	allocs := testing.AllocsPerRun(200, func() {
		buf = m.AppendCSVRow(buf[:0], "|")
	})
	assert.Less(t, allocs, 1.0, "scalar row must not allocate per column, got %v allocs/run", allocs)
	assert.Equal(t, legacyRowRef(&m, "|"), string(buf))
}

// TestAppendCSVRowRendererOnNumericColumn guards that a numeric column with a
// configured renderer (e.g. the built-in Proto/SrcNet/Etype renderers seeded by
// mapFormat) still goes through that renderer instead of the direct scalar path.
func TestAppendCSVRowRendererOnNumericColumn(t *testing.T) {
	var m ProtoProducerMessage
	m.formatter = &FormatterConfigMapper{
		fields: []string{"Proto", "SrcNet", "Etype", "Bytes"},
		render: map[string]RenderFunc{
			"Proto":  ProtoRenderer,
			"SrcNet": NetworkRenderer,
			"Etype":  EtypeRenderer,
		},
	}
	m.Proto = 6
	m.SrcNet = 24
	m.Etype = 0x0800
	m.Bytes = 4096

	// renderer 必须生效："TCP" / "IPv4"，而不是原始数字。
	assert.Equal(t, legacyRowRef(&m, "|"), string(m.AppendCSVRow(nil, "|")))
	assert.Contains(t, string(m.AppendCSVRow(nil, "|")), "TCP|")
	assert.Contains(t, string(m.AppendCSVRow(nil, "|")), "|IPv4|4096")
	assert.Equal(t, legacyRowRef(&m, "|"), string(m.AppendCSVRow(nil, "|")))
}

// TestAppendCSVRowNoRendererNumericIsPlain guards the fast path itself: a
// numeric column with no renderer renders as its decimal value.
func TestAppendCSVRowNoRendererNumericIsPlain(t *testing.T) {
	var m ProtoProducerMessage
	m.formatter = &FormatterConfigMapper{
		fields: []string{"Proto", "Bytes"},
		render: map[string]RenderFunc{},
	}
	m.Proto = 6
	m.Bytes = 4096

	assert.Equal(t, "6|4096", string(m.AppendCSVRow(nil, "|")))
}

// csvIPTimeFixture 构造一条与现网形态一致的消息：2 个时间列 + 3 个 IP 列
// + 若干原生标量列 + 1 个常量列，时间/IP 均使用内建 renderer。
//
// 必须走 mapFormat 这条真实路径：renderIDs（直写旁路的判定依据）由它填充，
// 直接构造 FormatterConfigMapper 会漏掉该信息。
func csvIPTimeFixture(t *testing.T, m *ProtoProducerMessage) {
	t.Helper()
	mapped, err := mapFormat(&ProducerConfig{
		Formatter: FormatterConfig{
			Fields: []string{
				"TimeFlowStartNs", "TimeFlowEndNs", "SrcAddr", "src_port",
				"DstAddr", "dst_port", "SamplerAddress", "packets", "Flows",
			},
			Render: map[string]RendererID{
				"TimeFlowStartNs": "datetimenano:2006-01-02 15:04:05.000",
				"TimeFlowEndNs":   "datetimenano:2006-01-02 15:04:05.000",
			},
			Literal: map[string]interface{}{"Flows": 1},
		},
	})
	require.NoError(t, err)
	m.formatter = mapped
	m.TimeFlowStartNs = 1705732882176 * 1e6
	m.TimeFlowEndNs = 1705732882192 * 1e6
	m.SrcAddr = []byte{192, 0, 2, 10}
	m.SrcPort = 54321
	m.DstAddr = []byte{198, 51, 100, 20}
	m.DstPort = 443
	m.SamplerAddress = []byte{10, 0, 0, 7}
	m.Packets = 17
}

// TestAppendCSVRowIPAndTimeMatchesLegacy pins byte-identical output for the
// IP/time append fast path against the legacy renderer pipeline.
func TestAppendCSVRowIPAndTimeMatchesLegacy(t *testing.T) {
	for _, delim := range []string{"|", ","} {
		var m ProtoProducerMessage
		csvIPTimeFixture(t, &m)
		assert.Equal(t, legacyRowRef(&m, delim), string(m.AppendCSVRow(nil, delim)),
			"delimiter %q", delim)
	}
}

// TestAppendCSVRowIPAndTimeIPv6 covers 16-byte addresses and the empty/invalid
// cases where the renderer returns an empty string.
func TestAppendCSVRowIPAndTimeIPv6(t *testing.T) {
	var m ProtoProducerMessage
	csvIPTimeFixture(t, &m)
	m.SrcAddr = []byte{0x24, 0x09, 0x8f, 0x1f, 0x92, 0x85, 0x2c, 0x04,
		0x70, 0x6b, 0x5a, 0xef, 0xe0, 0x8, 0xc5, 0xe8}
	m.DstAddr = nil // 空地址：renderer 返回 ""

	want := legacyRowRef(&m, "|")
	assert.Equal(t, want, string(m.AppendCSVRow(nil, "|")))
}

// TestAppendCSVRowCustomRendererNotBypassed guards that a user-supplied renderer
// on an IP column is still honored — the append fast path only applies to the
// built-in IP renderer.
func TestAppendCSVRowCustomRendererNotBypassed(t *testing.T) {
	var m ProtoProducerMessage
	csvIPTimeFixture(t, &m)
	// 只替换 render、不动 renderIDs —— 模拟"配置与实际 renderer 不一致"的最坏情况。
	// compilePlan 需检测到不一致并退回字符串路径，否则会绕过自定义 renderer。
	mapped := m.formatter.(*FormatterConfigMapper)
	mapped.render["SrcAddr"] =
		func(_ *ProtoProducerMessage, _ string, data interface{}) interface{} {
			return "CUSTOM"
		}

	want := legacyRowRef(&m, "|")
	got := string(m.AppendCSVRow(nil, "|"))
	assert.Equal(t, want, got)
	assert.Contains(t, got, "CUSTOM")
}

// TestAppendCSVRowIPAndTimeNoAlloc is the optimization contract: with the
// built-in IP/time renderers the row must be built without an intermediate
// string per column.
func TestAppendCSVRowIPAndTimeNoAlloc(t *testing.T) {
	var m ProtoProducerMessage
	csvIPTimeFixture(t, &m)

	buf := make([]byte, 0, 512)
	m.AppendCSVRow(buf[:0], "|") // warm pools

	allocs := testing.AllocsPerRun(200, func() {
		buf = m.AppendCSVRow(buf[:0], "|")
	})
	assert.Less(t, allocs, 3.0,
		"IP/time append path must not allocate per column, got %v allocs/run", allocs)
	assert.Equal(t, legacyRowRef(&m, "|"), string(buf))
}
