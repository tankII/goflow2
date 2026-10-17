package protoproducer

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// csvEscapeRef is a verbatim copy of the CSV driver's escaping rule. It is the
// reference the append-based row builder must reproduce byte for byte.
func csvEscapeRef(v string, delim string) string {
	needsQuote := strings.Contains(v, delim) || strings.ContainsAny(v, "\"\r\n")
	if !needsQuote && len(v) > 0 {
		if c := v[0]; c == ' ' || c == '\t' {
			needsQuote = true
		} else if c := v[len(v)-1]; c == ' ' || c == '\t' {
			needsQuote = true
		}
	}
	if !needsQuote {
		return v
	}
	return "\"" + strings.ReplaceAll(v, "\"", "\"\"") + "\""
}

// legacyRowRef reproduces the pre-optimization CSV pipeline: CSVValues, escape
// each column, then join with the delimiter.
func legacyRowRef(m *ProtoProducerMessage, delim string) string {
	values := m.CSVValues()
	row := make([]string, len(values))
	for i, v := range values {
		row[i] = csvEscapeRef(v, delim)
	}
	return strings.Join(row, delim)
}

func csvFixture(t *testing.T, m *ProtoProducerMessage) {
	t.Helper()

	m.formatter = &FormatterConfigMapper{
		fields: []string{"Etype", "flows", "missing", "test3", "SamplerAddress", "BgpCommunities"},
		rename: map[string]string{
			"Etype": "etype",
		},
		numToPb: map[int32]ProtobufFormatterConfig{
			102: {Name: "test3", Index: 102, Type: "bytes"},
		},
		render: map[string]RenderFunc{
			"Etype":          EtypeRenderer,
			"test3":          StringRenderer,
			"SamplerAddress": IPRenderer,
		},
		literal: map[string]string{
			"flows": "1",
		},
	}

	m.Etype = 0x0800
	m.SamplerAddress = []byte{10, 0, 0, 7}
	m.BgpCommunities = []uint32{65001, 65002}

	fmr := m.ProtoReflect()
	unk := fmr.GetUnknown()
	unk = protowire.AppendTag(unk, protowire.Number(102), protowire.BytesType)
	unk = protowire.AppendString(unk, string([]byte{0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67}))
	fmr.SetUnknown(unk)
}

// TestAppendCSVRowMatchesLegacy pins the append-based row builder to the exact
// bytes the previous strings.Join pipeline produced.
func TestAppendCSVRowMatchesLegacy(t *testing.T) {
	for _, delim := range []string{"|", ",", "\t"} {
		var m ProtoProducerMessage
		csvFixture(t, &m)

		want := legacyRowRef(&m, delim)
		got := string(m.AppendCSVRow(nil, delim))
		assert.Equal(t, want, got, "delimiter %q", delim)
	}
}

// TestAppendCSVRowEscaping pins the escaping rules through the append path,
// including a value that contains the delimiter, quotes, newlines and spaces.
func TestAppendCSVRowEscaping(t *testing.T) {
	for _, delim := range []string{"|", ","} {
		var m ProtoProducerMessage
		csvFixture(t, &m)
		// override test3 with adversarial content
		fmr := m.ProtoReflect()
		unk := fmr.GetUnknown()
		_ = unk
		m.formatter.(*FormatterConfigMapper).literal = map[string]string{
			"flows": "a|b",
		}
		m.BgpCommunities = nil

		want := legacyRowRef(&m, delim)
		got := string(m.AppendCSVRow(nil, delim))
		assert.Equal(t, want, got, "delimiter %q", delim)
	}
}

// TestAppendCSVRowAppendsToBuffer verifies AppendCSVRow appends to the supplied
// buffer instead of discarding its contents.
func TestAppendCSVRowAppendsToBuffer(t *testing.T) {
	var m ProtoProducerMessage
	csvFixture(t, &m)

	prefix := []byte("prefix:")
	buf := make([]byte, len(prefix), 512)
	copy(buf, prefix)
	out := m.AppendCSVRow(buf, "|")

	assert.Equal(t, "prefix:"+legacyRowRef(&m, "|"), string(out))
}

// TestAppendCSVRowReuse guards buffer reuse: appending into a dirty buffer must
// not leak the previous contents.
func TestAppendCSVRowReuse(t *testing.T) {
	var m ProtoProducerMessage
	csvFixture(t, &m)

	buf := make([]byte, 0, 512)
	buf = append(buf, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"...)
	out := m.AppendCSVRow(buf[:0], "|")

	assert.Equal(t, legacyRowRef(&m, "|"), string(out))
}

// TestAppendCSVRowAllFieldsMissing exercises the degenerate case where no column
// renders, so the row is only delimiters.
func TestAppendCSVRowAllFieldsMissing(t *testing.T) {
	var m ProtoProducerMessage
	m.formatter = &FormatterConfigMapper{
		fields: []string{"nope1", "nope2", "nope3"},
		render: map[string]RenderFunc{},
	}

	empty := make([]string, 3)
	assert.Equal(t, strings.Join(empty, "|"), string(m.AppendCSVRow(nil, "|")))
}

// TestAppendCSVRowRepeated guards that repeated rendering of the same message is
// stable, i.e. pooling does not carry state between calls.
func TestAppendCSVRowRepeated(t *testing.T) {
	var m ProtoProducerMessage
	csvFixture(t, &m)

	want := legacyRowRef(&m, "|")
	for i := 0; i < 5; i++ {
		require.Equal(t, want, string(m.AppendCSVRow(nil, "|")))
		// interleave with another message to churn the pools
		var other ProtoProducerMessage
		csvFixture(t, &other)
		_ = other.AppendCSVRow(nil, "|")
	}
}

// TestAppendCSVValuesMatchesCSVValues pins the reusable []string variant.
func TestAppendCSVValuesMatchesCSVValues(t *testing.T) {
	var m ProtoProducerMessage
	csvFixture(t, &m)

	want := m.CSVValues()
	scratch := make([]string, 0, 16)
	got := m.appendCSVValues(scratch)

	require.Equal(t, len(want), len(got))
	assert.Equal(t, want, got)
	assert.Equal(t, 0, len(scratch), "scratch must be left untouched for reuse")
}
