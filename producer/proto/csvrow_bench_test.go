package protoproducer

import (
	"strings"
	"sync"
	"testing"
)

// benchCSVMessage builds a message shaped like the production formatter:
// 14 columns mixing IPs, ports, counters, a constant column and a timestamp.
func benchCSVMessage(b *testing.B) *ProtoProducerMessage {
	b.Helper()

	m := &ProtoProducerMessage{}
	m.formatter = &FormatterConfigMapper{
		fields: []string{
			"TimeFlowStartNs", "TimeFlowEndNs", "Proto",
			"SrcAddr", "SrcPort", "DstAddr", "DstPort",
			"SrcAs", "DstAs", "Packets", "Bytes", "Flows", "IpTos",
			"SamplerAddress",
		},
		literal: map[string]string{"Flows": "1"},
		render: map[string]RenderFunc{
			"SrcAddr":        IPRenderer,
			"DstAddr":        IPRenderer,
			"SamplerAddress": IPRenderer,
		},
	}
	m.TimeFlowStartNs = 1705732882176 * 1e6
	m.TimeFlowEndNs = 1705732882192 * 1e6
	m.Proto = 6
	m.SrcAddr = []byte{10, 12, 34, 56}
	m.DstAddr = []byte{192, 168, 1, 1}
	m.SrcPort = 54321
	m.DstPort = 443
	m.SrcAs = 65001
	m.DstAs = 65002
	m.Packets = 17
	m.Bytes = 4096
	m.IpTos = 0
	m.SamplerAddress = []byte{10, 0, 0, 7}

	return m
}

const benchCSVDelimiter = "|"

// BenchmarkCSVRowAppend measures the append-based row builder.
func BenchmarkCSVRowAppend(b *testing.B) {
	m := benchCSVMessage(b)
	buf := make([]byte, 0, 1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = m.AppendCSVRow(buf[:0], benchCSVDelimiter)
	}
}

// BenchmarkCSVRowLegacy measures the previous CSVValues + escape + Join pipeline.
func BenchmarkCSVRowLegacy(b *testing.B) {
	m := benchCSVMessage(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values := m.CSVValues()
		row := make([]string, len(values))
		for i, v := range values {
			row[i] = csvEscapeRef(v, benchCSVDelimiter)
		}
		_ = []byte(strings.Join(row, benchCSVDelimiter))
	}
}

// BenchmarkCSVRowAppendCopy measures the full driver cost: append into a pooled
// scratch buffer plus the single copy the transport requires.
func BenchmarkCSVRowAppendCopy(b *testing.B) {
	m := benchCSVMessage(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bp := csvScratchPoolForBench.Get().(*[]byte)
		row := m.AppendCSVRow((*bp)[:0], benchCSVDelimiter)
		*bp = row
		out := make([]byte, len(row))
		copy(out, row)
		csvScratchPoolForBench.Put(bp)
		_ = out
	}
}

// csvScratchPoolForBench mirrors the driver's scratch pool so the benchmark can
// measure the full per-call cost including the copy the transport requires.
var csvScratchPoolForBench = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}
