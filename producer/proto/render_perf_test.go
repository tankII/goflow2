package protoproducer

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

const perfTestYAML = `
formatter:
  fields:
    - flow_direction
    - time_received_ns
    - sampling_rate
    - bytes
    - packets
    - src_addr
    - dst_addr
    - etype
    - proto
    - src_port
    - dst_port
    - icmp_name
    - csum
    - flows
  literal:
    flows: 1
  render:
    time_received_ns: datetimenano
  key:
    - sampler_address
  protobuf:
    - name: flow_direction
      index: 42
      type: varint
    - name: csum
      index: 999
      type: varint
`

func perfFormatter(tb testing.TB) FormatterMapper {
	tb.Helper()
	var cfg ProducerConfig
	if err := yaml.Unmarshal([]byte(perfTestYAML), &cfg); err != nil {
		tb.Fatal(err)
	}
	mapped, err := cfg.Compile()
	if err != nil {
		tb.Fatal(err)
	}
	return mapped.GetFormatter()
}

func perfMessage(tb testing.TB) *ProtoProducerMessage {
	tb.Helper()
	m := &ProtoProducerMessage{}
	m.formatter = perfFormatter(tb)
	m.TimeReceivedNs = 1705732882176000000
	m.SamplingRate = 1000
	m.Bytes = 1234
	m.Packets = 10
	m.SrcAddr = []byte{10, 0, 0, 1}
	m.DstAddr = []byte{10, 0, 0, 2}
	m.Etype = 0x800
	m.Proto = 6
	m.SrcPort = 1234
	m.DstPort = 80
	m.IcmpType = 8
	m.IcmpCode = 0
	return m
}

func BenchmarkCSVValues(b *testing.B) {
	m := perfMessage(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.CSVValues()
	}
}

func BenchmarkMarshalJSON(b *testing.B) {
	m := perfMessage(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(m)
	}
}

func TestCSVValuesAllocBudget(t *testing.T) {
	m := perfMessage(t)
	_ = m.CSVValues()
	if allocs := testing.AllocsPerRun(200, func() { _ = m.CSVValues() }); allocs > 40 {
		t.Fatalf("CSVValues allocates %.0f objects per render, want <= 40", allocs)
	}
}

func TestMarshalJSONAllocBudget(t *testing.T) {
	m := perfMessage(t)
	_ = m.MarshalJSON
	if allocs := testing.AllocsPerRun(200, func() { _, _ = m.MarshalJSON() }); allocs > 70 {
		t.Fatalf("MarshalJSON allocates %.0f objects per render, want <= 70", allocs)
	}
}

func BenchmarkKey(b *testing.B) {
	m := perfMessage(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Key()
	}
}
