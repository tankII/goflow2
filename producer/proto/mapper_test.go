package protoproducer

import (
	"testing"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNetFlowMapperKeyDistinguishesPenProvided guards the mapper key semantics:
// two config entries that only differ by PenProvided must resolve to distinct
// destinations.
func TestNetFlowMapperKeyDistinguishesPenProvided(t *testing.T) {
	m := mapFieldsNetFlow([]NetFlowMapField{
		{PenProvided: false, Type: 8, Pen: 0, Destination: "SrcAddr", Endian: BigEndian},
		{PenProvided: true, Type: 8, Pen: 0, Destination: "DstAddr", Endian: BigEndian},
	})

	noPen, ok := m.Map(netflow.DataField{PenProvided: false, Type: 8, Pen: 0})
	require.True(t, ok)
	assert.Equal(t, "SrcAddr", noPen.GetDestination())

	withPen, ok := m.Map(netflow.DataField{PenProvided: true, Type: 8, Pen: 0})
	require.True(t, ok)
	assert.Equal(t, "DstAddr", withPen.GetDestination())

	_, ok = m.Map(netflow.DataField{PenProvided: false, Type: 9, Pen: 0})
	assert.False(t, ok)
}

// TestMapFieldsNetFlowResolvesFieldIndex verifies that mapFieldsNetFlow resolves
// the protobuf field index at build time and that MapCustom writes through it.
func TestMapFieldsNetFlowResolvesFieldIndex(t *testing.T) {
	m := mapFieldsNetFlow([]NetFlowMapField{
		{Type: netflow.NFV9_FIELD_IPV4_SRC_ADDR, Destination: "SrcAddr", Endian: BigEndian},
	})

	mapped, ok := m.Map(netflow.DataField{Type: netflow.NFV9_FIELD_IPV4_SRC_ADDR})
	require.True(t, ok)

	dm, ok := mapped.(*DataMap)
	require.True(t, ok)
	assert.NotNil(t, dm.fieldIndex, "field index should be resolved at build time")

	msg := &ProtoProducerMessage{}
	require.NoError(t, MapCustom(msg, []byte{10, 0, 0, 1}, mapped))
	assert.Equal(t, []byte{10, 0, 0, 1}, msg.SrcAddr)
}

// TestMapFieldsNetFlowMarksNonFieldDestinationResolved guards the hot path: a
// compiled mapper must mark its binding as resolved even when the destination is
// not a Go field (e.g. a snake_case manual protobuf field), so MapCustom skips
// reflect.FieldByName on every record.
func TestMapFieldsNetFlowMarksNonFieldDestinationResolved(t *testing.T) {
	m := mapFieldsNetFlow([]NetFlowMapField{
		{Type: 61, Destination: "flow_direction", Endian: BigEndian},
	})

	mapped, ok := m.Map(netflow.DataField{Type: 61})
	require.True(t, ok)

	dm, ok := mapped.(*DataMap)
	require.True(t, ok)
	require.NotNil(t, dm.fieldIndex, "compiled config must be marked resolved")
	assert.Empty(t, dm.fieldIndex, "snake_case destination is not a Go field")
}

// TestMapCustomWithoutResolvedIndex verifies the FieldByName fallback still
// works for a mapper that was not built via mapFieldsNetFlow.
func TestMapCustomWithoutResolvedIndex(t *testing.T) {
	cfg := &DataMap{}
	cfg.Destination = "SrcAddr"
	cfg.Endianness = BigEndian

	msg := &ProtoProducerMessage{}
	require.NoError(t, MapCustom(msg, []byte{192, 168, 0, 1}, cfg))
	assert.Equal(t, []byte{192, 168, 0, 1}, msg.SrcAddr)
}
