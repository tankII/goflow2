package csv

import (
	"testing"

	"github.com/netsampler/goflow2/v3/format"
	"github.com/stretchr/testify/assert"
)

func TestCSVEscape(t *testing.T) {
	assert.Equal(t, "abc", csvEscape("abc", "|"))
	assert.Equal(t, "\"a|b\"", csvEscape("a|b", "|"))
	assert.Equal(t, "\"a\"\"b\"", csvEscape("a\"b", "|"))
	assert.Equal(t, "\"a\nb\"", csvEscape("a\nb", "|"))
	assert.Equal(t, "\" leading\"", csvEscape(" leading", "|"))
	assert.Equal(t, "\"trailing \"", csvEscape("trailing ", "|"))
	assert.Equal(t, "a, b", csvEscape("a, b", "|"))
}

type testRow struct {
	values []string
}

func (t *testRow) Key() []byte {
	return []byte("k")
}

func (t *testRow) CSVValues() []string {
	return t.values
}

// appendRow implements the optional append interface so the driver exercises
// its fast path.
type appendRow struct {
	values []string
}

func (t *appendRow) Key() []byte { return []byte("k") }

func (t *appendRow) CSVValues() []string { return t.values }

func (t *appendRow) AppendCSVRow(dst []byte, delim string) []byte {
	for i, v := range t.values {
		if i > 0 {
			dst = append(dst, delim...)
		}
		// AppendCSVRow owns escaping, exactly like the real implementation.
		dst = append(dst, csvEscape(v, delim)...)
	}
	return dst
}

// TestFormatAppendPath asserts the driver's append fast path and its legacy
// CSVValues path produce identical bytes.
func TestFormatAppendPath(t *testing.T) {
	d := &CSVDriver{delimiter: "|"}
	values := []string{"1.2.3.4", "443", "a|b", "c\"d"}

	_, legacy, err := d.Format(&testRow{values: values})
	assert.Nil(t, err)
	_, fast, err := d.Format(&appendRow{values: values})
	assert.Nil(t, err)

	assert.Equal(t, string(legacy), string(fast))
}

// TestFormatAppendPathReuse guards that recycling the scratch buffer across calls
// does not leak the previous row.
func TestFormatAppendPathReuse(t *testing.T) {
	d := &CSVDriver{delimiter: "|"}
	_, first, err := d.Format(&appendRow{values: []string{"aaaaaaaa", "bbbb"}})
	assert.Nil(t, err)
	_, second, err := d.Format(&appendRow{values: []string{"c", "d"}})
	assert.Nil(t, err)

	assert.Equal(t, "aaaaaaaa|bbbb", string(first))
	assert.Equal(t, "c|d", string(second))
}

func TestFormatRow(t *testing.T) {
	d := &CSVDriver{delimiter: "|"}
	key, out, err := d.Format(&testRow{values: []string{"1.2.3.4", "443", "a|b", "c\"d"}})
	assert.Nil(t, err)
	assert.Equal(t, []byte("k"), key)
	assert.Equal(t, "1.2.3.4|443|\"a|b\"|\"c\"\"d\"", string(out))

	_, _, err = d.Format("not a row")
	if assert.NotNil(t, err) {
		assert.ErrorIs(t, err, format.ErrNoSerializer)
	}
}
