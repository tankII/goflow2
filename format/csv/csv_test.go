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

func TestFormatRow(t *testing.T) {
	d := &CSVDriver{delimiter: "|"}
	key, out, err := d.Format(&testRow{values: []string{"1.2.3.4", "443", "a|b", "c\"d"}})
	assert.Nil(t, err)
	assert.Equal(t, []byte("k"), key)
	assert.Equal(t, "1.2.3.4|443|\"a|b\"|\"c\"\"d\"\n", string(out))

	_, _, err = d.Format("not a row")
	if assert.NotNil(t, err) {
		assert.ErrorIs(t, err, format.ErrNoSerializer)
	}
}
