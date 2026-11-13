package utils

import (
	"testing"

	"github.com/netsampler/goflow2/v3/format"

	_ "github.com/netsampler/goflow2/v3/format/binary"
	_ "github.com/netsampler/goflow2/v3/format/csv"
	_ "github.com/netsampler/goflow2/v3/format/json"
	_ "github.com/netsampler/goflow2/v3/format/text"
)

// countingMsg 记录 Key() 被调用的次数。
type countingMsg struct {
	keys int
}

func (m *countingMsg) Key() []byte { m.keys++; return []byte("k") }

// CSVValues 供 CSV 驱动序列化。
func (m *countingMsg) CSVValues() []string { return []string{"1"} }

// AppendCSVRow 供 CSV 驱动走追加式渲染路径。
func (m *countingMsg) AppendCSVRow(b []byte, d byte) []byte { return append(b, '1', d) }

// String 供 text 驱动序列化。
func (m *countingMsg) String() string { return "1" }

// MarshalBinary 供 binary 驱动序列化。
func (m *countingMsg) MarshalBinary() ([]byte, error) { return []byte{1}, nil }

// TestFormatDriversDoNotComputeKey 验证格式驱动不再无条件计算消息 key。
//
// key 的计算要走反射 + 格式化，而 netcat/file 并不使用它；由管道层在
// 传输确实需要时补算，避免每条记录都付出这份开销。
func TestFormatDriversDoNotComputeKey(t *testing.T) {
	for _, name := range []string{"csv", "json", "text", "bin"} {
		f, err := format.FindFormat(name)
		if err != nil {
			t.Fatalf("找不到格式 %s: %v", name, err)
		}
		msg := &countingMsg{}
		_, _, err = f.Format(msg)
		if err != nil {
			t.Fatalf("%s 格式化失败: %v", name, err)
		}
		if msg.keys != 0 {
			t.Errorf("%s 格式不应触发 Key()，实际调用 %d 次", name, msg.keys)
		}
	}
}
