// Package text implements text output formatting.
package text

import (
	"encoding"
	"fmt"

	"github.com/netsampler/goflow2/v3/format"
)

// TextDriver formats flow messages via text or string serializers.
type TextDriver struct{}

// Prepare performs any one-time setup for the driver.
func (d *TextDriver) Prepare() error {
	return nil
}

// Init finalizes runtime configuration for the driver.
func (d *TextDriver) Init() error {
	return nil
}

// Format marshals the payload via TextMarshaler or Stringer, preserving a Key when available.
func (d *TextDriver) Format(data interface{}) ([]byte, []byte, error) {
	// key 由调用方按需提供：消息 key 的计算需要反射 + 格式化，
	// 而 netcat/file 并不使用 key（只有 Kafka 用它做分区），
	// 无条件计算在高包率下是可观的无用开销。此处一律返回 nil，
	// 由 utils.flowpipe 在传输确实需要时再补算。
	var key []byte
	if dataIf, ok := data.(encoding.TextMarshaler); ok {
		text, err := dataIf.MarshalText()
		if err != nil {
			return key, nil, fmt.Errorf("text format: %w", err)
		}
		return key, text, nil
	}
	if dataIf, ok := data.(interface{ String() string }); ok {
		return key, []byte(dataIf.String()), nil
	}
	return key, nil, format.ErrNoSerializer
}

func init() {
	d := &TextDriver{}
	format.RegisterFormatDriver("text", d)
}
