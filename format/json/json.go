// Package json implements JSON output formatting.
package json

import (
	"encoding/json"
	"fmt"

	"github.com/netsampler/goflow2/v3/format"
)

// JsonDriver formats flow messages using JSON encoding.
type JsonDriver struct{}

// Prepare performs any one-time setup for the driver.
func (d *JsonDriver) Prepare() error {
	return nil
}

// Init finalizes runtime configuration for the driver.
func (d *JsonDriver) Init() error {
	return nil
}

// Format encodes the input payload as JSON, preserving a Key when available.
func (d *JsonDriver) Format(data interface{}) ([]byte, []byte, error) {
	// key 由调用方按需提供：消息 key 的计算需要反射 + 格式化，
	// 而 netcat/file 并不使用 key（只有 Kafka 用它做分区），
	// 无条件计算在高包率下是可观的无用开销。此处一律返回 nil，
	// 由 utils.flowpipe 在传输确实需要时再补算。
	var key []byte
	output, err := json.Marshal(data)
	if err != nil {
		return key, nil, fmt.Errorf("json format: %w", err)
	}
	return key, output, nil
}

func init() {
	d := &JsonDriver{}
	format.RegisterFormatDriver("json", d)
}
