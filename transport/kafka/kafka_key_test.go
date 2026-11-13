package kafka

import "testing"

// TestKafkaNeedsKey Kafka 用 key 作为分区键，保证同 key 落同一分区。
func TestKafkaNeedsKey(t *testing.T) {
	if !(&KafkaDriver{}).NeedsKey() {
		t.Error("kafka 应需要消息 key")
	}
}
