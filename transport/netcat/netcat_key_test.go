package netcat

import "testing"

// TestNetcatDoesNotNeedKey netcat 按轮询把消息发往各目标，不按 key 分区。
func TestNetcatDoesNotNeedKey(t *testing.T) {
	if (&NetcatDriver{}).NeedsKey() {
		t.Error("netcat 不应需要消息 key")
	}
}
