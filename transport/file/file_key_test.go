package file

import "testing"

// TestFileDoesNotNeedKey file 直接落盘，不使用消息 key。
func TestFileDoesNotNeedKey(t *testing.T) {
	if (&FileDriver{}).NeedsKey() {
		t.Error("file 不应需要消息 key")
	}
}
