package file

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// TestSendConcurrentNoMerge verifies that concurrent Send calls never
// interleave data and separator writes, which would produce merged
// lines or spurious blank lines.
func TestSendConcurrentNoMerge(t *testing.T) {
	path := t.TempDir() + "/concurrent.txt"
	d := &FileDriver{
		fileDestination: path,
		lineSeparator:   "\n",
		lock:            &sync.RWMutex{},
	}
	if err := d.Init(); err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const goroutines = 16
	const perG = 200
	var wg sync.WaitGroup
	var errs atomic.Int32
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if err := d.Send(nil, []byte("row")); err != nil {
					errs.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if errs.Load() > 0 {
		t.Fatal("Send returned errors")
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
	want := goroutines * perG
	if len(lines) != want {
		t.Fatalf("line count = %d, want %d (merged/blank lines)\nfirst merged excerpt:\n%q",
			len(lines), want, string(got[:min(200, len(got))]))
	}
	for _, l := range lines {
		if l != "row" {
			t.Fatalf("corrupted line: %q", l)
		}
	}
}
