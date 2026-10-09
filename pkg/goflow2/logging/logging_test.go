package logging

import (
	"bytes"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func pipeStderr(t *testing.T) func() []byte {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w

	var mu sync.Mutex
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		tmp := make([]byte, 32*1024)
		for {
			n, err := r.Read(tmp)
			if n > 0 {
				mu.Lock()
				buf.Write(tmp[:n])
				mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()

	return func() []byte {
		os.Stderr = old
		_ = w.Close()
		<-done
		_ = r.Close()
		mu.Lock()
		defer mu.Unlock()
		return append([]byte(nil), buf.Bytes()...)
	}
}

func TestNewLoggerInvalidLevel(t *testing.T) {
	_, err := NewLogger(Options{Level: "verbose"})
	if err == nil {
		t.Fatal("expected error for invalid level")
	}
}

func TestNewLoggerStderrDefault(t *testing.T) {
	finish := pipeStderr(t)
	defer finish()

	logger, err := NewLogger(Options{Level: "info"})
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	logger.Info("hello")
	logger.Debug("hidden")

	out := finish()
	if !bytes.Contains(out, []byte("hello")) {
		t.Fatalf("expected info message in output, got %q", out)
	}
	if bytes.Contains(out, []byte("hidden")) {
		t.Fatalf("debug message should be filtered at level=info, got %q", out)
	}
}

func TestNewLoggerJSONHandler(t *testing.T) {
	finish := pipeStderr(t)
	defer finish()

	logger, err := NewLogger(Options{Level: "debug", Format: "json"})
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	logger.Info("hello")

	out := finish()
	lines := strings.FieldsFunc(string(out), func(r rune) bool { return r == '\n' })
	if len(lines) == 0 {
		t.Fatal("expected at least one line")
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("expected a JSON line, got %q: %v", lines[0], err)
	}
	if rec["msg"] != "hello" {
		t.Fatalf("expected msg=hello, got %v", rec["msg"])
	}
}

func TestNewLoggerAddSource(t *testing.T) {
	finish := pipeStderr(t)
	defer finish()

	logger, err := NewLogger(Options{Level: "debug", AddSource: true})
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	logger.Info("hello")

	out := finish()
	if !bytes.Contains(out, []byte("source=")) {
		t.Fatalf("expected source= attr with AddSource, got %q", out)
	}
}

func TestNewLoggerFileWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	finish := pipeStderr(t)

	logger, err := NewLogger(Options{
		Level:        "info",
		Format:       "json",
		File:         path,
		MaxSize:      1,
		MaxBackups:   3,
		MaxAge:       365,
		Compress:     true,
		AlsoToStderr: true,
	})
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	pad := strings.Repeat("x", 4096)
	for i := 0; i < 500; i++ {
		logger.Info("message", slog.Int("i", i), slog.String("pad", pad))
	}
	finish()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read app.log: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected app.log to contain log lines")
	}
	first := strings.SplitN(string(data), "\n", 2)[0]
	var rec map[string]any
	if err := json.Unmarshal([]byte(first), &rec); err != nil {
		t.Fatalf("expected a JSON line in file, got %q: %v", first, err)
	}

	if !bytes.Contains(data, []byte("message")) {
		t.Fatal("expected message content in log file")
	}

	stderrData := finish()
	if !bytes.Contains(stderrData, []byte("message")) {
		t.Fatal("expected also-stderr output to contain message")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		var allGz bool
		var backups []string
		for _, e := range entries {
			if e.Name() != "app.log" {
				backups = append(backups, e.Name())
			}
		}
		allGz = len(backups) > 0
		for _, b := range backups {
			if !strings.HasSuffix(b, ".gz") {
				allGz = false
			}
		}
		if allGz || time.Now().After(deadline) {
			if len(backups) == 0 {
				t.Fatal("expected at least one rotated backup after writing >1MB")
			}
			for _, b := range backups {
				if !strings.HasSuffix(b, ".gz") {
					t.Fatalf("compressed backups should end .gz after mill settles, got %q", b)
				}
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
		entries, err = os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read dir: %v", err)
		}
	}
}

func TestNewLoggerFileNoSizeLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	logger, err := NewLogger(Options{Level: "info", File: path, MaxSize: 0})
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	for i := 0; i < 100; i++ {
		logger.Info("small", slog.Int("i", i))
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "app.log" {
		t.Fatalf("MaxSize=0 must disable size rotation; found %d files: %v", len(entries), entries)
	}
}

func TestNewLoggerConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	logger, err := NewLogger(Options{Level: "info", File: path, MaxSize: 1, MaxBackups: 3, MaxAge: 365, Compress: true})
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			pad := strings.Repeat("x", 1024)
			for i := 0; i < 200; i++ {
				logger.Info("concurrent", slog.Int("worker", worker), slog.Int("i", i), slog.String("pad", pad))
			}
		}(g)
	}
	wg.Wait()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatalf("stat %s: %v", e.Name(), err)
		}
		total += info.Size()
	}
	if total < 1024*1024 {
		t.Fatalf("expected >1MB retained across log files under concurrency, got %d bytes", total)
	}
}

func TestRegisterFlagsDefaults(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts := &Options{}
	RegisterFlags(fs, opts)
	if err := fs.Parse([]string{}); err != nil {
		t.Fatalf("parse: %v", err)
	}

	if opts.Level != "info" {
		t.Errorf("Level default = %q, want info", opts.Level)
	}
	if opts.Format != "normal" {
		t.Errorf("Format default = %q, want normal", opts.Format)
	}
	if opts.File != "" {
		t.Errorf("File default = %q, want empty", opts.File)
	}
	if opts.MaxSize != 100 {
		t.Errorf("MaxSize default = %d, want 100", opts.MaxSize)
	}
	if opts.MaxBackups != 3 {
		t.Errorf("MaxBackups default = %d, want 3", opts.MaxBackups)
	}
	if opts.MaxAge != 28 {
		t.Errorf("MaxAge default = %d, want 28", opts.MaxAge)
	}
	if opts.Compress != true {
		t.Errorf("Compress default = %v, want true", opts.Compress)
	}
	if opts.AlsoToStderr != false {
		t.Errorf("AlsoToStderr default = %v, want false", opts.AlsoToStderr)
	}
	if opts.AddSource != false {
		t.Errorf("AddSource default = %v, want false", opts.AddSource)
	}
	if opts.HTTP != false {
		t.Errorf("HTTP default = %v, want false", opts.HTTP)
	}
}
