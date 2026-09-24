package netcat

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// listenCollect starts a TCP listener that collects every line received on the
// first accepted connection and publishes it when that connection closes.
func listenCollect(t *testing.T) (string, <-chan []string) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	got := make(chan []string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			got <- nil
			return
		}
		defer conn.Close()

		var lines []string
		sc := bufio.NewScanner(conn)
		for sc.Scan() {
			lines = append(lines, sc.Text())
		}
		got <- lines
	}()

	return ln.Addr().String(), got
}

// collectConn scans the first accepted connection on ln and publishes the
// collected lines once that connection is closed.
func collectConn(ln net.Listener) (<-chan []string, error) {
	got := make(chan []string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			got <- nil
			return
		}
		defer conn.Close()
		defer ln.Close()

		var lines []string
		sc := bufio.NewScanner(conn)
		for sc.Scan() {
			lines = append(lines, sc.Text())
		}
		got <- lines
	}()
	return got, nil
}

func newDriver(queueSize int, drop bool, addrs ...string) *NetcatDriver {
	d := &NetcatDriver{
		dialTimeout: time.Second,
		reconnect:   time.Millisecond * 20,
		sep:         []byte("\n"),
		queueSize:   queueSize,
		drop:        drop,
	}
	for _, addr := range addrs {
		t := &ncTarget{
			addr:  addr,
			queue: make(chan []byte, queueSize),
			done:  make(chan struct{}),
		}
		d.targets = append(d.targets, t)
		t.wg.Add(1)
		go d.writer(t)
	}
	return d
}

// waitReady waits until every target has an established connection and its
// queue has been drained (queueEmpty=true), or fails the test on timeout.
func waitReady(t *testing.T, d *NetcatDriver, queueEmpty bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, tg := range d.targets {
			if !tg.connected.Load() {
				ready = false
				break
			}
			if queueEmpty && len(tg.queue) != 0 {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("netcat: target did not become ready in time")
}

func TestRoundRobinDistribution(t *testing.T) {
	addr1, got1 := listenCollect(t)
	addr2, got2 := listenCollect(t)

	d := newDriver(1024, true, addr1, addr2)
	waitReady(t, d, true)

	const n = 20
	for i := 0; i < n; i++ {
		assert.Nil(t, d.Send(nil, []byte("line-"+string(rune('0'+i%10))+"-"+string(rune('0'+i/10)))))
	}
	assert.Nil(t, d.Close())

	lines1 := <-got1
	lines2 := <-got2

	assert.Equal(t, n, len(lines1)+len(lines2), "every line must reach exactly one target")

	seen := map[string]bool{}
	for _, l := range lines1 {
		assert.False(t, seen[l], "line %q must not be duplicated", l)
		seen[l] = true
	}
	for _, l := range lines2 {
		assert.False(t, seen[l], "line %q must not be duplicated", l)
		seen[l] = true
	}
	assert.Equal(t, n, len(seen))
}

func TestReconnectWhileDisconnected(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	addr := ln.Addr().String()
	_ = ln.Close()

	d := newDriver(2, true, addr)

	// target is down: lines accumulate in the bounded queue
	assert.Nil(t, d.Send(nil, []byte("buffered-a")))
	assert.Nil(t, d.Send(nil, []byte("buffered-b")))

	// re-bind the same address and let the writer reconnect and drain
	ln2, err := net.Listen("tcp", addr)
	assert.Nil(t, err)

	linesCh, err := collectConn(ln2)
	assert.Nil(t, err)
	waitReady(t, d, true)

	assert.Nil(t, d.Send(nil, []byte("new-c")))
	assert.Nil(t, d.Close())

	lines := <-linesCh
	assert.Equal(t, []string{"buffered-a", "buffered-b", "new-c"}, lines)
}

func TestDropWhenQueueFull(t *testing.T) {
	addr := "127.0.0.1:0"

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	addr = ln.Addr().String()
	_ = ln.Close()

	d := newDriver(2, true, addr)
	defer d.Close()

	for i := 0; i < 20; i++ {
		assert.Nil(t, d.Send(nil, []byte("x")))
	}
	assert.Equal(t, uint64(18), d.Dropped(addr))
}
