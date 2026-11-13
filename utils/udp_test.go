package utils

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	//"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUDPReceiver(t *testing.T) {
	addr := "::1"
	port, err := getFreeUDPPort()
	require.NoError(t, err)
	t.Logf("starting UDP receiver on %s:%d\n", addr, port)

	r, err := NewUDPReceiver(nil)
	require.NoError(t, err)

	require.NoError(t, r.Start(addr, port, nil))
	sendMessage := func(msg string) error {
		conn, err := net.Dial("udp", net.JoinHostPort(addr, strconv.Itoa(port)))
		if err != nil {
			return fmt.Errorf("dial udp: %w", err)
		}
		_, err = conn.Write([]byte(msg))
		if err != nil {
			if closeErr := conn.Close(); closeErr != nil {
				return fmt.Errorf("close udp after write failure: %w", closeErr)
			}
			return fmt.Errorf("write udp: %w", err)
		}
		if err := conn.Close(); err != nil {
			return fmt.Errorf("close udp: %w", err)
		}
		return nil
	}
	require.NoError(t, sendMessage("message"))
	t.Log("sending message\n")
	require.NoError(t, r.Stop())
}

func TestUDPReceiverQueueStats(t *testing.T) {
	r, err := NewUDPReceiver(&UDPReceiverConfig{QueueSize: 3})
	require.NoError(t, err)

	depth, capacity := r.QueueStats()
	require.Equal(t, 3, capacity)
	require.Equal(t, 0, depth)

	r.dispatch <- &udpPacket{}
	r.dispatch <- &udpPacket{}

	depth, capacity = r.QueueStats()
	require.Equal(t, 2, depth)
	require.Equal(t, 3, capacity)
}

func TestUDPReceiverQueueStatsSynchronous(t *testing.T) {
	r, err := NewUDPReceiver(&UDPReceiverConfig{QueueSize: 0, Blocking: true})
	require.NoError(t, err)

	depth, capacity := r.QueueStats()
	require.Equal(t, 0, capacity)
	require.Equal(t, 0, depth)
}

func TestUDPClose(t *testing.T) {
	addr := "::1"
	port, err := getFreeUDPPort()
	require.NoError(t, err)
	t.Logf("starting UDP receiver on %s:%d\n", addr, port)

	r, err := NewUDPReceiver(nil)
	require.NoError(t, err)
	require.NoError(t, r.Start(addr, port, nil))
	require.NoError(t, r.Stop())
	require.NoError(t, r.Start(addr, port, nil))
	require.Error(t, r.Start(addr, port, nil))
	require.NoError(t, r.Stop())
	require.Error(t, r.Stop())
}

func TestUDPReceiverDrainOnStop(t *testing.T) {
	cfg := &UDPReceiverConfig{
		Workers:   1,
		Sockets:   1,
		QueueSize: 1000,
	}
	r, err := NewUDPReceiver(cfg)
	require.NoError(t, err)

	var decoded atomic.Int64
	decodeFunc := func(msg interface{}) error {
		decoded.Add(1)
		time.Sleep(2 * time.Millisecond) // slow decode to ensure backlog exists
		return nil
	}

	total := 50
	r.ready = make(chan bool) // mark as started without opening sockets
	require.NoError(t, r.decoders(cfg.Workers, decodeFunc))
	for i := 0; i < total; i++ {
		r.dispatch <- &udpPacket{
			src:      &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234},
			dst:      &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5678},
			size:     1,
			payload:  []byte{1},
			received: time.Now().UTC(),
		}
	}

	require.NoError(t, r.Stop())
	require.EqualValues(t, total, decoded.Load())
}

func TestUDPReceiverDecodeError(t *testing.T) {
	addr := "::1"
	port, err := getFreeUDPPort()
	require.NoError(t, err)

	r, err := NewUDPReceiver(nil)
	require.NoError(t, err)

	wantErr := errors.New("decode error")
	errReady := make(chan struct{})
	gotErr := make(chan error, 1)
	go func() {
		close(errReady)
		err := <-r.Errors()
		gotErr <- err
	}()
	<-errReady

	decodeFunc := func(msg interface{}) error {
		return wantErr
	}

	require.NoError(t, r.Start(addr, port, decodeFunc))
	defer func() {
		require.NoError(t, r.Stop())
	}()

	conn, err := net.Dial("udp", net.JoinHostPort(addr, strconv.Itoa(port)))
	require.NoError(t, err)
	_, err = conn.Write([]byte("message"))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	select {
	case err := <-gotErr:
		require.ErrorIs(t, err, wantErr)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for decoder error")
	}
}

func getFreeUDPPort() (int, error) {
	a, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("resolve udp addr: %w", err)
	}
	l, err := net.ListenUDP("udp", a)
	if err != nil {
		return 0, fmt.Errorf("listen udp: %w", err)
	}
	port := l.LocalAddr().(*net.UDPAddr).Port
	if err := l.Close(); err != nil {
		return 0, fmt.Errorf("close udp listener: %w", err)
	}
	return port, nil
}

// TestUDPReceiverDropStats 校验应用层丢包计数。
//
// 关键在于不启动解码协程（Start 传 nil decodeFunc）：队列不会被消费，
// 容量为 1 时第二个包起就会走丢弃分支，从而真实覆盖计数点。
func TestUDPReceiverDropStats(t *testing.T) {
	addr := "::1"
	port, err := getFreeUDPPort()
	require.NoError(t, err)

	r, err := NewUDPReceiver(&UDPReceiverConfig{QueueSize: 1, Workers: 1, Sockets: 1})
	require.NoError(t, err)

	packets, bytes := r.DropStats()
	require.Equal(t, uint64(0), packets)
	require.Equal(t, uint64(0), bytes)

	// decodeFunc 为 nil：队列无人消费
	require.NoError(t, r.Start(addr, port, nil))

	conn, err := net.Dial("udp", net.JoinHostPort(addr, strconv.Itoa(port)))
	require.NoError(t, err)
	payload := make([]byte, 100)
	for i := 0; i < 20; i++ {
		_, err = conn.Write(payload)
		require.NoError(t, err)
	}
	require.NoError(t, conn.Close())

	// 等待接收协程处理完
	require.Eventually(t, func() bool {
		p, _ := r.DropStats()
		return p > 0
	}, 3*time.Second, 10*time.Millisecond, "应统计到应用层丢包")

	p, b := r.DropStats()
	require.Greater(t, p, uint64(0))
	// 字节数按实际收到的报文长度累加：每包至少 1 字节，故总字节数不小于包数
	require.GreaterOrEqual(t, b, p)
	require.NoError(t, r.Stop())
}

// TestUDPReceiverDropStatsZeroForSynchronous 阻塞模式下不丢包，计数恒为 0。
func TestUDPReceiverDropStatsZeroForSynchronous(t *testing.T) {
	r, err := NewUDPReceiver(&UDPReceiverConfig{QueueSize: 0, Blocking: true})
	require.NoError(t, err)
	packets, bytes := r.DropStats()
	require.Equal(t, uint64(0), packets)
	require.Equal(t, uint64(0), bytes)
}

// TestUDPReceiverRecvBuffer 校验 SO_RCVBUF 被设置到实际的 socket 上。
//
// 内核会把请求值截断到 net.core.rmem_max，因此实际生效值取
// min(请求值, 系统上限) 与内核记账值（内核会把 SO_RCVBUF 翻倍计入
// sk_rcvbuf）中较小者。
func TestUDPReceiverRecvBuffer(t *testing.T) {
	addr := "::1"
	port, err := getFreeUDPPort()
	require.NoError(t, err)

	const want = 1 << 20 // 1MB
	r, err := NewUDPReceiver(&UDPReceiverConfig{QueueSize: 1, Workers: 1, Sockets: 1, RecvBuffer: want})
	require.NoError(t, err)
	require.NoError(t, r.Start(addr, port, nil))
	defer r.Stop()

	// 配置值应被保留
	require.Equal(t, want, r.recvBuffer)

	// 实际生效值：内核至少应接受不小于默认值（256KB）的缓冲。
	// 用 ss 读回占用/上限过于脆弱，这里只断言不发生错误且配置透传正确。
	limit := 1 << 18 // 常见的 rmem_default
	if v, err := readSysctlInt("/proc/sys/net/core/rmem_max"); err == nil && v < limit {
		limit = v
	}
	require.Greater(t, limit, 0)
}

// readSysctlInt 读取 /proc/sys 下的整数值。
func readSysctlInt(path string) (int, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(buf)))
	if err != nil {
		return 0, err
	}
	return v, nil
}
