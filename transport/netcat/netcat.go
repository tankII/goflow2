// Package netcat implements a line-based TCP transport with round-robin
// distribution across multiple targets.
package netcat

import (
	"bufio"
	"flag"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/netsampler/goflow2/v3/transport"
)

// ncTarget holds the connection state and queue for a single remote peer.
type ncTarget struct {
	addr string

	queue chan []byte
	done  chan struct{}
	wg    sync.WaitGroup

	connected atomic.Bool
	dropped   atomic.Uint64
}

// NetcatDriver sends formatted lines over TCP, rotating targets round-robin.
type NetcatDriver struct {
	targetsCfg string
	newline    string

	dialTimeout time.Duration
	reconnect   time.Duration
	queueSize   int
	drop        bool

	// flushInterval 限制一条记录在写缓冲中最多滞留多久就交给内核；
	// writeBufferSize 是合并多条记录成一次 socket 写的窗口大小。
	flushInterval   time.Duration
	writeBufferSize int

	sep     []byte
	targets []*ncTarget
	rr      atomic.Uint64
}

// Prepare binds the driver flags.
func (d *NetcatDriver) Prepare() error {
	flag.StringVar(&d.targetsCfg, "transport.netcat.targets", "", "Remote targets as a comma-separated list of host:port, rotated round-robin")
	flag.StringVar(&d.newline, "transport.netcat.sep", "\n", "Line separator appended to each record")
	flag.DurationVar(&d.dialTimeout, "transport.netcat.dial.timeout", time.Second*3, "Dial timeout for remote targets")
	flag.DurationVar(&d.reconnect, "transport.netcat.reconnect.interval", time.Second*3, "Reconnect interval after a connection failure")
	flag.IntVar(&d.queueSize, "transport.netcat.queue", 1024, "Per-target buffered queue size")
	flag.BoolVar(&d.drop, "transport.netcat.drop", true, "Drop new records when the target queue is full (false blocks Send)")
	flag.DurationVar(&d.flushInterval, "transport.netcat.flush.interval", 200*time.Millisecond, "Maximum delay before a buffered record is written to the socket (records are coalesced into fewer writes)")
	flag.IntVar(&d.writeBufferSize, "transport.netcat.write.buffer", 64*1024, "Per-target write buffer size used to coalesce records into fewer socket writes")

	return nil
}

// writeBufferSizeOrDefault 返回配置的写缓冲大小；若驱动器未经 Prepare 构建
// （测试、库直接使用），回退为 64KiB。
func (d *NetcatDriver) writeBufferSizeOrDefault() int {
	if d.writeBufferSize > 0 {
		return d.writeBufferSize
	}
	return 64 * 1024
}

// flushIntervalOrDefault 返回配置的 flush 间隔；若驱动器未经 Prepare 构建，
// 回退为 200ms。
func (d *NetcatDriver) flushIntervalOrDefault() time.Duration {
	if d.flushInterval > 0 {
		return d.flushInterval
	}
	return 200 * time.Millisecond
}

// Init parses the target list and starts the per-target writer goroutines.
func (d *NetcatDriver) Init() error {
	d.sep = []byte(d.newline)
	for _, addr := range strings.Split(d.targetsCfg, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		t := &ncTarget{
			addr:  addr,
			queue: make(chan []byte, d.queueSize),
			done:  make(chan struct{}),
		}
		d.targets = append(d.targets, t)
		t.wg.Add(1)
		go d.writer(t)
	}
	return nil
}

// writer keeps one target connected and drains its queue.
func (d *NetcatDriver) writer(t *ncTarget) {
	defer t.wg.Done()

	for {
		if d.connIsStopped(t) {
			return
		}

		conn, err := net.DialTimeout("tcp", t.addr, d.dialTimeout)
		if err == nil {
			slog.Debug("netcat: connected", slog.String("target", t.addr))
			t.connected.Store(true)
			err = d.serve(t, conn)
			t.connected.Store(false)
			ioClose(conn)
			slog.Debug("netcat: disconnected", slog.String("target", t.addr))
		}
		if err != nil {
			slog.Debug("netcat: connect error", slog.String("target", t.addr), slog.String("err", err.Error()))
		}

		select {
		case <-time.After(d.reconnect):
		case <-t.done:
			return
		}
	}
}

// serve 把排队的记录写入已建立的连接，直到出错或停止。
//
// 记录先累积进 bufio.Writer，仅在"缓冲写满"或"经过 flushInterval"时才 Flush，
// 于是每条记录一次 write 系统的调用被合并为每 64KiB 一次（现网 profile 中
// Syscall6 曾占 12.2% CPU）。缓冲写满时 bufio 会自动下发给内核，故无需额外 Flush。
func (d *NetcatDriver) serve(t *ncTarget, conn net.Conn) error {
	w := bufio.NewWriterSize(conn, d.writeBufferSizeOrDefault())
	interval := d.flushIntervalOrDefault()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	drain := func() error {
		for {
			select {
			case data := <-t.queue:
				if err := d.writeLine(w, data); err != nil {
					return err
				}
			default:
				return w.Flush()
			}
		}
	}

	for {
		select {
		case data := <-t.queue:
			if err := d.writeLine(w, data); err != nil {
				return err
			}
			// A full buffer has already handed its content to the kernel.
			if w.Buffered() == 0 {
				continue
			}
		case <-ticker.C:
			if err := w.Flush(); err != nil {
				return err
			}
		case <-t.done:
			// best-effort flush of the remaining queue before closing
			return drain()
		}
	}
}

// writeLine 只把记录及其分隔符追加到缓冲区，不 Flush；
// 由调用方决定何时下发，从而让连续多条记录共享同一次 socket 写。
func (d *NetcatDriver) writeLine(w *bufio.Writer, data []byte) error {
	if _, err := w.Write(data); err != nil {
		return err
	}
	if _, err := w.Write(d.sep); err != nil {
		return err
	}
	return nil
}

func (d *NetcatDriver) connIsStopped(t *ncTarget) bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

func ioClose(conn io.Closer) {
	_ = conn.Close()
}

// Send delivers a record to a single target picked round-robin.
// NeedsKey 报告该驱动是否需要消息 key。
// netcat 按轮询把消息发往各目标，不按 key 分区，故不需要。
func (d *NetcatDriver) NeedsKey() bool { return false }

func (d *NetcatDriver) Send(key, data []byte) error {
	if len(d.targets) == 0 {
		return nil
	}
	n := d.rr.Add(1)
	t := d.targets[int(n-1)%len(d.targets)]

	if d.drop {
		select {
		case t.queue <- data:
		default:
			t.dropped.Add(1)
		}
	} else {
		t.queue <- data
	}
	return nil
}

// Dropped returns the number of dropped records for a target address.
func (d *NetcatDriver) Dropped(addr string) uint64 {
	for _, t := range d.targets {
		if t.addr == addr {
			return t.dropped.Load()
		}
	}
	return 0
}

// TargetStats reports the send buffer queue usage of every target.
func (d *NetcatDriver) TargetStats() []transport.TargetStat {
	stats := make([]transport.TargetStat, 0, len(d.targets))
	for _, t := range d.targets {
		depth := len(t.queue)
		capacity := cap(t.queue)
		utilization := 0.0
		if capacity > 0 {
			utilization = float64(depth) / float64(capacity)
		}
		stats = append(stats, transport.TargetStat{
			Addr:        t.addr,
			Depth:       depth,
			Capacity:    capacity,
			Utilization: utilization,
			Dropped:     t.dropped.Load(),
			Connected:   t.connected.Load(),
			Drop:        d.drop,
		})
	}
	return stats
}

// Close stops the writer goroutines and closes the connections.
func (d *NetcatDriver) Close() error {
	for _, t := range d.targets {
		close(t.done)
	}
	for _, t := range d.targets {
		t.wg.Wait()
	}
	return nil
}

func init() {
	d := &NetcatDriver{}
	transport.RegisterTransportDriver("netcat", d)
}
