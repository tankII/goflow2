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

	return nil
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

// serve writes queued lines to an established connection until failure or stop.
func (d *NetcatDriver) serve(t *ncTarget, conn net.Conn) error {
	w := bufio.NewWriter(conn)
	for {
		select {
		case data := <-t.queue:
			if err := d.writeLine(w, data); err != nil {
				return err
			}
		case <-t.done:
			// best-effort flush of the remaining queue before closing
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
	}
}

func (d *NetcatDriver) writeLine(w *bufio.Writer, data []byte) error {
	if _, err := w.Write(data); err != nil {
		return err
	}
	if _, err := w.Write(d.sep); err != nil {
		return err
	}
	return w.Flush()
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
