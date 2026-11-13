// Package transport provides a registry and interfaces for output transports.
package transport

import (
	"fmt"
	"sync"
)

var (
	transportDrivers = make(map[string]TransportDriver)
	lock             = &sync.RWMutex{}

	// ErrTransport is the base error for transport failures.
	ErrTransport = fmt.Errorf("transport error")
)

// DriverTransportError wraps a driver-specific error with its transport name.
type DriverTransportError struct {
	Driver string
	Err    error
}

func (e *DriverTransportError) Error() string {
	return fmt.Sprintf("%s for %s transport", e.Err.Error(), e.Driver)
}

func (e *DriverTransportError) Unwrap() []error {
	return []error{ErrTransport, e.Err}
}

// TransportDriver describes a transport plugin lifecycle and send method.
type TransportDriver interface {
	Prepare() error              // Prepare driver
	Init() error                 // Init driver
	Close() error                // Close driver
	Send(key, data []byte) error // Send a message
}

// TargetStat reports the send buffer queue usage of a single transport target.
type TargetStat struct {
	Addr        string  `json:"addr"`
	Depth       int     `json:"depth"`
	Capacity    int     `json:"capacity"`
	Utilization float64 `json:"utilization"`
	Dropped     uint64  `json:"dropped"`
	Connected   bool    `json:"connected"`
	Drop        bool    `json:"drop"`
}

// TargetStatsProvider is implemented by transports exposing per-target send
// buffer queue usage.
type TargetStatsProvider interface {
	TargetStats() []TargetStat
}

// TransportInterface is the minimal interface needed to send payloads.
type TransportInterface interface {
	Send(key, data []byte) error
}

// KeyAwareTransport 是可选接口：声明该传输是否真的使用消息 key。
//
// 消息 key 的计算需要反射 + 格式化（见 ProtoProducerMessage.Key），
// 在高包率下开销可观。而 netcat 走轮询、file 直接落盘，两者都不使用 key；
// 只有 Kafka 用它做分区键。故让驱动自己声明，避免为不需要的用途付出代价。
//
// 未实现该接口的传输按「需要 key」处理，保持既有行为不变。
type KeyAwareTransport interface {
	// NeedsKey 报告该传输是否需要消息 key。
	NeedsKey() bool
}

// TransportNeedsKey 报告给定传输是否需要消息 key。
func TransportNeedsKey(t TransportInterface) bool {
	if t == nil {
		return false
	}
	if d, ok := t.(KeyAwareTransport); ok {
		return d.NeedsKey()
	}
	return true
}

// Transport is a named transport wrapper used by the registry.
type Transport struct {
	TransportDriver
	name string
}

// NeedsKey 转发给底层驱动，使包装后的传输同样能声明是否需要 key。
func (t *Transport) NeedsKey() bool {
	if t == nil || t.TransportDriver == nil {
		return false
	}
	if d, ok := t.TransportDriver.(KeyAwareTransport); ok {
		return d.NeedsKey()
	}
	return true
}

// Close calls the driver Close and wraps errors with transport metadata.
func (t *Transport) Close() error {
	if err := t.TransportDriver.Close(); err != nil {
		return &DriverTransportError{t.name, err}
	}
	return nil
}

// Send forwards data to the driver and wraps errors with transport metadata.
func (t *Transport) Send(key, data []byte) error {
	if err := t.TransportDriver.Send(key, data); err != nil {
		return &DriverTransportError{t.name, err}
	}
	return nil
}

// RegisterTransportDriver registers and prepares a transport under a name.
func RegisterTransportDriver(name string, t TransportDriver) {
	lock.Lock()
	transportDrivers[name] = t
	lock.Unlock()

	if err := t.Prepare(); err != nil {
		panic(err)
	}
}

// FindTransport returns a configured transport by name.
func FindTransport(name string) (*Transport, error) {
	lock.RLock()
	t, ok := transportDrivers[name]
	lock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w %s not found", ErrTransport, name)
	}

	err := t.Init()
	if err != nil {
		err = &DriverTransportError{name, err}
	}
	return &Transport{t, name}, err
}

// StatsSource returns a function reporting the send buffer queue usage of the
// transport targets, or nil when the driver does not expose such stats.
func (t *Transport) StatsSource() func() []TargetStat {
	if provider, ok := t.TransportDriver.(TargetStatsProvider); ok {
		return provider.TargetStats
	}
	return nil
}

// GetTransports returns the list of registered transport names.
func GetTransports() []string {
	lock.RLock()
	defer lock.RUnlock()
	t := make([]string, len(transportDrivers))
	var i int
	for k := range transportDrivers {
		t[i] = k
		i++
	}
	return t
}
