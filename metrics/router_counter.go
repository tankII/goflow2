// Package metrics exposes Prometheus collectors and device statistics.
package metrics

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// hourlyKeyLayout is the local wall-clock layout used for hourly buckets.
const hourlyKeyLayout = "2006-01-02T15:00:00"

// dailyKeyLayout is the local wall-clock layout used for daily buckets.
const dailyKeyLayout = "2006-01-02"

// BucketCounter aggregates counters for a single time bucket.
type BucketCounter struct {
	Packets int64 // received UDP datagrams
	Flows   int64 // decoded flow records
	Bytes   int64 // received payload bytes
}

// HourlyEntry is one hour of per-device statistics.
type HourlyEntry struct {
	Time    string `json:"time"`
	Packets int64  `json:"packets"`
	Flows   int64  `json:"flows"`
	Bytes   int64  `json:"bytes"`
}

// DailyEntry is one day of per-device statistics.
type DailyEntry struct {
	Time    string `json:"time"`
	Packets int64  `json:"packets"`
	Flows   int64  `json:"flows"`
	Bytes   int64  `json:"bytes"`
}

// DeviceInfo identifies a device in the device list.
type DeviceInfo struct {
	Router       string `json:"router"`
	LastSeen     string `json:"last_seen"`
	TotalPackets int64  `json:"total_packets"`
	TotalFlows   int64  `json:"total_flows"`
}

// deviceStats holds per-device buckets. Time buckets use local time.
type deviceStats struct {
	hourly   map[string]*BucketCounter
	daily    map[string]*BucketCounter
	lastSeen time.Time
}

// RouterCounter tracks per-device hourly and daily statistics. Safe for
// concurrent use. All bucket keys are formatted in local time.
type RouterCounter struct {
	mu        sync.RWMutex
	devices   map[string]*deviceStats
	hourlyTTL time.Duration
	dailyTTL  time.Duration
	stopCh    chan struct{}
	doneCh    chan struct{}
	now       func() time.Time
}

// NewRouterCounter creates a RouterCounter with the given retention periods
// (non-positive values fall back to 72h / 90d) and starts the sweep loop.
func NewRouterCounter(hourlyTTL, dailyTTL time.Duration) *RouterCounter {
	if hourlyTTL <= 0 {
		hourlyTTL = 72 * time.Hour
	}
	if dailyTTL <= 0 {
		dailyTTL = 90 * 24 * time.Hour
	}
	rc := &RouterCounter{
		devices:   make(map[string]*deviceStats),
		hourlyTTL: hourlyTTL,
		dailyTTL:  dailyTTL,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		now:       time.Now,
	}
	go rc.sweep()
	return rc
}

// Stop stops the background sweep loop and waits for it to exit.
func (rc *RouterCounter) Stop() {
	close(rc.stopCh)
	<-rc.doneCh
}

// IncPacket records one received UDP datagram of the given size.
func (rc *RouterCounter) IncPacket(router string, size int64) {
	rc.inc(router, 1, 0, size)
}

// IncFlow records count decoded flow records.
func (rc *RouterCounter) IncFlow(router string, count int64) {
	rc.inc(router, 0, count, 0)
}

func (rc *RouterCounter) inc(router string, packets, flows int64, bytes int64) {
	if packets == 0 && flows == 0 {
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()

	ds, ok := rc.devices[router]
	if !ok {
		ds = &deviceStats{
			hourly: make(map[string]*BucketCounter),
			daily:  make(map[string]*BucketCounter),
		}
		rc.devices[router] = ds
		DeviceActiveGauge.Inc()
	}

	now := rc.now()
	ds.lastSeen = now
	hKey := now.Format(hourlyKeyLayout)
	dKey := now.Format(dailyKeyLayout)

	if ds.hourly[hKey] == nil {
		ds.hourly[hKey] = &BucketCounter{}
	}
	ds.hourly[hKey].Packets += packets
	ds.hourly[hKey].Flows += flows
	ds.hourly[hKey].Bytes += bytes

	if ds.daily[dKey] == nil {
		ds.daily[dKey] = &BucketCounter{}
	}
	ds.daily[dKey].Packets += packets
	ds.daily[dKey].Flows += flows
	ds.daily[dKey].Bytes += bytes

	// keep Prometheus gauges current on the hot path (cheap, per-device)
	labels := prometheus.Labels{"router": router}
	DeviceLastSeenGauge.With(labels).Set(float64(now.Unix()))
	if packets > 0 {
		DevicePacketsTotalGauge.With(labels).Add(float64(packets))
	}
	if flows > 0 {
		DeviceFlowsTotalGauge.With(labels).Add(float64(flows))
	}
}

// removeDevice deletes a device and clears its Prometheus gauge series.
// Callers must hold the write lock.
func (rc *RouterCounter) removeDevice(router string) {
	delete(rc.devices, router)
	DeviceActiveGauge.Dec()
	labels := prometheus.Labels{"router": router}
	DeviceLastSeenGauge.Delete(labels)
	DevicePacketsTotalGauge.Delete(labels)
	DeviceFlowsTotalGauge.Delete(labels)
}

// Devices returns active devices sorted by last seen (newest first).
// Totals are derived from daily buckets only to avoid overlapping counts
// between hourly and daily retentions.
func (rc *RouterCounter) Devices() []DeviceInfo {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	now := rc.now()
	result := make([]DeviceInfo, 0, len(rc.devices))
	for router, ds := range rc.devices {
		if rc.pruneDevice(ds, now) {
			rc.removeDevice(router)
			continue
		}
		var totalPackets, totalFlows int64
		for _, b := range ds.daily {
			totalPackets += b.Packets
			totalFlows += b.Flows
		}
		result = append(result, DeviceInfo{
			Router:       router,
			LastSeen:     ds.lastSeen.Format(time.RFC3339),
			TotalPackets: totalPackets,
			TotalFlows:   totalFlows,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[j].LastSeen < result[i].LastSeen
	})
	return result
}

// GetHourly returns hourly statistics for a router, oldest first.
func (rc *RouterCounter) GetHourly(router string) ([]HourlyEntry, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	ds, ok := rc.devices[router]
	if !ok {
		return nil, fmt.Errorf("router %q not found", router)
	}
	if rc.pruneDevice(ds, rc.now()) {
		rc.removeDevice(router)
		return nil, fmt.Errorf("router %q not found", router)
	}

	entries := make([]HourlyEntry, 0, len(ds.hourly))
	for t, b := range ds.hourly {
		entries = append(entries, HourlyEntry{Time: t, Packets: b.Packets, Flows: b.Flows, Bytes: b.Bytes})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Time < entries[j].Time })
	return entries, nil
}

// GetDaily returns daily statistics for a router, oldest first.
func (rc *RouterCounter) GetDaily(router string) ([]DailyEntry, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	ds, ok := rc.devices[router]
	if !ok {
		return nil, fmt.Errorf("router %q not found", router)
	}
	if rc.pruneDevice(ds, rc.now()) {
		rc.removeDevice(router)
		return nil, fmt.Errorf("router %q not found", router)
	}

	entries := make([]DailyEntry, 0, len(ds.daily))
	for t, b := range ds.daily {
		entries = append(entries, DailyEntry{Time: t, Packets: b.Packets, Flows: b.Flows, Bytes: b.Bytes})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Time < entries[j].Time })
	return entries, nil
}

// pruneDevice removes buckets older than the retention periods. It returns
// true when the device no longer holds any buckets. Callers must hold the
// write lock.
func (rc *RouterCounter) pruneDevice(ds *deviceStats, now time.Time) bool {
	hourCut := now.Add(-rc.hourlyTTL)
	for k := range ds.hourly {
		t, err := time.ParseInLocation(hourlyKeyLayout, k, time.Local)
		if err != nil || t.Before(hourCut) {
			delete(ds.hourly, k)
		}
	}
	dayCut := now.Add(-rc.dailyTTL)
	for k := range ds.daily {
		t, err := time.ParseInLocation(dailyKeyLayout, k, time.Local)
		if err != nil || t.Before(dayCut) {
			delete(ds.daily, k)
		}
	}
	return len(ds.hourly) == 0 && len(ds.daily) == 0
}

// sweep periodically prunes expired devices.
func (rc *RouterCounter) sweep() {
	defer close(rc.doneCh)
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-rc.stopCh:
			return
		case <-ticker.C:
			rc.mu.Lock()
			now := rc.now()
			for router, ds := range rc.devices {
				if rc.pruneDevice(ds, now) {
					rc.removeDevice(router)
				}
			}
			rc.mu.Unlock()
		}
	}
}

// DeviceActiveGauge reports the number of active source devices.
var DeviceActiveGauge = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name:      "device_active_count",
		Help:      "Number of active source devices currently tracked.",
		Namespace: NAMESPACE,
	},
)

// DeviceLastSeenGauge reports the last-seen time of each device.
var DeviceLastSeenGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name:      "device_last_seen_timestamp_seconds",
		Help:      "Unix timestamp of the last flow received from each device.",
		Namespace: NAMESPACE,
	},
	[]string{"router"},
)

// DevicePacketsTotalGauge reports cumulative datagram count per device.
var DevicePacketsTotalGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name:      "device_packets_total",
		Help:      "Cumulative UDP datagrams received from each device.",
		Namespace: NAMESPACE,
	},
	[]string{"router"},
)

// DeviceFlowsTotalGauge reports cumulative flow-record count per device.
var DeviceFlowsTotalGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name:      "device_flows_total",
		Help:      "Cumulative flow records received from each device.",
		Namespace: NAMESPACE,
	},
	[]string{"router"},
)

func init() {
	prometheus.MustRegister(DeviceActiveGauge)
	prometheus.MustRegister(DeviceLastSeenGauge)
	prometheus.MustRegister(DevicePacketsTotalGauge)
	prometheus.MustRegister(DeviceFlowsTotalGauge)
}

// DeviceListHandler serves GET /device-list.
func DeviceListHandler(rc *RouterCounter) http.HandlerFunc {
	return func(wr http.ResponseWriter, r *http.Request) {
		wr.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(wr).Encode(rc.Devices()); err != nil {
			slog.Error("device list encode error", slog.String("error", err.Error()))
		}
	}
}

// DeviceHourlyHandler serves GET <base>/hourly?router=X.
func DeviceHourlyHandler(rc *RouterCounter) http.HandlerFunc {
	return func(wr http.ResponseWriter, r *http.Request) {
		router := r.URL.Query().Get("router")
		if router == "" {
			http.Error(wr, `{"error":"missing router parameter"}`+"\n", http.StatusBadRequest)
			return
		}
		entries, err := rc.GetHourly(router)
		if err != nil {
			http.Error(wr, `{"error":"`+err.Error()+`"}`+"\n", http.StatusNotFound)
			return
		}
		wr.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(wr).Encode(map[string]interface{}{
			"router":  router,
			"window":  "hourly",
			"buckets": entries,
		}); err != nil {
			slog.Error("device hourly encode error", slog.String("error", err.Error()))
		}
	}
}

// DeviceDailyHandler serves GET <base>/daily?router=X.
func DeviceDailyHandler(rc *RouterCounter) http.HandlerFunc {
	return func(wr http.ResponseWriter, r *http.Request) {
		router := r.URL.Query().Get("router")
		if router == "" {
			http.Error(wr, `{"error":"missing router parameter"}`+"\n", http.StatusBadRequest)
			return
		}
		entries, err := rc.GetDaily(router)
		if err != nil {
			http.Error(wr, `{"error":"`+err.Error()+`"}`+"\n", http.StatusNotFound)
			return
		}
		wr.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(wr).Encode(map[string]interface{}{
			"router":  router,
			"window":  "daily",
			"buckets": entries,
		}); err != nil {
			slog.Error("device daily encode error", slog.String("error", err.Error()))
		}
	}
}
