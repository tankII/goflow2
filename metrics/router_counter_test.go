package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestIncCreatesBuckets(t *testing.T) {
	rc := NewRouterCounter(time.Hour*72, time.Hour*24*90)
	defer rc.Stop()

	now := time.Now().Local()
	fixed := now.Format(hourlyKeyLayout)

	rc.IncPacket("10.0.0.1", 1500)
	rc.IncFlow("10.0.0.1", 3)
	rc.IncPacket("10.0.0.1", 2000)
	rc.IncFlow("10.0.0.1", 5)

	entries, err := rc.GetHourly("10.0.0.1")
	if err != nil {
		t.Fatalf("GetHourly: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 hourly bucket, got %d", len(entries))
	}
	if entries[0].Time != fixed {
		t.Fatalf("hourly key: got %q want %q", entries[0].Time, fixed)
	}
	if entries[0].Packets != 2 {
		t.Errorf("packets: got %d want 2", entries[0].Packets)
	}
	if entries[0].Flows != 8 {
		t.Errorf("flows: got %d want 8", entries[0].Flows)
	}
	if entries[0].Bytes != 3500 {
		t.Errorf("bytes: got %d want 3500", entries[0].Bytes)
	}

	daily, err := rc.GetDaily("10.0.0.1")
	if err != nil {
		t.Fatalf("GetDaily: %v", err)
	}
	if len(daily) != 1 {
		t.Fatalf("expected 1 daily bucket, got %d", len(daily))
	}
	if daily[0].Time != now.Format(dailyKeyLayout) {
		t.Errorf("daily key: got %q want %q", daily[0].Time, now.Format(dailyKeyLayout))
	}
	if daily[0].Packets != 2 || daily[0].Flows != 8 || daily[0].Bytes != 3500 {
		t.Errorf("daily counts wrong: %+v", daily[0])
	}
}

func TestRotationAcrossHourAndDay(t *testing.T) {
	rc := NewRouterCounter(time.Hour*72, time.Hour*24*90)
	defer rc.Stop()

	base := time.Date(2026, 10, 8, 15, 30, 0, 0, time.Local)
	rc.now = func() time.Time { return base }
	rc.IncPacket("10.0.0.1", 100)

	// next hour
	rc.now = func() time.Time { return base.Add(1 * time.Hour) }
	rc.IncPacket("10.0.0.1", 200)

	entries, err := rc.GetHourly("10.0.0.1")
	if err != nil {
		t.Fatalf("GetHourly: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 hourly buckets after crossing an hour, got %d", len(entries))
	}
	if entries[0].Time != base.Format(hourlyKeyLayout) || entries[0].Packets != 1 || entries[0].Bytes != 100 {
		t.Errorf("first bucket wrong: %+v", entries[0])
	}
	if entries[1].Time != base.Add(time.Hour).Format(hourlyKeyLayout) || entries[1].Packets != 1 || entries[1].Bytes != 200 {
		t.Errorf("second bucket wrong: %+v", entries[1])
	}

	// next day
	rc.now = func() time.Time { return base.Add(9 * time.Hour) }
	rc.IncPacket("10.0.0.1", 300)

	daily, err := rc.GetDaily("10.0.0.1")
	if err != nil {
		t.Fatalf("GetDaily: %v", err)
	}
	if len(daily) != 2 {
		t.Fatalf("expected 2 daily buckets after crossing a day, got %d", len(daily))
	}
	total := daily[0].Packets + daily[1].Packets
	if total != 3 {
		t.Errorf("daily total packets: got %d want 3 (one datagram per IncPacket)", total)
	}
}

func TestPruneExpiredBuckets(t *testing.T) {
	rc := NewRouterCounter(time.Hour, time.Hour*24)
	defer rc.Stop()

	rc.IncPacket("10.0.0.1", 100)
	rc.IncPacket("10.0.0.2", 100)

	// advance just past both TTLs: everything expires
	now := time.Now().Add(time.Hour * 26)
	rc.now = func() time.Time { return now }

	if devs := rc.Devices(); len(devs) != 0 {
		t.Fatalf("expected all devices pruned, got %d", len(devs))
	}
	if _, err := rc.GetHourly("10.0.0.1"); err == nil {
		t.Fatal("expected error for pruned router hourly")
	}
	if _, err := rc.GetDaily("10.0.0.1"); err == nil {
		t.Fatal("expected error for pruned router daily")
	}
}

func TestPruneKeepsFreshBuckets(t *testing.T) {
	rc := NewRouterCounter(time.Hour*72, time.Hour*24*90)
	defer rc.Stop()

	base := time.Now().Local()
	rc.now = func() time.Time { return base }
	rc.IncPacket("10.0.0.1", 100)

	// advance 2h, still within retention
	rc.now = func() time.Time { return base.Add(2 * time.Hour) }
	rc.IncPacket("10.0.0.1", 50)

	devs := rc.Devices()
	if len(devs) != 1 {
		t.Fatalf("expected device to survive short advance, got %d", len(devs))
	}
	if devs[0].TotalPackets != 2 {
		t.Errorf("total packets: got %d want 2 (one datagram per IncPacket)", devs[0].TotalPackets)
	}
	if devs[0].TotalFlows != 0 {
		t.Errorf("total flows: got %d want 0", devs[0].TotalFlows)
	}
}

func TestDevicesSortedByLastSeen(t *testing.T) {
	rc := NewRouterCounter(time.Hour*72, time.Hour*24*90)
	defer rc.Stop()

	rc.now = func() time.Time { return time.Date(2026, 10, 8, 8, 0, 0, 0, time.Local) }
	rc.IncPacket("10.0.0.1", 100)

	rc.now = func() time.Time { return time.Date(2026, 10, 8, 9, 0, 0, 0, time.Local) }
	rc.IncPacket("10.0.0.2", 100)

	rc.now = func() time.Time { return time.Date(2026, 10, 8, 7, 0, 0, 0, time.Local) }
	rc.IncPacket("10.0.0.3", 100)

	devs := rc.Devices()
	if len(devs) != 3 {
		t.Fatalf("expected 3 devices, got %d", len(devs))
	}
	// newest first: 10.0.0.2 (09:00), 10.0.0.1 (08:00), 10.0.0.3 (07:00)
	want := []string{"10.0.0.2", "10.0.0.1", "10.0.0.3"}
	for i, w := range want {
		if devs[i].Router != w {
			t.Errorf("devices[%d].Router = %q, want %q", i, devs[i].Router, w)
		}
	}
}

func TestTotalsFromDailyOnly(t *testing.T) {
	rc := NewRouterCounter(time.Hour*72, time.Hour*24*90)
	defer rc.Stop()

	base := time.Date(2026, 10, 8, 10, 0, 0, 0, time.Local)
	rc.now = func() time.Time { return base }
	// 3 hourly buckets, all within the same day
	rc.IncPacket("10.0.0.1", 100)
	rc.now = func() time.Time { return base.Add(1 * time.Hour) }
	rc.IncPacket("10.0.0.1", 100)
	rc.now = func() time.Time { return base.Add(2 * time.Hour) }
	rc.IncPacket("10.0.0.1", 100)
	rc.IncFlow("10.0.0.1", 7)

	devs := rc.Devices()
	if len(devs) != 1 {
		t.Fatalf("expected 1 device, got %d", len(devs))
	}
	// daily buckets total all 3 datagrams + 7 flows, no hourly/daily overlap
	if devs[0].TotalPackets != 3 {
		t.Errorf("total packets: got %d want 3", devs[0].TotalPackets)
	}
	if devs[0].TotalFlows != 7 {
		t.Errorf("total flows: got %d want 7", devs[0].TotalFlows)
	}
}

func TestConcurrentIncAndRead(t *testing.T) {
	rc := NewRouterCounter(time.Hour*72, time.Hour*24*90)
	defer rc.Stop()

	const workers = 16
	const iterations = 500

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			router := "10.0.0.1"
			for i := 0; i < iterations; i++ {
				rc.IncPacket(router, 100)
				rc.IncFlow(router, 1)
				if i%50 == 0 {
					_, _ = rc.GetHourly(router)
					_, _ = rc.GetDaily(router)
					_ = rc.Devices()
				}
			}
		}(w)
	}
	wg.Wait()

	devs := rc.Devices()
	if len(devs) != 1 {
		t.Fatalf("expected 1 device, got %d", len(devs))
	}
	if devs[0].TotalPackets != workers*iterations {
		t.Errorf("total packets: got %d want %d", devs[0].TotalPackets, workers*iterations)
	}
	if devs[0].TotalFlows != workers*iterations {
		t.Errorf("total flows: got %d want %d", devs[0].TotalFlows, workers*iterations)
	}
}

func TestGetUnknownRouter(t *testing.T) {
	rc := NewRouterCounter(time.Hour*72, time.Hour*24*90)
	defer rc.Stop()

	if _, err := rc.GetHourly("192.0.2.1"); err == nil {
		t.Fatal("expected error for unknown router hourly")
	}
	if _, err := rc.GetDaily("192.0.2.1"); err == nil {
		t.Fatal("expected error for unknown router daily")
	}
}
