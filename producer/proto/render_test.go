package protoproducer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func setRendererLocation(loc *time.Location) func() {
	prev := rendererLocation
	rendererLocation = loc
	return func() { rendererLocation = prev }
}

func TestDetectRendererLocation(t *testing.T) {
	t.Setenv("TZ", "")
	assert.Equal(t, time.UTC, detectRendererLocation())

	t.Setenv("TZ", "Asia/Shanghai")
	assert.Equal(t, time.Local, detectRendererLocation())
}

func TestDateTimeFormatRendererNanos(t *testing.T) {
	defer setRendererLocation(time.UTC)()

	ts := time.Date(2026, 10, 6, 21, 42, 4, 746000000, time.UTC)

	var m ProtoProducerMessage

	got := dateTimeFormatRenderer("2006-01-02 15:04:05.000", true)(&m, "TimeReceivedNs", uint64(ts.UnixNano()))
	assert.Equal(t, "2026-10-06 21:42:04.746", got)

	got = dateTimeFormatRenderer("2006-01-02 15:04:05.000000", true)(&m, "TimeReceivedNs", int64(ts.UnixNano()))
	assert.Equal(t, "2026-10-06 21:42:04.746000", got)
}

func TestDateTimeFormatRendererSeconds(t *testing.T) {
	defer setRendererLocation(time.UTC)()

	ts := time.Date(2026, 10, 6, 21, 42, 4, 0, time.UTC)

	var m ProtoProducerMessage
	got := dateTimeFormatRenderer("2006-01-02 15:04:05", false)(&m, "time_received", uint64(ts.Unix()))
	assert.Equal(t, "2026-10-06 21:42:04", got)
}

func TestDateTimeFormatRendererLocal(t *testing.T) {
	defer setRendererLocation(time.FixedZone("CST", 8*3600))()

	ts := time.Date(2026, 10, 6, 21, 42, 4, 746000000, time.UTC)

	var m ProtoProducerMessage

	got := dateTimeFormatRenderer("2006-01-02 15:04:05.000", true)(&m, "TimeReceivedNs", uint64(ts.UnixNano()))
	assert.Equal(t, "2026-10-07 05:42:04.746", got)
}

func TestDateTimeRendererDefaultUTC(t *testing.T) {
	defer setRendererLocation(time.UTC)()

	ts := time.Date(2026, 10, 6, 21, 42, 4, 0, time.UTC)

	var m ProtoProducerMessage

	assert.Equal(t, ts.Format(time.RFC3339Nano), DateTimeRenderer(&m, "TimeFlowStart", uint64(ts.Unix())))
	assert.Equal(t, ts.Add(123456789*time.Nanosecond).Format(time.RFC3339Nano), DateTimeNanoRenderer(&m, "TimeFlowStartNs", uint64(ts.Add(123456789*time.Nanosecond).UnixNano())))
}

func TestDateTimeRendererLocal(t *testing.T) {
	defer setRendererLocation(time.FixedZone("CST", 8*3600))()

	ts := time.Date(2026, 10, 6, 21, 42, 4, 0, time.UTC)

	var m ProtoProducerMessage

	assert.Equal(t, "2026-10-07T05:42:04+08:00", DateTimeRenderer(&m, "TimeFlowStart", uint64(ts.Unix())))
}

func TestResolveRenderer(t *testing.T) {
	_, ok := resolveRenderer(RendererIP)
	assert.True(t, ok)

	_, ok = resolveRenderer("datetime")
	assert.True(t, ok)

	_, ok = resolveRenderer("datetimenano")
	assert.True(t, ok)

	_, ok = resolveRenderer("datetime:2006-01-02 15:04:05.000")
	assert.True(t, ok)

	_, ok = resolveRenderer("datetimenano:2006-01-02 15:04:05.000")
	assert.True(t, ok)

	_, ok = resolveRenderer("bogus")
	assert.False(t, ok)

	_, ok = resolveRenderer("bogus:xyz")
	assert.False(t, ok)
}

func TestMapFormatTimeLayout(t *testing.T) {
	defer setRendererLocation(time.UTC)()

	cfg := &ProducerConfig{}
	cfg.Formatter.Render = map[string]RendererID{
		"TimeReceivedNs": "datetimenano:2006-01-02 15:04:05.000",
	}

	fm, err := mapFormat(cfg)
	assert.Nil(t, err)

	renderer, ok := fm.Render("TimeReceivedNs")
	assert.True(t, ok)

	ts := time.Date(2026, 10, 6, 21, 42, 4, 746000000, time.UTC)

	var m ProtoProducerMessage
	assert.Equal(t, "2026-10-06 21:42:04.746", renderer(&m, "TimeReceivedNs", uint64(ts.UnixNano())))
}

func TestMapFormatInvalidRenderer(t *testing.T) {
	cfg := &ProducerConfig{}
	cfg.Formatter.Render = map[string]RendererID{
		"TimeReceivedNs": "bogus",
	}

	_, err := mapFormat(cfg)
	assert.NotNil(t, err)
}
