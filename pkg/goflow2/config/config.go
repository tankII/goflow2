package config

import (
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/netsampler/goflow2/v3/pkg/goflow2/logging"
	protoproducer "github.com/netsampler/goflow2/v3/producer/proto"
	"gopkg.in/yaml.v3"
)

// Config holds configuration for the GoFlow2 application.
type Config struct {
	ListenAddresses string

	Logging logging.Options

	Produce   string
	Format    string
	Transport string

	ErrCnt int
	ErrInt time.Duration

	Addr string

	StoreHTTPPath string

	DeviceStatsPath string
	DeviceHourlyTTL time.Duration
	DeviceDailyTTL  time.Duration

	QueueStatsPath string

	TemplatesTTL time.Duration

	TemplatesSweepInterval  time.Duration
	TemplatesExtendOnAccess bool

	SamplingRatesTTL            time.Duration
	SamplingRatesSweepInterval  time.Duration
	SamplingRatesExtendOnAccess bool

	StoreJSONPath     string
	StoreJSONInterval time.Duration

	MappingFile string
}

// BindFlags registers configuration flags and returns a Config.
func BindFlags(fs *flag.FlagSet) *Config {
	cfg := &Config{}

	fs.StringVar(&cfg.ListenAddresses, "listen", "sflow://:6343,netflow://:2055", "listen addresses")
	fs.StringVar(&cfg.Produce, "produce", "sample", "Producer method (sample or raw)")
	logging.RegisterFlags(fs, &cfg.Logging)
	BindCommonFlags(fs, &cfg.Format, &cfg.Transport)
	fs.IntVar(&cfg.ErrCnt, "err.cnt", 10, "Maximum errors per batch for muting")
	fs.DurationVar(&cfg.ErrInt, "err.int", time.Second*10, "Maximum errors interval for muting")
	fs.StringVar(&cfg.Addr, "addr", ":8080", "HTTP server address")
	fs.StringVar(&cfg.StoreHTTPPath, "store.http.path", "/store", "Flowstore HTTP path")
	fs.StringVar(&cfg.DeviceStatsPath, "device.stats.path", "/device-stats", "Device statistics base path")
	fs.DurationVar(&cfg.DeviceHourlyTTL, "device.retention.hourly", 72*time.Hour, "Hourly device stats retention (hours)")
	fs.DurationVar(&cfg.DeviceDailyTTL, "device.retention.daily", 90*24*time.Hour, "Daily device stats retention (hours, e.g. 2160h for 90 days)")
	fs.StringVar(&cfg.QueueStatsPath, "queue.stats.path", "/queue-stats", "Receive queue usage HTTP path")
	fs.DurationVar(&cfg.TemplatesTTL, "templates.ttl", 0, "NetFlow/IPFIX templates TTL (0 disables expiry)")
	fs.DurationVar(&cfg.TemplatesSweepInterval, "templates.sweep-interval", time.Minute, "NetFlow/IPFIX template expiry sweep interval")
	fs.BoolVar(&cfg.TemplatesExtendOnAccess, "templates.ttl.extend-on-access", false, "Extend template TTL on access")
	fs.StringVar(&cfg.StoreJSONPath, "store.json.path", "", "Shared flowstore JSON output path (empty disables persistence)")
	fs.DurationVar(&cfg.StoreJSONInterval, "store.json.interval", time.Second*10, "Shared flowstore JSON write interval")

	fs.DurationVar(&cfg.SamplingRatesTTL, "sampling.ttl", 0, "Sampling rates TTL (0 disables expiry)")
	fs.DurationVar(&cfg.SamplingRatesSweepInterval, "sampling.sweep-interval", time.Minute, "Sampling rates expiry sweep interval")
	fs.BoolVar(&cfg.SamplingRatesExtendOnAccess, "sampling.ttl.extend-on-access", false, "Extend sampling rate TTL on access")
	fs.StringVar(&cfg.MappingFile, "mapping", "", "Configuration file for custom mappings")

	return cfg
}

// LoadMapping reads a YAML mapping configuration.
func LoadMapping(r io.Reader) (*protoproducer.ProducerConfig, error) {
	config := &protoproducer.ProducerConfig{}
	dec := yaml.NewDecoder(r)
	err := dec.Decode(config)
	if err != nil {
		return nil, fmt.Errorf("decode mapping: %w", err)
	}
	return config, nil
}
