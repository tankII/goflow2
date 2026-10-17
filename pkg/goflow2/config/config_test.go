package config

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetricsFlagDefaultsTrue(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := BindFlags(fs)

	require.NoError(t, fs.Parse(nil))
	require.True(t, cfg.Metrics)
}

func TestMetricsFlagParsesFalse(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := BindFlags(fs)

	require.NoError(t, fs.Parse([]string{"-metrics=false"}))
	require.False(t, cfg.Metrics)
}

func TestPprofFlagDefaultsFalse(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := BindFlags(fs)

	require.NoError(t, fs.Parse(nil))
	require.False(t, cfg.Pprof)
}

func TestPprofFlagParsesTrue(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := BindFlags(fs)

	require.NoError(t, fs.Parse([]string{"-pprof"}))
	require.True(t, cfg.Pprof)
}
