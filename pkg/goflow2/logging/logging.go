package logging

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

type Options struct {
	Level        string
	Format       string
	File         string
	MaxSize      int
	MaxBackups   int
	MaxAge       int
	Compress     bool
	AlsoToStderr bool
	AddSource    bool
	HTTP         bool
}

// RegisterFlags binds all logging options to a flag set.
func RegisterFlags(fs *flag.FlagSet, opts *Options) {
	fs.StringVar(&opts.Level, "loglevel", "info", "Log level (debug, info, warn, error)")
	fs.StringVar(&opts.Format, "logfmt", "normal", "Log format (json, normal)")
	fs.StringVar(&opts.File, "log.file", "", "Write logs to this file (default: stderr)")
	fs.IntVar(&opts.MaxSize, "log.file.max-size", 100, "Max size in MB of a log file before rotation")
	fs.IntVar(&opts.MaxBackups, "log.file.max-backups", 3, "Max number of old log files to retain")
	fs.IntVar(&opts.MaxAge, "log.file.max-age", 28, "Max number of days to retain old log files")
	fs.BoolVar(&opts.Compress, "log.file.compress", true, "Gzip-compress rotated log files")
	fs.BoolVar(&opts.AlsoToStderr, "log.also-stderr", false, "Also write logs to stderr when -log.file is set")
	fs.BoolVar(&opts.AddSource, "log.source", false, "Add source file:line to log records")
	fs.BoolVar(&opts.HTTP, "log.http", false, "Enable HTTP access log")
}

// NewLogger constructs a slog logger from options.
func NewLogger(opts Options) (*slog.Logger, error) {
	var loglevel slog.Level
	if err := loglevel.UnmarshalText([]byte(opts.Level)); err != nil {
		return nil, fmt.Errorf("parse log level %q: %w", opts.Level, err)
	}

	hopts := slog.HandlerOptions{
		Level:     loglevel,
		AddSource: opts.AddSource,
	}

	var writer io.Writer = os.Stderr
	if opts.File != "" {
		lj := &lumberjack.Logger{
			Filename:   opts.File,
			MaxSize:    opts.MaxSize,
			MaxBackups: opts.MaxBackups,
			MaxAge:     opts.MaxAge,
			Compress:   opts.Compress,
			LocalTime:  true,
		}
		if opts.AlsoToStderr {
			writer = io.MultiWriter(lj, os.Stderr)
		} else {
			writer = lj
		}
	}

	if opts.Format == "json" {
		return slog.New(slog.NewJSONHandler(writer, &hopts)), nil
	}
	return slog.New(slog.NewTextHandler(writer, &hopts)), nil
}
