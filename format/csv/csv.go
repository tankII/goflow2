// Package csv implements a configurable CSV output formatter.
package csv

import (
	"flag"
	"fmt"
	"strings"
	"sync"

	"github.com/netsampler/goflow2/v3/format"
)

// CSVDriver formats flow messages as CSV rows with a configurable separator.
type CSVDriver struct {
	delimiter string
}

// csvRowSerials are exposed by messages that can render a CSV row.
type csvRowSerials interface {
	CSVValues() []string
}

// csvRowAppender 由可直接把转义后的 CSV 行渲染进字节缓冲的消息实现，
// 从而省去中间 []string 与 strings.Join 的整行拷贝；存在该接口时优先使用。
type csvRowAppender interface {
	AppendCSVRow(dst []byte, delim string) []byte
}

// csvScratch 复用行构建缓冲区。Format 会被所有 worker 并发调用，
// 因此缓冲必须"每次调用获取/归还"，而不能挂在驱动器上共享。
var csvScratch = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}

// Prepare binds the driver flags.
func (d *CSVDriver) Prepare() error {
	flag.StringVar(&d.delimiter, "format.csv.delimiter", "|", "Delimiter for CSV output, separated by a single character")
	return nil
}

// Init validates the runtime configuration.
func (d *CSVDriver) Init() error {
	if len([]rune(d.delimiter)) != 1 {
		return fmt.Errorf("csv format: delimiter must be a single character")
	}
	return nil
}

// csvEscape returns the RFC 4180 representation of a single CSV field.
func csvEscape(v string, delim string) string {
	needsQuote := strings.Contains(v, delim) || strings.ContainsAny(v, "\"\r\n")
	if !needsQuote && len(v) > 0 {
		if c := v[0]; c == ' ' || c == '\t' {
			needsQuote = true
		} else if c := v[len(v)-1]; c == ' ' || c == '\t' {
			needsQuote = true
		}
	}
	if !needsQuote {
		return v
	}
	return "\"" + strings.ReplaceAll(v, "\"", "\"\"") + "\""
}

// Format marshals the payload into a single CSV row. The row carries no
// trailing newline: transports own record separation via their separator flag.
func (d *CSVDriver) Format(data interface{}) ([]byte, []byte, error) {
	var key []byte
	if dataIf, ok := data.(interface{ Key() []byte }); ok {
		key = dataIf.Key()
	}

	if ap, ok := data.(csvRowAppender); ok {
		bp := csvScratch.Get().(*[]byte)
		row := ap.AppendCSVRow((*bp)[:0], d.delimiter)
		*bp = row
		// 传输层会持有返回的切片（netcat 会把它入队），因此必须返回一份独占的副本，
		// 并把池化缓冲归还。
		out := make([]byte, len(row))
		copy(out, row)
		csvScratch.Put(bp)
		return key, out, nil
	}

	rows, ok := data.(csvRowSerials)
	if !ok {
		return key, nil, format.ErrNoSerializer
	}

	values := rows.CSVValues()
	row := make([]string, len(values))
	for i, v := range values {
		row[i] = csvEscape(v, d.delimiter)
	}

	return key, []byte(strings.Join(row, d.delimiter)), nil
}

func init() {
	d := &CSVDriver{}
	format.RegisterFormatDriver("csv", d)
}
