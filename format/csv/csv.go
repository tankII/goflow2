// Package csv implements a configurable CSV output formatter.
package csv

import (
	"flag"
	"fmt"
	"strings"

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

// Format marshals the payload into a single CSV row.
func (d *CSVDriver) Format(data interface{}) ([]byte, []byte, error) {
	var key []byte
	if dataIf, ok := data.(interface{ Key() []byte }); ok {
		key = dataIf.Key()
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

	return key, []byte(strings.Join(row, d.delimiter) + "\n"), nil
}

func init() {
	d := &CSVDriver{}
	format.RegisterFormatDriver("csv", d)
}
