package config

import (
	"flag"
	"fmt"
	"strings"

	"github.com/netsampler/goflow2/v3/format"
	"github.com/netsampler/goflow2/v3/transport"
)

// BindCommonFlags registers shared format/transport flags.
func BindCommonFlags(fs *flag.FlagSet, formatName, transportName *string) {
	fs.StringVar(formatName, "format", "json", fmt.Sprintf("Choose the format (available: %s)", strings.Join(format.GetFormats(), ", ")))
	fs.StringVar(transportName, "transport", "file", fmt.Sprintf("Choose the transport (available: %s)", strings.Join(transport.GetTransports(), ", ")))
}
