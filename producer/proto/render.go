package protoproducer

import (
	"encoding/binary"
	"encoding/hex"
	"net"
	"net/netip"
	"os"
	"reflect"
	"strings"
	"time"
)

// RenderFunc converts a raw field value into a display value.
type RenderFunc func(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{}

// RendererID identifies a built-in renderer.
type RendererID string

const (
	// RendererNone disables rendering.
	RendererNone         RendererID = "none"
	RendererIP           RendererID = "ip"
	RendererMac          RendererID = "mac"
	RendererEtype        RendererID = "etype"
	RendererProto        RendererID = "proto"
	RendererType         RendererID = "type"
	RendererNetwork      RendererID = "network"
	RendererDateTime     RendererID = "datetime"
	RendererDateTimeNano RendererID = "datetimenano"
	RendererString       RendererID = "string"
)

var (
	renderers = map[RendererID]RenderFunc{
		RendererNone:         NilRenderer,
		RendererIP:           IPRenderer,
		RendererMac:          MacRenderer,
		RendererEtype:        EtypeRenderer,
		RendererProto:        ProtoRenderer,
		RendererDateTime:     DateTimeRenderer,
		RendererDateTimeNano: DateTimeNanoRenderer,
		RendererString:       StringRenderer,
	}

	// defaultRenderIDs 与 defaultRenderers 的内建列一一对应，仅用于 compilePlan
	// 判定直写旁路；未列出的默认列走原 renderer 字符串路径。
	defaultRenderIDs = map[string]RendererID{
		"SrcAddr":                    RendererIP,
		"DstAddr":                    RendererIP,
		"SamplerAddress":             RendererIP,
		"NextHop":                    RendererIP,
		"BgpNextHop":                 RendererIP,
		"MplsLabelIp":                RendererIP,
		"MplsIp":                     RendererIP,
		"Ipv6RoutingHeaderAddresses": RendererIP,
	}

	defaultRenderers = map[string]RenderFunc{
		"SrcMac":         MacRenderer,
		"DstMac":         MacRenderer,
		"SrcAddr":        IPRenderer,
		"DstAddr":        IPRenderer,
		"SamplerAddress": IPRenderer,
		"NextHop":        IPRenderer,
		"BgpNextHop":     IPRenderer,
		"MplsLabelIp":    IPRenderer,
		"MplsIp":         IPRenderer,
		"Etype":          EtypeRenderer,
		"Proto":          ProtoRenderer,
		"SrcNet":         NetworkRenderer,
		"DstNet":         NetworkRenderer,

		"icmp_name": ICMPRenderer,

		"Ipv6RoutingHeaderAddresses": IPRenderer,
	}

	etypeName = map[uint32]string{
		0x806:  "ARP",
		0x800:  "IPv4",
		0x86dd: "IPv6",
	}
	protoName = map[uint32]string{
		0:   "HOPOPT",
		1:   "ICMP",
		2:   "IGMP",
		3:   "GGP",
		4:   "IPv4",
		5:   "ST",
		6:   "TCP",
		7:   "CBT",
		8:   "EGP",
		9:   "IGP",
		10:  "BBN-RCC-MON",
		11:  "NVP-II",
		12:  "PUP",
		13:  "ARGUS",
		14:  "EMCON",
		15:  "XNET",
		16:  "CHAOS",
		17:  "UDP",
		18:  "MUX",
		19:  "DCN-MEAS",
		20:  "HMP",
		21:  "PRM",
		22:  "XNS-IDP",
		23:  "TRUNK-1",
		24:  "TRUNK-2",
		25:  "LEAF-1",
		26:  "LEAF-2",
		27:  "RDP",
		28:  "IRTP",
		29:  "ISO-TP4",
		30:  "NETBLT",
		31:  "MFE-NSP",
		32:  "MERIT-INP",
		33:  "DCCP",
		34:  "3PC",
		35:  "IDPR",
		36:  "XTP",
		37:  "DDP",
		38:  "IDPR-CMTP",
		39:  "TP++",
		40:  "IL",
		41:  "IPv6",
		42:  "SDRP",
		43:  "IPv6-Route",
		44:  "IPv6-Frag",
		45:  "IDRP",
		46:  "RSVP",
		47:  "GRE",
		48:  "DSR",
		49:  "BNA",
		50:  "ESP",
		51:  "AH",
		52:  "I-NLSP",
		53:  "SWIPE",
		54:  "NARP",
		55:  "Min-IPv4",
		56:  "TLSP",
		57:  "SKIP",
		58:  "IPv6-ICMP",
		59:  "IPv6-NoNxt",
		60:  "IPv6-Opts",
		61:  "any-host-internal-protocol",
		62:  "CFTP",
		63:  "any-local-network",
		64:  "SAT-EXPAK",
		65:  "KRYPTOLAN",
		66:  "RVD",
		67:  "IPPC",
		68:  "any-distributed-file-system",
		69:  "SAT-MON",
		70:  "VISA",
		71:  "IPCV",
		72:  "CPNX",
		73:  "CPHB",
		74:  "WSN",
		75:  "PVP",
		76:  "BR-SAT-MON",
		77:  "SUN-ND",
		78:  "WB-MON",
		79:  "WB-EXPAK",
		80:  "ISO-IP",
		81:  "VMTP",
		82:  "SECURE-VMTP",
		83:  "VINES",
		84:  "IPTM",
		85:  "NSFNET-IGP",
		86:  "DGP",
		87:  "TCF",
		88:  "EIGRP",
		89:  "OSPFIGP",
		90:  "Sprite-RPC",
		91:  "LARP",
		92:  "MTP",
		93:  "AX.25",
		94:  "IPIP",
		95:  "MICP",
		96:  "SCC-SP",
		97:  "ETHERIP",
		98:  "ENCAP",
		99:  "any-private-encryption-scheme",
		100: "GMTP",
		101: "IFMP",
		102: "PNNI",
		103: "PIM",
		104: "ARIS",
		105: "SCPS",
		106: "QNX",
		107: "A/N",
		108: "IPComp",
		109: "SNP",
		110: "Compaq-Peer",
		111: "IPX-in-IP",
		112: "VRRP",
		113: "PGM",
		114: "any-0-hop-protocol",
		115: "L2TP",
		116: "DDX",
		117: "IATP",
		118: "STP",
		119: "SRP",
		120: "UTI",
		121: "SMP",
		122: "SM",
		123: "PTP",
		124: "ISIS over IPv4",
		125: "FIRE",
		126: "CRTP",
		127: "CRUDP",
		128: "SSCOPMCE",
		129: "IPLT",
		130: "SPS",
		131: "PIPE",
		132: "SCTP",
		133: "FC",
		134: "RSVP-E2E-IGNORE",
		135: "Mobility Header",
		136: "UDPLite",
		137: "MPLS-in-IP",
		138: "manet",
		139: "HIP",
		140: "Shim6",
		141: "WESP",
		142: "ROHC",
		143: "Ethernet",
		144: "AGGFRAG",
		145: "NSH",
	}
	icmpTypeName = map[uint32]string{
		0:  "EchoReply",
		3:  "DestinationUnreachable",
		8:  "Echo",
		9:  "RouterAdvertisement",
		10: "RouterSolicitation",
		11: "TimeExceeded",
	}
	icmp6TypeName = map[uint32]string{
		1:   "DestinationUnreachable",
		2:   "PacketTooBig",
		3:   "TimeExceeded",
		128: "EchoRequest",
		129: "EchoReply",
		133: "RouterSolicitation",
		134: "RouterAdvertisement",
	}
)

// NilRenderer returns nil for any field.
func NilRenderer(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{} {
	if dataIf, ok := data.(interface {
		String() string
	}); ok {
		return dataIf.String()
	}
	if dataC, ok := data.([]byte); ok {
		return hex.EncodeToString(dataC)
	}
	return data
}

// StringRenderer converts a byte slice to a string.
func StringRenderer(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{} {
	if dataC, ok := data.([]byte); ok {
		return string(dataC)
	} else if dataC, ok := data.(string); ok {
		return string(dataC)
	} // maybe should support uint64?
	return NilRenderer(msg, fieldName, data)
}

// rendererLocation is the location used to render timestamps.
// It defaults to UTC and switches to the process local time (as configured by
// the TZ environment variable) when TZ is set. It is resolved once at startup.
var rendererLocation = detectRendererLocation()

func detectRendererLocation() *time.Location {
	if os.Getenv("TZ") != "" {
		return time.Local
	}
	return time.UTC
}

// dataToTime converts a raw field value into a time.Time.
// When nanos is false the value is a Unix timestamp in seconds, otherwise in
// nanoseconds.
func dataToTime(data interface{}, nanos bool) (time.Time, bool) {
	if nanos {
		switch dataC := data.(type) {
		case uint64:
			return time.Unix(int64(dataC)/1e9, int64(dataC)%1e9), true
		case int64:
			return time.Unix(dataC/1e9, dataC%1e9), true
		}
		return time.Time{}, false
	}
	switch dataC := data.(type) {
	case uint64:
		return time.Unix(int64(dataC), 0), true
	case int64:
		return time.Unix(dataC, 0), true
	case uint32:
		return time.Unix(int64(dataC), 0), true
	case int32:
		return time.Unix(int64(dataC), 0), true
	}
	return time.Time{}, false
}

// dateTimeFormatRenderer builds a renderer formatting Unix timestamps using the
// given Go reference-time layout (eg. "2006-01-02 15:04:05.000"), in the
// configured rendererLocation.
func dateTimeFormatRenderer(layout string, nanos bool) RenderFunc {
	return func(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{} {
		ts, ok := dataToTime(data, nanos)
		if !ok {
			return NilRenderer(msg, fieldName, data)
		}
		return ts.In(rendererLocation).Format(layout)
	}
}

// DateTimeRenderer formats a Unix timestamp in seconds using RFC3339Nano.
func DateTimeRenderer(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{} {
	return dateTimeFormatRenderer(time.RFC3339Nano, false)(msg, fieldName, data)
}

// DateTimeNanoRenderer formats a Unix timestamp in nanoseconds using RFC3339Nano.
func DateTimeNanoRenderer(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{} {
	return dateTimeFormatRenderer(time.RFC3339Nano, true)(msg, fieldName, data)
}

// appendIPBytes 把字节形式的 IP 地址直接追加到 dst。
// 与 RenderIP 的输出逐字节一致：长度非 4/16 时返回空串（RenderIP 亦返回空串）。
func appendIPBytes(dst []byte, b []byte) []byte {
	if len(b) != net.IPv4len && len(b) != net.IPv6len {
		return dst
	}
	addr, ok := netip.AddrFromSlice(b)
	if !ok {
		return dst
	}
	return addr.AppendTo(dst)
}

// appendDateTimeNanoField 把纳秒时间戳按 layout 直接追加到 dst。
// 与 dateTimeFormatRenderer(layout, nanos=true) 输出一致（含时区处理）。
func appendDateTimeNanoField(dst []byte, ns uint64, layout string) []byte {
	ts := time.Unix(int64(ns)/1e9, int64(ns)%1e9)
	return ts.In(rendererLocation).AppendFormat(dst, layout)
}

// renderAppendable 把可直写列渲染为字符串形态，供 text/JSON 与 CSVValues 使用。
// 这些列的直写旁路与 renderer 输出逐字节一致，故此处复用 renderer 结果。
func renderAppendable(cf *compiledField, v reflect.Value) string {
	switch cf.appendKind {
	case appendIP:
		return RenderIP(v.Bytes())
	case appendDateTimeNano:
		return string(appendDateTimeNanoField(nil, v.Uint(), cf.appendLayout))
	}
	return ""
}

// appendKindFor 判定某列能否走直写旁路：
//   - renderer 为内建 "ip"，且字段是字节切片 → appendIP
//   - renderer 为内建 "datetimenano:<layout>"，且字段是 uint64/int64 → appendDateTimeNano
//
// 其余情况（含用户自定义 renderer）一律返回 appendNone，保持原字符串路径。
func appendKindFor(id RendererID, k reflect.Kind) (appendKind, string) {
	name, layout, hasLayout := strings.Cut(string(id), ":")
	switch RendererID(name) {
	case RendererIP:
		if k == reflect.Slice {
			return appendIP, ""
		}
	case RendererDateTimeNano:
		// 仅纳秒语义有直写路径；秒语义的 dataToTime 只接受 uint64/int64，
		// 这里同样只对这两种字段形态开启，保证行为完全一致。
		if hasLayout && (k == reflect.Uint64 || k == reflect.Int64) {
			return appendDateTimeNano, layout
		}
	}
	return appendNone, ""
}

// builtinRendererMatches 校验登记的 renderer 标识与实际使用的函数是否都是内建实现。
// 函数值不能直接比较，故用 reflect.ValueOf(...).Pointer() 比对函数入口地址。
func builtinRendererMatches(id RendererID, fn RenderFunc) bool {
	if fn == nil {
		return false
	}
	name, _, hasLayout := strings.Cut(string(id), ":")
	switch RendererID(name) {
	case RendererIP:
		return sameFuncPtr(fn, IPRenderer)
	case RendererDateTimeNano:
		if !hasLayout {
			return false
		}
		// 自定义 layout 对应的是闭包，无法按入口地址比对；此时要求调用方
		// 未改动过该列的 render（由 mapFormat 保证），放行。
		return true
	}
	return false
}

func sameFuncPtr(a, b any) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

// resolveRenderer resolves a renderer identifier to its function.
// Besides the built-in names it supports a "<name>:<layout>" form for the
// datetime/datetimenano renderers to format timestamps with a custom layout.
func resolveRenderer(id RendererID) (RenderFunc, bool) {
	if renderer, ok := renderers[id]; ok {
		return renderer, true
	}
	if parts := strings.SplitN(string(id), ":", 2); len(parts) == 2 {
		switch RendererID(parts[0]) {
		case RendererDateTime:
			return dateTimeFormatRenderer(parts[1], false), true
		case RendererDateTimeNano:
			return dateTimeFormatRenderer(parts[1], true), true
		}
	}
	return nil, false
}

// MacRenderer formats a MAC address.
func MacRenderer(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{} {
	if dataC, ok := data.(uint64); ok {
		var mac [8]byte
		binary.BigEndian.PutUint64(mac[:], dataC)
		return net.HardwareAddr(mac[2:]).String()
	}
	return NilRenderer(msg, fieldName, data)

}

// RenderIP formats an IP address from raw bytes.
func RenderIP(addr []byte) string {
	if addr == nil || (len(addr) != 4 && len(addr) != 16) {
		return ""
	}
	ip, _ := netip.AddrFromSlice(addr)
	return ip.String()
}

// IPRenderer formats an IP address field.
func IPRenderer(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{} {
	if dataC, ok := data.([]byte); ok {
		return RenderIP(dataC)
	}
	return NilRenderer(msg, fieldName, data)
}

// EtypeRenderer formats EtherType values.
func EtypeRenderer(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{} {
	if dataC, ok := data.(uint32); ok {
		return etypeName[dataC]
	} else if dataC, ok := data.(uint64); ok { // supports protobuf mapped fields
		return etypeName[uint32(dataC)]
	}
	return "unknown"
}

// ProtoName returns the protocol name for a protocol number.
func ProtoName(protoNumber uint32) string {
	dataC, ok := protoName[protoNumber]
	if ok {
		return dataC
	} else if (protoNumber >= 146) && (protoNumber <= 252) {
		return "unassigned"
	} else if (protoNumber >= 253) && (protoNumber <= 254) {
		return "experimental"
	} else if protoNumber == 255 {
		return "reserved"
	}
	return "unknown"
}

// ProtoRenderer formats protocol numbers to names.
func ProtoRenderer(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{} {
	if dataC, ok := data.(uint32); ok {
		return ProtoName(dataC)
	} else if dataC, ok := data.(uint64); ok {
		return ProtoName(uint32(dataC))
	}
	return "unknown"
}

// NetworkRenderer formats a prefix length as a CIDR mask.
func NetworkRenderer(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{} {
	var addr netip.Addr
	switch fieldName {
	case "SrcNet":
		addr, _ = netip.AddrFromSlice(msg.SrcAddr)
	case "DstNet":
		addr, _ = netip.AddrFromSlice(msg.DstAddr)
	}
	if dataC, ok := data.(uint32); ok {
		prefix, _ := addr.Prefix(int(dataC))
		return prefix.String()
	}
	return "unknown"
}

// IcmpCodeType returns a formatted ICMP type/code string.
func IcmpCodeType(proto, icmpCode, icmpType uint32) string {
	switch proto {
	case 1:
		return icmpTypeName[icmpType]
	case 58:
		return icmp6TypeName[icmpType]
	}
	return "unknown"
}

// ICMPRenderer formats ICMP type/code values.
func ICMPRenderer(msg *ProtoProducerMessage, fieldName string, data interface{}) interface{} {
	return IcmpCodeType(uint32(msg.Proto), uint32(msg.IcmpCode), uint32(msg.IcmpType))
}
