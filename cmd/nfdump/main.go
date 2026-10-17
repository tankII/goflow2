// nfdump 从十六进制（tcpdump -x 的负载）解码 NetFlow v9/IPFIX 报文，
// 打印模板字段清单，用于确认设备是否上报 FIRST/LAST_SWITCHED 及其粒度。
package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"github.com/netsampler/goflow2/v3/utils/store/templates"
)

func main() {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	var raw []byte
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		line = strings.ReplaceAll(line, " ", "")
		if i := strings.Index(line, "0x"); i >= 0 {
			line = strings.ReplaceAll(line[i:], "0x", "")
		}
		line = strings.Trim(line, "|:")
		if len(line)%2 == 0 && line != "" {
			if b, err := hex.DecodeString(line); err == nil {
				raw = append(raw, b...)
			}
		}
	}
	if len(raw) < 20 {
		fmt.Fprintln(os.Stderr, "输入的十六进制数据不足一个 NetFlow 报文头（20 字节）")
		os.Exit(2)
	}

	version := binary16(raw[0:2])
	store := templates.NewTemplateFlowStore()
	ctx := netflow.FlowContext{RouterKey: "dump"}

	var sets []interface{}
	var err error
	if version == 9 {
		pkt := &netflow.NFv9Packet{}
		// DecodeMessageVersion 会先读 2 字节 version，再解析余下报文头与流集
		err = netflow.DecodeMessageVersion(bytes.NewBuffer(raw), store, ctx, pkt, nil)
		sets = pkt.FlowSets
		uptime := pkt.SystemUptime
		defer func() {
			fmt.Printf("NetFlow v9  obsDomain=%d  sysUptime=%dms(=%.3fs, 毫秒位=%d)  unixSeconds=%d\n\n",
				pkt.SourceId, uptime, float64(uptime)/1000, uptime%1000, pkt.UnixSeconds)
		}()
	} else {
		fmt.Fprintf(os.Stderr, "目前仅支持 NetFlow v9（版本=%d）\n", version)
		os.Exit(3)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "解码告警:", err)
	}

	shown := false
	for _, s := range sets {
		ts, ok := s.(netflow.TemplateFlowSet)
		if !ok {
			continue
		}
		shown = true
		for _, r := range ts.Records {
			fmt.Printf("模板 ID=%d  字段数=%d\n", r.TemplateId, r.FieldCount)
			var names []string
			hasTiming := false
			for _, f := range r.Fields {
				n := fieldName(f.Type)
				names = append(names, fmt.Sprintf("%s(len=%d)", n, f.Length))
				if f.Type == netflow.NFV9_FIELD_FIRST_SWITCHED || f.Type == netflow.NFV9_FIELD_LAST_SWITCHED {
					hasTiming = true
				}
			}
			fmt.Printf("   %s\n", strings.Join(names, ", "))
			if hasTiming {
				fmt.Printf("   → 含 FIRST/LAST_SWITCHED：输出时间是否带毫秒，取决于这些值 %% 1000 是否为 0\n")
			} else {
				fmt.Printf("   → ✗ 不含时间字段：goflow2 用报文头 unixSeconds（整秒），毫秒必然为 0\n")
			}
			fmt.Println()
		}
	}
	if !shown {
		fmt.Println("本次抓包只含数据/选项集，未含模板流集。请先抓模板刷新窗口的包，或累积多个包后再执行。")
		fmt.Println("（模板通常每 N 个包或每 N 分钟重发一次）")
	}
}

// fieldName 返回常见 NetFlow v9 字段名，未知字段回退为 "field_<type>"。
func fieldName(t uint16) string {
	if n, ok := knownFields[t]; ok {
		return n
	}
	return fmt.Sprintf("field_%d", t)
}

var knownFields = map[uint16]string{
	1: "IN_BYTES", 2: "IN_PKTS", 4: "PROTOCOL", 7: "L4_SRC_PORT", 8: "L4_DST_PORT",
	10: "INPUT_SNMP", 11: "OUTPUT_SNMP", 12: "SRC_AS", 13: "DST_AS", 14: "SRC_IPV4",
	15: "DST_IPV4", 21: "LAST_SWITCHED", 22: "FIRST_SWITCHED", 27: "SRC_MAC",
	31: "SRC_VLAN", 32: "DST_VLAN", 34: "SRC_TOS", 61: "FLOW_DIRECTION",
}

func binary16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }
func binary32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
