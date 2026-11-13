package httpserver

// flowFieldDesc 为输出字段名补充中文释义。
//
// 输出字段名来自 mapping.yaml 的 formatter.fields，可由用户任意重命名，
// 因此无法从名称反推含义——这里按 producer 内置的字段名维护一张释义表，
// 并收录现网自定义列。未收录的字段显示为「—」，不编造含义。
//
// 内置字段名取自 pb/flow.proto 的 protobuf 字段；现网自定义列取自
// .deploy/conf/mapping.yaml。
var flowFieldDesc = map[string]string{
	// ── 基础与时间 ──
	"type":               "流记录类型",
	"time_received_ns":   "采集器收到该报文的时刻（纳秒时间戳）",
	"sequence_num":       "NetFlow 报文序列号，用于检测丢包",
	"sampling_rate":      "采样率配置（源自模板的 samplingInterval）",
	"flow_direction":     "流方向（1=入向，2=出向）",
	"sampler_address":    "采样器地址",
	"time_flow_start_ns": "流开始时刻（纳秒时间戳）",
	"time_flow_end_ns":   "流结束时刻（纳秒时间戳）",

	// ── 计数 ──
	"bytes":   "流字节数",
	"packets": "流报文数",

	// ── 三层地址与掩码 ──
	"src_addr":        "源 IP 地址",
	"dst_addr":        "目的 IP 地址",
	"src_net":         "源网段",
	"dst_net":         "目的网段",
	"next_hop":        "下一跳 IP 地址",
	"next_hop_as":     "下一跳 AS 号",
	"src_as":          "源 AS 号",
	"dst_as":          "目的 AS 号",
	"as_path":         "AS 路径",
	"bgp_next_hop":    "BGP 下一跳地址",
	"bgp_communities": "BGP 社区属性",

	// ── 传输层 ──
	"src_port":  "源端口",
	"dst_port":  "目的端口",
	"proto":     "IP 协议号",
	"etype":     "以太网类型",
	"icmp_type": "ICMP 类型",
	"icmp_code": "ICMP 编码",

	// ── 接口与二层 ──
	"in_if":    "入接口索引（SNMP ifIndex）",
	"out_if":   "出接口索引（SNMP ifIndex）",
	"src_mac":  "源 MAC 地址",
	"dst_mac":  "目的 MAC 地址",
	"src_vlan": "源 VLAN ID",
	"dst_vlan": "目的 VLAN ID",
	"vlan_id":  "VLAN ID",

	// ── IP 头字段 ──
	"ip_tos":            "IP ToS（DSCP/ECN）",
	"ip_ttl":            "IP TTL",
	"ip_flags":          "IP 标志位",
	"tcp_flags":         "TCP 标志位",
	"forwarding_status": "转发状态位",
	"fragment_id":       "分片标识",
	"fragment_offset":   "分片偏移",
	"ipv6_flow_label":   "IPv6 流标签",

	// ── MPLS / 隧道 ──
	"mpls_ttl":                      "MPLS 栈 TTL",
	"mpls_label":                    "MPLS 标签栈",
	"mpls_ip":                       "MPLS 封装 IP",
	"layer_stack":                   "封装层栈",
	"layer_size":                    "各层长度",
	"ipv6_routing_header_addresses": "IPv6 路由头地址列表",
	"ipv6_routing_header_seg_left":  "IPv6 路由头剩余段数",

	// ── 观测点 ──
	"observation_domain_id": "观测域 ID",
	"observation_point_id":  "观测点 ID",

	// ── 现网自定义列（.deploy/conf/mapping.yaml）──
	"icmp_name":         "ICMP 类型名称（虚拟列，由 icmp_type 派生）",
	"csum":              "UDP/TCP 校验和",
	"bi_flow_direction": "双向流方向（源自 IPFIX 字段 239）",
	"ingress_vrf_id":    "入方向 VRF ID（源自 IPFIX 字段 234）",
	"egress_vrf_id":     "出方向 VRF ID（源自 IPFIX 字段 235）",
}

// FlowFieldDescription 返回输出字段的中文释义，未收录时返回空串。
// 供 app 层填充 FlowField.Desc——producer 包不依赖 httpserver，
// 故释义表放在此包、由上层注入。
func FlowFieldDescription(name string) string {
	return flowFieldDesc[name]
}
