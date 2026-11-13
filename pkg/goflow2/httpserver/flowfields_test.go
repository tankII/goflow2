package httpserver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFlowFieldDescCoversProducerFields 确保释义表覆盖 producer 内置的全部
// 输出字段名。输出名取自 pb/flow.proto，新增字段时需同步补释义。
func TestFlowFieldDescCoversProducerFields(t *testing.T) {
	// pb/flow.proto 中定义的全部 protobuf 字段（输出记录列）
	builtin := []string{
		"as_path", "bgp_communities", "bgp_next_hop", "bytes", "dst_addr", "dst_as",
		"dst_mac", "dst_net", "dst_port", "dst_vlan", "etype", "forwarding_status",
		"fragment_id", "fragment_offset", "icmp_code", "icmp_type", "in_if", "ip_flags",
		"ip_tos", "ip_ttl", "ipv6_flow_label", "ipv6_routing_header_addresses",
		"ipv6_routing_header_seg_left", "layer_size", "layer_stack", "mpls_ip",
		"mpls_label", "mpls_ttl", "next_hop", "next_hop_as", "observation_domain_id",
		"observation_point_id", "out_if", "packets", "proto", "sampler_address",
		"sampling_rate", "sequence_num", "src_addr", "src_as", "src_mac", "src_net",
		"src_port", "src_vlan", "tcp_flags", "time_flow_end_ns", "time_flow_start_ns",
		"time_received_ns", "type", "vlan_id",
	}
	for _, name := range builtin {
		require.NotEmpty(t, FlowFieldDescription(name), "内置输出字段 %q 缺少中文释义", name)
	}
}

// TestFlowFieldDescCoversCustomColumns 覆盖现网 mapping.yaml 的自定义列。
func TestFlowFieldDescCoversCustomColumns(t *testing.T) {
	custom := []string{"icmp_name", "csum", "bi_flow_direction", "ingress_vrf_id", "egress_vrf_id"}
	for _, name := range custom {
		require.NotEmpty(t, FlowFieldDescription(name), "自定义列 %q 缺少中文释义", name)
	}
}

// TestFlowFieldDescUnknown 未知字段返回空串，前端据此显示「—」，不编造含义。
func TestFlowFieldDescUnknown(t *testing.T) {
	require.Empty(t, FlowFieldDescription("definitely_not_a_real_field"))
}
