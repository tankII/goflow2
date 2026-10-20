package httpserver

import (
	"fmt"
	"strings"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
)

// fieldNameZh 为 NetFlow v9 / IPFIX 字段类型号补充中文释义。
//
// 英文名取自解码器内置名字表（netflow.NFv9TypeToString /
// netflow.IPFIXTypeToString），本表只负责回答「这个字段是什么意思」。
//
// 释义一律以 RFC 3954（NetFlow v9）与 RFC 5102（IPFIX）为准。
// 注意两点容易写错的地方：
//  1. SRC_MASK/DST_MASK 一族是「前缀长度（位数）」而非掩码本身；
//  2. 编号在 RFC 中属保留段、但 Cisco 等厂商给了具体含义的字段
//     （89/92/93/98 等），已在释义中标注，不冒充标准定义。
//
// 覆盖范围以现网实际导出的字段类型为准，并补充了一批常见保留字段；
// 未收录的类型回退为英文名，不会让界面出现空白。
var fieldNameZh = map[uint16]string{
	// 计数器
	1:  "入向字节数",
	2:  "入向报文数",
	3:  "聚合的流数",
	23: "出向字节数",
	24: "出向报文数",

	// 报文长度。25/26 在 RFC 3954 中属保留段，以下含义取自 Cisco 文档
	25: "最小 IP 总长（厂商定义，RFC 保留段）",
	26: "最大 IP 总长（厂商定义，RFC 保留段）",

	// 协议与端口
	4:  "IP 协议号",
	7:  "源传输层端口",
	11: "目的传输层端口",
	32: "ICMP 类型（值为 ICMP 类型×256 + ICMP 编码）",
	33: "组播 IGMP 类型",
	60: "IP 协议版本",

	// 时间。
	// RFC 3954 原文：LAST_SWITCHED「sysUptime in msec at which the last packet
	// of this Flow was switched」。即它是「本次流最后一个包被交换时的
	// uptime 时刻（毫秒）」，既不是墙上时钟，也不同于报文头的 sysUpTime 字段本身。
	// 采集端按 导出墙钟时间 - (sysUpTime - 该字段值) 换算成绝对时间。
	21: "流结束时刻（最后一个包被交换时的 sysUptime 毫秒值）",
	22: "流开始时刻（第一个包被交换时的 sysUptime 毫秒值）",

	// 超时。RFC 3954：单位为秒
	36: "流活动超时（秒）",
	37: "流非活动超时（秒）",

	// 前缀长度。RFC 5102 §5.4.4：number of contiguous bits，范围 0-32（IPv6 为 0-128）
	// 注意是「位数」而不是掩码本身
	9:  "源 IPv4 前缀长度（0–32 位）",
	13: "目的 IPv4 前缀长度（0–32 位）",
	29: "源 IPv6 前缀长度（0–128 位）",
	30: "目的 IPv6 前缀长度（0–128 位）",

	// IP 地址与掩码
	8:  "源 IPv4 地址",
	12: "目的 IPv4 地址",
	10: "入接口 SNMP ifIndex",
	14: "出接口 SNMP ifIndex",
	15: "IPv4 下一跳地址",
	18: "BGP IPv4 下一跳地址",
	27: "源 IPv6 地址",
	28: "目的 IPv6 地址",
	31: "IPv6 流量标签",
	62: "IPv6 下一跳地址",
	63: "BGP IPv6 下一跳地址",

	// 服务质量 / 分类
	5: "源 ToS（入接口处的 DSCP/ECN）",
	6: "TCP 标志位（流内累计）",
	// RFC 3954 与 RFC 5102 §5.11.6 均定义为 0=ingress / 1=egress；
	// Cisco 产品文档则写 1=ingress / 2=egress。两种口径都列出，避免误判。
	61: "流方向（RFC：0=入向 1=出向；Cisco 文档：1=入向 2=出向）",

	// VLAN。RFC 3954：58 关联 ingress interface，59 关联 egress interface
	58: "源 VLAN ID（关联入接口）",
	59: "目的 VLAN ID（关联出接口）",

	// 以下编号在 RFC 中属保留段，含义取自 Cisco 文档
	89: "转发状态位（厂商定义，RFC 保留段）",
	92: "源流量索引 / QoS 队列（厂商定义，RFC 保留段）",
	93: "目的流量索引 / QoS 队列（厂商定义，RFC 保留段）",
	98: "出向 IP 的 DSCP（厂商定义，RFC 保留段）",

	// 自治域
	16: "源 AS 号",
	17: "目的 AS 号",

	// 二层 MAC。
	// RFC 3954 称 SRC_MAC(56)/DST_MAC(57)，Cisco 文档称 IN_SRC_MAC/OUT_DST_MAC，
	// 编号对应同一含义；IN_/OUT_ 前缀表示该地址是在入方向还是出方向观测到的。
	56: "源 MAC（入方向观测）",
	57: "目的 MAC（出方向观测）",
	80: "目的 MAC（入方向观测）",
	81: "源 MAC（出方向观测）",

	// MPLS
	46: "MPLS 顶层标签类型",
	47: "MPLS 顶层标签 IP 地址",
	70: "MPLS 标签栈第 1 层",
	71: "MPLS 标签栈第 2 层",
	72: "MPLS 标签栈第 3 层",
	73: "MPLS 标签栈第 4 层",

	// 采样。
	// RFC 3954：值 100 表示「每 100 个包抽 1 个」；Cisco 文档：值为 2^n。
	// 两种口径在业界同时存在，故并列说明。
	34: "采样间隔（RFC：1/N 抽样；Cisco 文档：2^N）",
	35: "采样算法（1=确定性抽样，2=随机抽样）",
	48: "采样器 ID",
	49: "采样器模式",

	// 转发引擎
	38: "流交换引擎类型（如路由处理器、线卡）",
	39: "流交换引擎 ID",

	// 厂商私有。RFC 3954 将 43-45、51-54 列为保留段
	43: "厂商私有字段（RFC 保留段）",
	51: "厂商私有字段（RFC 保留段）",
}

// fieldTypeName 返回字段类型号对应的英文名。
// 名字表未收录时返回空字符串，由调用方决定如何标注。
func fieldTypeName(version uint16, typeID uint16) string {
	if version == 10 {
		return netflow.IPFIXTypeToString(typeID)
	}
	return netflow.NFv9TypeToString(typeID)
}

// fieldDescription 返回字段的英文名与中文释义。
//
// 解码器的名字表对未收录的类型返回 "Unassigned"，表内有空隙时返回空串；
// 两者都表示「没有可用的标准名」，此时改为带 type 号的占位说明，
// 避免界面上出现一排看不出含义的 Unassigned。
func fieldDescription(version uint16, typeID uint16) (name string, zh string) {
	name = fieldTypeName(version, typeID)
	zh = fieldNameZh[typeID]

	if name == "" || name == "Unassigned" {
		name = fmt.Sprintf("未收录 (type=%d)", typeID)
	}
	if zh == "" {
		// 英文名表里字面就是厂商私有占位符时，中文单独说明清楚
		if strings.Contains(name, "Vendor Proprietary") {
			zh = "厂商私有字段"
		} else {
			zh = "暂无中文释义"
		}
	}
	return name, zh
}

// uptimeFieldTypes 是以设备 sysUptime 为基准的时间字段。
// 模板含这些字段时，界面需要提示换算方式，避免误读成墙上时钟。
var uptimeFieldTypes = map[uint16]bool{21: true, 22: true}

// hasUptimeField 判断模板是否包含 sysUptime 基准的时间字段。
func hasUptimeField(fields []templateFieldView) bool {
	for _, f := range fields {
		if uptimeFieldTypes[f.Type] {
			return true
		}
	}
	return false
}
