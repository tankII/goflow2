package httpserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
)

// templateKeyOf mirrors the packing used by the template store so tests can
// build fixture stores without reaching into unexported helpers.
func templateKeyOf(version uint16, obsDomain uint32, templateID uint16) uint64 {
	return (uint64(version) << 48) | (uint64(obsDomain) << 16) | uint64(templateID)
}

type fakeTemplateStore map[string]netflow.FlowBaseTemplateSet

func (f fakeTemplateStore) GetAll() map[string]netflow.FlowBaseTemplateSet { return f }

func TestFieldDescription(t *testing.T) {
	cases := []struct {
		name     string
		version  uint16
		typeID   uint16
		wantName string
		wantZh   string
	}{
		{
			name: "v9 有英文名且已收录中文",
			// FIRST_SWITCHED 是判定设备模板是否完整的关键字段
			version: 9, typeID: 22,
			wantName: "FIRST_SWITCHED", wantZh: "流开始时刻（第一个包被交换时的 sysUptime 毫秒值）",
		},
		{
			name:    "v9 LAST_SWITCHED",
			version: 9, typeID: 21,
			wantName: "LAST_SWITCHED", wantZh: "流结束时刻（最后一个包被交换时的 sysUptime 毫秒值）",
		},
		{
			name:    "v9 IN_BYTES",
			version: 9, typeID: 1,
			wantName: "IN_BYTES", wantZh: "入向字节数",
		},
		{
			name: "ipfix 走另一张名字表",
			// IPFIX 的 1 是 octetDeltaCount，v9 的 1 是 IN_BYTES，不能串表
			version: 10, typeID: 1,
			wantName: "octetDeltaCount", wantZh: "入向字节数",
		},
		{
			name:    "厂商私有字面名翻译成中文",
			version: 9, typeID: 43,
			wantName: "*Vendor Proprietary*", wantZh: "厂商私有字段（RFC 保留段）",
		},
		{
			name: "名字表未收录时给出带 type 的占位",
			// 现网 type=210 出现过 112 次，名字表里查不到
			version: 9, typeID: 210,
			wantName: "未收录 (type=210)", wantZh: "暂无中文释义",
		},
		{
			name:    "type=0 同样不空白",
			version: 9, typeID: 0,
			wantName: "未收录 (type=0)", wantZh: "暂无中文释义",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotName, gotZh := fieldDescription(tc.version, tc.typeID)
			if gotName != tc.wantName {
				t.Errorf("英文名 = %q, 期望 %q", gotName, tc.wantName)
			}
			if gotZh != tc.wantZh {
				t.Errorf("中文释义 = %q, 期望 %q", gotZh, tc.wantZh)
			}
		})
	}
}

func TestFieldDescriptionNeverBlank(t *testing.T) {
	// 界面不接受空白项：任意类型都必须拿到非空的英文名与中文释义
	for typeID := 0; typeID <= 1024; typeID++ {
		name, zh := fieldDescription(9, uint16(typeID))
		if name == "" || zh == "" {
			t.Fatalf("type=%d 得到空描述: name=%q zh=%q", typeID, name, zh)
		}
	}
}

func TestSplitRouter(t *testing.T) {
	cases := []struct{ in, ip, port string }{
		{"117.185.127.2:2055", "117.185.127.2", "2055"},
		{"10.0.0.1:40000", "10.0.0.1", "40000"},
		{"no-port", "", "no-port"},
	}
	for _, tc := range cases {
		ip, port := splitRouter(tc.in)
		if ip != tc.ip || port != tc.port {
			t.Errorf("splitRouter(%q) = (%q, %q), 期望 (%q, %q)", tc.in, ip, port, tc.ip, tc.port)
		}
	}
}

func TestMatchesRouter(t *testing.T) {
	cases := []struct {
		router, filter string
		want           bool
	}{
		{"117.185.127.2:2055", "", true},                   // 无条件匹配全部
		{"117.185.127.2:2055", "117.185.127.2", true},      // 只给 IP，应命中带端口的 key
		{"117.185.127.2:2055", "117.185.127.2:2055", true}, // 完整写法
		{"117.185.127.2:2055", "117.185.127.9", false},
		{"", "117.1", false},
	}
	for _, tc := range cases {
		if got := matchesRouter(tc.router, tc.filter); got != tc.want {
			t.Errorf("matchesRouter(%q, %q) = %v, 期望 %v", tc.router, tc.filter, got, tc.want)
		}
	}
}

func TestBuildTemplateBrowserDocGroupsByRouter(t *testing.T) {
	store := fakeTemplateStore{
		"117.185.127.2:2055": {
			templateKeyOf(9, 16711688, 270): netflow.TemplateRecord{
				TemplateId: 270,
				FieldCount: 2,
				Fields: []netflow.Field{
					{Type: 22, Length: 4},
					{Type: 21, Length: 4},
				},
			},
		},
		"117.184.216.1:2055": {},
	}

	doc := buildTemplateBrowserDoc(store, "")
	// 空模板集合的设备会被跳过，前端不需要展示空分组
	if len(doc.Routers) != 1 {
		t.Fatalf("设备数 = %d, 期望 1（空模板集合的设备应被跳过）", len(doc.Routers))
	}
	for _, r := range doc.Routers {
		if r.Router == "117.184.216.1:2055" {
			t.Errorf("空模板集合的设备不应出现在结果中")
		}
	}

	first := doc.Routers[0]
	if first.Router != "117.185.127.2:2055" {
		t.Fatalf("首个设备 = %q, 期望按 router 排序后的第一台", first.Router)
	}
	if first.IP != "117.185.127.2" || first.Port != "2055" {
		t.Errorf("ip/port 拆分错误: %q / %q", first.IP, first.Port)
	}
	if len(first.Templates) != 1 {
		t.Fatalf("模板数 = %d, 期望 1", len(first.Templates))
	}

	tpl := first.Templates[0]
	if tpl.Key != "9/16711688/270" {
		t.Errorf("模板键 = %q, 期望 9/16711688/270", tpl.Key)
	}
	if tpl.Version != 9 || tpl.ObsDomain != 16711688 || tpl.TemplateID != 270 {
		t.Errorf("模板元数据解析错误: %+v", tpl)
	}
	if len(tpl.Fields) != 2 {
		t.Fatalf("字段数 = %d, 期望 2", len(tpl.Fields))
	}
	if tpl.Fields[0].Index != 0 || tpl.Fields[0].Name != "FIRST_SWITCHED" {
		t.Errorf("首个字段解析错误: %+v", tpl.Fields[0])
	}
	if tpl.Fields[1].Index != 1 || tpl.Fields[1].Name != "LAST_SWITCHED" {
		t.Errorf("第二个字段解析错误: %+v", tpl.Fields[1])
	}
}

func TestBuildTemplateBrowserDocFiltersByRouter(t *testing.T) {
	store := fakeTemplateStore{
		"117.185.127.1:2055": {templateKeyOf(9, 1, 270): netflow.TemplateRecord{TemplateId: 270, FieldCount: 1}},
		"117.185.127.2:2055": {templateKeyOf(9, 1, 270): netflow.TemplateRecord{TemplateId: 270, FieldCount: 1}},
		"117.184.216.1:2055": {templateKeyOf(9, 1, 270): netflow.TemplateRecord{TemplateId: 270, FieldCount: 1}},
	}

	doc := buildTemplateBrowserDoc(store, "117.185.127.2")
	if len(doc.Routers) != 1 {
		t.Fatalf("按 IP 过滤后设备数 = %d, 期望 1", len(doc.Routers))
	}
	if doc.Routers[0].Router != "117.185.127.2:2055" {
		t.Errorf("过滤结果错误: %q", doc.Routers[0].Router)
	}
}

func TestBuildTemplateBrowserDocNilStore(t *testing.T) {
	doc := buildTemplateBrowserDoc(nil, "")
	if doc.Routers == nil {
		t.Error("Routers 应为空切片而非 nil，便于前端直接遍历")
	}
	if len(doc.Routers) != 0 {
		t.Errorf("设备数 = %d, 期望 0", len(doc.Routers))
	}
}

func TestTemplateHandler(t *testing.T) {
	store := fakeTemplateStore{
		"117.185.127.2:2055": {
			templateKeyOf(9, 16711688, 270): netflow.TemplateRecord{
				TemplateId: 270,
				FieldCount: 1,
				Fields:     []netflow.Field{{Type: 22, Length: 4}},
			},
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/templates", TemplateHandler(store))

	t.Run("返回分组结构", func(t *testing.T) {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/templates", nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("状态码 = %d, 期望 200", rr.Code)
		}
		if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q", ct)
		}
		var doc templateBrowserDoc
		if err := json.Unmarshal(rr.Body.Bytes(), &doc); err != nil {
			t.Fatalf("响应不是合法 JSON: %v", err)
		}
		if len(doc.Routers) != 1 || len(doc.Routers[0].Templates) != 1 {
			t.Fatalf("响应结构不符: %+v", doc)
		}
		if got := doc.Routers[0].Templates[0].Fields[0].Zh; got != "流开始时刻（第一个包被交换时的 sysUptime 毫秒值）" {
			t.Errorf("中文释义 = %q", got)
		}
	})

	t.Run("router 参数过滤", func(t *testing.T) {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/templates?router=10.0.0.9", nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("状态码 = %d, 期望 200", rr.Code)
		}
		var doc templateBrowserDoc
		if err := json.Unmarshal(rr.Body.Bytes(), &doc); err != nil {
			t.Fatalf("响应不是合法 JSON: %v", err)
		}
		if len(doc.Routers) != 0 {
			t.Errorf("未命中设备应返回空列表, 实际 %d 台", len(doc.Routers))
		}
	})

	t.Run("无数据源时 404", func(t *testing.T) {
		nilMux := http.NewServeMux()
		nilMux.HandleFunc("/templates", TemplateHandler(nil))
		rr := httptest.NewRecorder()
		nilMux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/templates", nil))
		if rr.Code != http.StatusNotFound {
			t.Errorf("状态码 = %d, 期望 404", rr.Code)
		}
	})
}

func TestAsTemplateRecord(t *testing.T) {
	// 运行时解码与持久化预加载存入的都是值类型，这里固定该约定
	if _, ok := asTemplateRecord(netflow.TemplateRecord{TemplateId: 1}); !ok {
		t.Error("值类型 TemplateRecord 应被识别")
	}
	// 接口是 interface{}，指针形态同样兼容，避免调用方实现差异导致字段为空
	if _, ok := asTemplateRecord(&netflow.TemplateRecord{TemplateId: 1}); !ok {
		t.Error("指针类型 *TemplateRecord 应被识别")
	}
	var nilRec *netflow.TemplateRecord
	if _, ok := asTemplateRecord(nilRec); ok {
		t.Error("nil 指针不应被识别为有效模板")
	}
	// 选项模板结构不同，不应被当作普通模板渲染
	if _, ok := asTemplateRecord(netflow.NFv9OptionsTemplateRecord{}); ok {
		t.Error("选项模板不应被识别为普通 TemplateRecord")
	}
}

// TestFieldNameZhMatchesOfficialName 固定「中文释义不得与官方名称张冠李戴」。
// 这些字段的编号并不直观（ICMP_TYPE 是 32 而非 VLAN，SAMPLING_INTERVAL 是 2^n
// 而非采样率），凭印象书写极易出错，故逐条锁定。
func TestFieldNameZhMatchesOfficialName(t *testing.T) {
	cases := []struct {
		typeID   uint16
		wantName string
		contains string // 中文释义需包含的关键词
	}{
		{32, "ICMP_TYPE", "ICMP 类型×256"},
		{34, "SAMPLING_INTERVAL", "2^N"},
		{36, "FLOW_ACTIVE_TIMEOUT", "活动超时"},
		{37, "FLOW_INACTIVE_TIMEOUT", "非活动超时"},
		{38, "ENGINE_TYPE", "流交换引擎类型"},
		{39, "ENGINE_ID", "流交换引擎 ID"},
		{49, "FLOW_SAMPLER_MODE", "采样器模式"},
		{61, "DIRECTION", "1=入向"},
		{98, "postipDiffServCodePoint", "DSCP"},
		// MAC 方向：IN_ 前缀=入方向观测，OUT_ 前缀=出方向观测
		{56, "IN_SRC_MAC", "入方向观测"},
		{57, "OUT_DST_MAC", "出方向观测"},
		{80, "IN_DST_MAC", "入方向观测"},
		{81, "OUT_SRC_MAC", "出方向观测"},
		// uptime 基准时间字段
		{21, "LAST_SWITCHED", "sysUptime 毫秒值"},
		{22, "FIRST_SWITCHED", "sysUptime 毫秒值"},
	}
	for _, tc := range cases {
		name, zh := fieldDescription(9, tc.typeID)
		if name != tc.wantName {
			t.Errorf("type=%d 英文名 = %q, 期望 %q", tc.typeID, name, tc.wantName)
		}
		if !strings.Contains(zh, tc.contains) {
			t.Errorf("type=%d 中文释义 = %q, 期望包含 %q", tc.typeID, zh, tc.contains)
		}
	}
}

// TestNoAnnotationOnWrongFieldType 防止把释义挂到编号不符的字段上。
func TestNoAnnotationOnWrongFieldType(t *testing.T) {
	// 这些编号曾被错标过，逐个确认现在不会返回错误的释义
	wrong := map[uint16][]string{
		32: {"VLAN"},
		36: {"结束原因"},
		37: {"ToS"},
		38: {"流开始时间"},
		39: {"流结束时间"},
		49: {"流结束标志"},
		98: {"下一跳 AS"},
	}
	for typeID, banned := range wrong {
		_, zh := fieldDescription(9, typeID)
		for _, b := range banned {
			if strings.Contains(zh, b) {
				t.Errorf("type=%d 的释义 %q 不应包含 %q", typeID, zh, b)
			}
		}
	}
}

func TestHasUptimeField(t *testing.T) {
	if !hasUptimeField([]templateFieldView{{Type: 22}, {Type: 1}}) {
		t.Error("含 FIRST_SWITCHED 的模板应返回 true")
	}
	if hasUptimeField([]templateFieldView{{Type: 1}, {Type: 2}}) {
		t.Error("不含时间字段的模板应返回 false")
	}
}

// TestAnnotationsCoverProductionFields 保证现网在用的字段类型都有中文释义。
// 用仓库外的现场样本做覆盖度检查：英文名已知却仍显示「暂无中文释义」的字段，
// 说明注解表有遗漏，应在评审阶段暴露而不是等到界面截图才发现。
func TestAnnotationsCoverProductionFields(t *testing.T) {
	// 现网 store.json 中出现过的字段类型（0/210/351 名字表无收录，预期缺失）
	const productionTypes = `1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,21,22,23,24,25,26,
		27,28,29,30,31,34,35,36,37,38,39,43,46,47,48,49,51,56,57,58,59,60,61,62,63,70,71,72,73,
		80,81,89,92,93,210,351`
	var missing []string
	for _, part := range strings.Split(productionTypes, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			t.Fatalf("解析字段类型 %q 失败: %v", part, err)
		}
		_, zh := fieldDescription(9, uint16(n))
		if zh == "暂无中文释义" && fieldNameZh[uint16(n)] == "" {
			// 名字表也没有收录（0/210/351）时属于预期缺失，仅在英文名已知时才报错
			if fieldTypeName(9, uint16(n)) != "Unassigned" {
				missing = append(missing, fmt.Sprintf("type=%d (%s)", n, fieldTypeName(9, uint16(n))))
			}
		}
	}
	if len(missing) > 0 {
		t.Errorf("以下现网在用字段缺少中文释义: %s", strings.Join(missing, ", "))
	}
}

// TestPrefixLengthIsNotMask 固化一次真实的认知错误：
// SRC_MASK/DST_MASK 及 IPv6 对应字段在 RFC 5102 中是 sourceIPv4PrefixLength，
// 即「前缀位数」(0-32 / 0-128)，而不是掩码本身。
func TestPrefixLengthIsNotMask(t *testing.T) {
	cases := []struct {
		typeID   uint16
		name     string
		contains string
	}{
		{9, "SRC_MASK", "前缀长度"},
		{13, "DST_MASK", "前缀长度"},
		{29, "IPV6_SRC_MASK", "前缀长度"},
		{30, "IPV6_DST_MASK", "前缀长度"},
	}
	for _, tc := range cases {
		name, zh := fieldDescription(9, tc.typeID)
		if name != tc.name {
			t.Errorf("type=%d 英文名 = %q, 期望 %q", tc.typeID, name, tc.name)
		}
		if !strings.Contains(zh, tc.contains) {
			t.Errorf("type=%d 释义 = %q, 应说明是%s", tc.typeID, zh, tc.contains)
		}
		if strings.Contains(zh, "掩码") {
			t.Errorf("type=%d 释义 %q 不应称其为掩码，RFC 定义的是前缀位数", tc.typeID, zh)
		}
	}
}

// TestDirectionFollowsRFC 固化 DIRECTION 的取值口径争议：
// RFC 3954 与 RFC 5102 §5.11.6 都定义为 0=ingress / 1=egress，
// Cisco 产品文档却写 1=ingress / 2=egress。释义必须同时列出两种口径，
// 只写其中一种会让人按错误口径判断流方向。
func TestDirectionFollowsRFC(t *testing.T) {
	_, zh := fieldDescription(9, 61)
	for _, want := range []string{"0=入向", "1=出向", "Cisco", "1=入向", "2=出向"} {
		if !strings.Contains(zh, want) {
			t.Errorf("DIRECTION 释义 %q 缺少 %q，两种口径都应列出", zh, want)
		}
	}
}

// TestSamplingIntervalNotesBothConventions 采样间隔在 RFC（1/N）与
// Cisco 文档（2^N）中含义不同，释义需并列说明而非二选一。
func TestSamplingIntervalNotesBothConventions(t *testing.T) {
	_, zh := fieldDescription(9, 34)
	for _, want := range []string{"RFC", "Cisco", "1/N", "2^N"} {
		if !strings.Contains(zh, want) {
			t.Errorf("SAMPLING_INTERVAL 释义 %q 缺少 %q", zh, want)
		}
	}
}
