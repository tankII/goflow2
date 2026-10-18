package protoproducer

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	flowmessage "github.com/netsampler/goflow2/v3/pb"
)

// ProtoProducerMessageIf provides access to flow message helpers.
type ProtoProducerMessageIf interface {
	GetFlowMessage() *ProtoProducerMessage                   // access the underlying structure
	MapCustom(key string, v []byte, cfg MappableField) error // inject custom field
}

// ProtoProducerMessage wraps the protobuf FlowMessage with formatting helpers.
type ProtoProducerMessage struct {
	flowmessage.FlowMessage

	formatter FormatterMapper

	skipDelimiter bool // for binary marshalling, skips the varint prefix
}

var protoMessagePool = sync.Pool{
	New: func() any {
		return &ProtoProducerMessage{}
	},
}

func (m *ProtoProducerMessage) GetFlowMessage() *ProtoProducerMessage {
	return m
}

// MapCustom injects a custom field into the message.
func (m *ProtoProducerMessage) MapCustom(key string, v []byte, cfg MappableField) error {
	return MapCustom(m, v, cfg)
}

// AddLayer appends a layer name to the layer stack.
func (m *ProtoProducerMessage) AddLayer(name string) (ok bool) {
	value, ok := flowmessage.FlowMessage_LayerStack_value[name]
	m.LayerStack = append(m.LayerStack, flowmessage.FlowMessage_LayerStack(value))
	return ok
}

// MarshalBinary encodes the message, optionally skipping the delimiter.
func (m *ProtoProducerMessage) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	if m.skipDelimiter {
		b, err := proto.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("marshal protobuf: %w", err)
		}
		return b, nil
	} else {
		_, err := protodelim.MarshalTo(buf, m)
		if err != nil {
			return nil, fmt.Errorf("marshal protobuf delim: %w", err)
		}
		return buf.Bytes(), nil
	}
}

// MarshalText renders the message using the reflective text formatter.
func (m *ProtoProducerMessage) MarshalText() ([]byte, error) {
	return []byte(m.FormatMessageReflectText("")), nil
}

func (m *ProtoProducerMessage) baseKey(h hash.Hash) {
	plan := m.fieldPlan()
	if len(plan.keys) == 0 {
		return
	}

	var unkMap map[string]interface{}
	if plan.keysNeedUnknown {
		unkMap = m.mapUnknown()
	}

	vfm := reflect.ValueOf(m).Elem()
	for i := range plan.keys {
		ck := &plan.keys[i]

		var fieldValue reflect.Value
		if ck.hasField {
			fieldValue = vfm.FieldByIndex(ck.fieldIndex)
		} else if unkField, ok := unkMap[ck.name]; ok {
			fieldValue = reflect.ValueOf(unkField)
		} else {
			continue
		}
		if _, err := fmt.Fprintf(h, "%v", fieldValue.Interface()); err != nil {
			return
		}
	}
}

func (m *ProtoProducerMessage) Key() []byte {
	if len(m.fieldPlan().keys) == 0 {
		return nil
	}
	h := fnv.New32()
	m.baseKey(h)
	return h.Sum(nil)
}

// MarshalJSON renders the message using the reflective JSON formatter.
func (m *ProtoProducerMessage) MarshalJSON() ([]byte, error) {
	return []byte(m.FormatMessageReflectJSON("")), nil
}

// FormatMessageReflectText renders a text representation using configured fields.
func (m *ProtoProducerMessage) FormatMessageReflectText(ext string) string {
	return m.FormatMessageReflectCustom(ext, "", " ", "=", false)
}

// FormatMessageReflectJSON renders a JSON-like representation using configured fields.
func (m *ProtoProducerMessage) FormatMessageReflectJSON(ext string) string {
	return fmt.Sprintf("{%s}", m.FormatMessageReflectCustom(ext, "\"", ",", ":", true))
}

// ExtractTag retrieves a named struct tag or returns the original field name.
func ExtractTag(name, original string, tag reflect.StructTag) string {
	lookup, ok := tag.Lookup(name)
	if !ok {
		return original
	}
	before, _, _ := strings.Cut(lookup, ",")
	return before
}

func (m *ProtoProducerMessage) mapUnknown() map[string]interface{} {
	unk := m.ProtoReflect().GetUnknown()
	if len(unk) == 0 {
		return nil
	}
	unkMap := make(map[string]interface{})
	var offset int
	for offset < len(unk) {
		num, dataType, length := protowire.ConsumeTag(unk[offset:])
		offset += length
		length = protowire.ConsumeFieldValue(num, dataType, unk[offset:])
		data := unk[offset : offset+length]
		offset += length

		// we check if the index is listed in the config
		if pbField, ok := m.formatter.NumToProtobuf(int32(num)); ok {

			var dest interface{}
			var value interface{}
			switch dataType {
			case protowire.VarintType:
				v, _ := protowire.ConsumeVarint(data)
				value = v
			case protowire.BytesType:
				v, _ := protowire.ConsumeString(data)
				//value = hex.EncodeToString([]byte(v)) // removed, this conversion is left to the renderer
				value = []byte(v)
			default:
				continue
			}
			if pbField.Array {
				var destSlice []interface{}
				if dest, ok := unkMap[pbField.Name]; !ok {
					destSlice = make([]interface{}, 0)
				} else {
					destSlice = dest.([]interface{})
				}
				destSlice = append(destSlice, value)
				dest = destSlice
			} else {
				dest = value
			}

			unkMap[pbField.Name] = dest

		}
	}
	return unkMap
}

type valueRender struct {
	val   string
	isStr bool
}

// fieldPart is a single rendered output column. The common (non-array) case
// stores its single value inline to avoid a per-column heap allocation; array
// columns fall back to a dynamically grown slice.
type fieldPart struct {
	present   bool
	finalName string
	array     bool
	length    int   // original slice length (used to mirror comma placement)
	indices   []int // original indices of appended items
	values    []valueRender
	inline    [1]valueRender
}

// reset 将 fieldPart 复位为零值，但保留 inline 内联存储，使常见的单值列仍无需分配。
func (p *fieldPart) reset() {
	p.present = false
	p.finalName = ""
	p.array = false
	p.length = 0
	p.indices = p.indices[:0]
	p.values = p.inline[:0]
	p.inline[0] = valueRender{}
}

// fieldPartsPool 复用每条消息的 []fieldPart 切片。
// 渲染在每条 flow 上都会执行，"每条消息新建一个切片"曾是堆分配的大头（现网约 1.1KB/条）。
var fieldPartsPool = sync.Pool{
	New: func() any {
		s := make([]fieldPart, 0, 32)
		return &s
	},
}

// acquireFieldParts 返回长度为 n 的、已复位的切片，供 renderFieldParts 填充。
// 返回的指针必须在使用完毕后交回 releaseFieldParts。
func acquireFieldParts(n int) *[]fieldPart {
	sp := fieldPartsPool.Get().(*[]fieldPart)
	s := *sp
	if cap(s) < n {
		s = make([]fieldPart, n)
		*sp = s
	}
	s = s[:n]
	for i := range s {
		s[i].reset()
	}
	return sp
}

// releaseFieldParts 把切片归还池中；归还前先清空对渲染结果的引用，
// 避免池内条目长期持有消息数据而无法回收。
func releaseFieldParts(sp *[]fieldPart) {
	s := *sp
	for i := range s {
		s[i].values = nil
		s[i].indices = nil
		s[i].finalName = ""
	}
	*sp = s
	fieldPartsPool.Put(sp)
}

// compiledField is a per-column rendering plan precomputed once per formatter.
// It removes the per-message reflection (FieldByName) and the repeated map
// lookups that a reflective renderer would perform for every flow.
type compiledField struct {
	finalName   string // display name (after rename)
	renderName  string // name passed to the renderer (after remap)
	unkName     string // key to look up in the unknown-field map
	litVal      string
	literal     bool
	hasField    bool
	fieldIndex  []int
	renderer    RenderFunc
	hasRenderer bool
	isArray     bool

	// directScalar 表示"原生 protobuf 标量列且用户未自定义 renderer"。
	// 这类列无需装箱成 interface 再交给 renderer，可在渲染期直接取值。
	directScalar bool

	// appendKind 表示该列可走"直写旁路"：使用内建 renderer 且字段形态匹配，
	// 因此可绕过 renderer 的"返回值转字符串"这一步，直接追加进目标缓冲。
	// 仅内建 IP / datetime 类 renderer 会被识别；用户自定义 renderer 一律不走此路。
	appendKind   appendKind
	appendLayout string // 仅 appendDateTimeNano 使用
}

// appendKind 标识某列的直写方式（0 表示不支持直写，仍走 renderer 字符串路径）。
type appendKind uint8

const (
	appendNone appendKind = iota
	// appendIP：字段是 []byte，追加其文本形式的 IP 地址。
	appendIP
	// appendDateTimeNano：字段是纳秒级时间戳，按自定义 layout 追加。
	appendDateTimeNano
)

// appendColumn 直写旁路：把该列的文本形式追加到 dst，全程不装箱、不建中间字符串。
// 输出与对应 renderer 的字符串结果逐字节一致。
func (cf *compiledField) appendColumn(v reflect.Value, dst []byte) []byte {
	switch cf.appendKind {
	case appendIP:
		// Bytes() 直接取 []byte，不经过 Interface() 装箱
		return appendIPBytes(dst, v.Bytes())
	case appendDateTimeNano:
		return appendDateTimeNanoField(dst, v.Uint(), cf.appendLayout)
	}
	return dst
}

// scalarDirect reports whether a reflect.Kind can be formatted without boxing.
// 与 renderValue 的标量分支保持一致（此消息的标量列均为 uint32/uint64）。
func scalarDirect(k reflect.Kind) bool {
	switch k {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Bool:
		return true
	}
	return false
}

type compiledKey struct {
	name       string
	hasField   bool
	fieldIndex []int
}

type compiledPlan struct {
	fields          []compiledField
	keys            []compiledKey
	needsUnknown    bool
	keysNeedUnknown bool
}

var protoMessageType = reflect.TypeOf((*ProtoProducerMessage)(nil)).Elem()

// rendererIDOf 以可选接口方式取列的 renderer 标识；不支持的实现返回 false，
// 该列便不会走直写旁路（安全退化到原字符串路径）。
func rendererIDOf(f FormatterMapper, name string) (RendererID, bool) {
	if g, ok := f.(interface {
		RenderID(name string) (RendererID, bool)
	}); ok {
		return g.RenderID(name)
	}
	return "", false
}

// compilePlan resolves once, per formatter, everything that does not depend on
// the message: field indices, renderers, literals and array-ness.
func compilePlan(f FormatterMapper) *compiledPlan {
	fields := f.Fields()
	plan := &compiledPlan{fields: make([]compiledField, len(fields))}
	for i, s := range fields {
		cf := compiledField{unkName: s, renderer: NilRenderer}
		if rn, ok := f.Rename(s); ok && rn != "" {
			cf.finalName = rn
		} else {
			cf.finalName = s
		}
		if lit, ok := f.Literal(s); ok {
			cf.literal = true
			cf.litVal = lit
			plan.fields[i] = cf
			continue
		}
		fieldName := s
		if rm, ok := f.Remap(fieldName); ok && rm != "" {
			fieldName = rm
		}
		cf.renderName = fieldName
		if renderer, ok := f.Render(fieldName); ok {
			cf.renderer = renderer
			cf.hasRenderer = true
		}
		fieldKind := reflect.Invalid
		if sf, ok := protoMessageType.FieldByName(fieldName); ok {
			cf.hasField = true
			cf.fieldIndex = sf.Index
			fieldKind = sf.Type.Kind()
		} else {
			// No struct field: the value can only come from the unknown-field map.
			plan.needsUnknown = true
		}
		cf.isArray = f.IsArray(fieldName)
		// 原生标量列 + 未自定义 renderer → 可跳过装箱与 renderer，直接格式化。
		cf.directScalar = cf.hasField && !cf.isArray && !cf.hasRenderer && scalarDirect(fieldKind)
		// 内建 IP / datetime renderer + 字段形态匹配 → 可直写进缓冲。
		if cf.hasField && !cf.isArray && cf.hasRenderer {
			// 仅当"登记的标识"与"当前实际使用的 renderer 函数"都指向内建实现时
			// 才开启直写：若调用方只替换了 render 而未同步 renderIDs（或反之），
			// 这里会检测到不一致并退���到原字符串路径，避免绕过自定义 renderer。
			if id, ok := rendererIDOf(f, fieldName); ok && builtinRendererMatches(id, cf.renderer) {
				cf.appendKind, cf.appendLayout = appendKindFor(id, fieldKind)
			}
		}
		plan.fields[i] = cf
	}

	for _, s := range f.Keys() {
		ck := compiledKey{name: s}
		fieldName := s
		if rm, ok := f.Remap(fieldName); ok && rm != "" {
			fieldName = rm
		}
		if sf, ok := protoMessageType.FieldByName(fieldName); ok {
			ck.hasField = true
			ck.fieldIndex = sf.Index
		} else {
			plan.keysNeedUnknown = true
		}
		plan.keys = append(plan.keys, ck)
	}

	return plan
}

func (m *ProtoProducerMessage) fieldPlan() *compiledPlan {
	if p, ok := m.formatter.(interface{ compiledPlan() *compiledPlan }); ok {
		return p.compiledPlan()
	}
	return compilePlan(m.formatter)
}

// renderValue 以与 fmt.Sprintf("%v", v) 相同的方式格式化渲染结果，
// 但对常见标量不做反射。第二个返回值表示是否为字符串（供 JSON 格式化器决定加引号）。
func renderValue(v interface{}) (string, bool) {
	switch val := v.(type) {
	case string:
		return val, true
	case int:
		return strconv.Itoa(val), false
	case int8:
		return strconv.FormatInt(int64(val), 10), false
	case int16:
		return strconv.FormatInt(int64(val), 10), false
	case int32:
		return strconv.FormatInt(int64(val), 10), false
	case int64:
		return strconv.FormatInt(val, 10), false
	case uint:
		return strconv.FormatUint(uint64(val), 10), false
	case uint8:
		return strconv.FormatUint(uint64(val), 10), false
	case uint16:
		return strconv.FormatUint(uint64(val), 10), false
	case uint32:
		return strconv.FormatUint(uint64(val), 10), false
	case uint64:
		return strconv.FormatUint(val, 10), false
	case bool:
		return strconv.FormatBool(val), false
	}
	return fmt.Sprintf("%v", v), reflect.TypeOf(v).Kind() == reflect.String
}

// appendScalar 把原生标量字段直接追加到 dst，不经 interface{} 装箱，
// 输出与 renderValue 的标量分支完全一致（数字不加引号，bool 输出 true/false）。
func appendScalar(dst []byte, v reflect.Value) []byte {
	switch v.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.AppendUint(dst, v.Uint(), 10)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(dst, v.Int(), 10)
	case reflect.Bool:
		return strconv.AppendBool(dst, v.Bool())
	}
	// 理论上不会走到（directScalar 只由 scalarDirect 判定为标量时置位）。
	return append(dst, fmt.Sprintf("%v", v.Interface())...)
}

// renderScalar 是需要字符串形态的调用方（text/JSON、CSVValues）使用的版本。
func renderScalar(v reflect.Value) string {
	switch v.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Bool:
		return strconv.FormatBool(v.Bool())
	}
	return fmt.Sprintf("%v", v.Interface())
}

// renderFieldParts 将渲染结果写入池化切片（长度 len(plan.fields)）。
// 缺失的（非虚拟）字段 present=false，调用方可据此压缩输出（text/JSON）
// 或保持列对齐（CSV）。
// skipDirectScalar 为 true 时（CSV 追加路径）不为原生标量列生成字符串，
// 由调用方直接取值追加，从而完全避免中间字符串分配。
func (m *ProtoProducerMessage) renderFieldParts(sp *[]fieldPart, skipDirectScalar bool) []fieldPart {
	plan := m.fieldPlan()
	if cap(*sp) < len(plan.fields) {
		*sp = make([]fieldPart, len(plan.fields))
	}
	parts := (*sp)[:len(plan.fields)]
	for i := range parts {
		parts[i].reset()
	}

	var unkMap map[string]interface{}
	if plan.needsUnknown {
		unkMap = m.mapUnknown()
	}

	vfm := reflect.ValueOf(m).Elem()
	for idx := range plan.fields {
		cf := &plan.fields[idx]
		part := &parts[idx]

		// constant (non-native) columns take precedence: emit the configured
		// value verbatim, honoring any rename for the display name.
		if cf.literal {
			part.present = true
			part.finalName = cf.finalName
			part.inline[0] = valueRender{val: cf.litVal, isStr: true}
			part.values = part.inline[:1]
			continue
		}

		var fieldValue reflect.Value
		if cf.hasField {
			fieldValue = vfm.FieldByIndex(cf.fieldIndex)
		} else if unkField, ok := unkMap[cf.unkName]; ok {
			fieldValue = reflect.ValueOf(unkField)
		} else if !cf.hasRenderer { // not a virtual field
			continue
		}

		part.finalName = cf.finalName
		part.array = cf.isArray

		// 可直写列（原生标量 / 内建 IP·时间）：CSV 追加路径不需要中间字符串，
		// 由 AppendCSVRow 直接取值追加；此处只标记 present。
		if cf.appendKind != appendNone {
			part.present = true
			if fieldValue.IsValid() && !skipDirectScalar {
				part.inline[0] = valueRender{val: renderAppendable(cf, fieldValue), isStr: true}
				part.values = part.inline[:1]
			}
			continue
		}

		// 原生标量列（未自定义 renderer）：跳过 Interface() 装箱与 renderer 调用。
		// 值仍经 renderValue 格式化，保证 text/JSON 路径输出与旧实现完全一致；
		// CSV 路径会在 AppendCSVRow 中进一步直接 append，省掉中间字符串。
		if cf.directScalar {
			part.present = true
			if fieldValue.IsValid() && !skipDirectScalar {
				part.inline[0] = valueRender{val: renderScalar(fieldValue)}
				part.values = part.inline[:1]
			}
			continue
		}

		// render each item of the array independently
		// note: isArray is necessary to consider certain byte arrays in their entirety
		// eg: IP addresses
		if cf.isArray {
			part.present = true
			if fieldValue.IsValid() {
				c := fieldValue.Len()
				part.length = c
				for i := 0; i < c; i++ {
					fieldValueI := fieldValue.Index(i)
					var val interface{}
					if fieldValueI.IsValid() {
						val = fieldValueI.Interface()
					}
					rendered := cf.renderer(m, cf.renderName, val)
					if rendered == nil {
						continue
					}
					s, isStr := renderValue(rendered)
					part.values = append(part.values, valueRender{val: s, isStr: isStr})
					part.indices = append(part.indices, i)
				}
			}
			continue
		}

		var val interface{}
		if fieldValue.IsValid() {
			val = fieldValue.Interface()
		}
		rendered := cf.renderer(m, cf.renderName, val)
		if rendered == nil {
			continue
		}
		s, isStr := renderValue(rendered)
		part.present = true
		part.inline[0] = valueRender{val: s, isStr: isStr}
		part.values = part.inline[:1]
	}

	return parts
}

// FormatMessageReflectCustom renders a custom-delimited representation.
func (m *ProtoProducerMessage) FormatMessageReflectCustom(ext, quotes, sep, sign string, null bool) string {
	sp := acquireFieldParts(len(m.fieldPlan().fields))
	parts := m.renderFieldParts(sp, false)
	var b strings.Builder
	first := true
	for i := range parts {
		part := &parts[i]
		if !part.present {
			continue
		}
		if !first {
			b.WriteString(sep)
		}
		first = false

		if part.array {
			b.WriteString(quotes)
			b.WriteString(part.finalName)
			b.WriteString(quotes)
			b.WriteString(sign)
			b.WriteByte('[')
			for j, it := range part.values {
				if it.isStr {
					b.WriteString(quotes)
					b.WriteString(it.val)
					b.WriteString(quotes)
				} else {
					b.WriteString(it.val)
				}
				if part.indices[j] < part.length-1 {
					b.WriteByte(',')
				}
			}
			b.WriteByte(']')
			continue
		}

		b.WriteString(quotes)
		b.WriteString(part.finalName)
		b.WriteString(quotes)
		b.WriteString(sign)
		if part.values[0].isStr {
			b.WriteString(quotes)
			b.WriteString(part.values[0].val)
			b.WriteString(quotes)
		} else {
			b.WriteString(part.values[0].val)
		}
	}
	releaseFieldParts(sp)

	return b.String()
}

// csvValuesFromParts 把渲染结果映射为 CSV 列，追加到 dst 中，
// 便于调用方复用一个底层数组。
func csvValuesFromParts(parts []fieldPart, dst []string) []string {
	for i := range parts {
		part := &parts[i]
		if !part.present {
			dst = append(dst, "")
			continue
		}
		if part.array {
			var b strings.Builder
			b.WriteByte('[')
			for j, it := range part.values {
				if j > 0 {
					b.WriteByte(',')
				}
				b.WriteString(it.val)
			}
			b.WriteByte(']')
			dst = append(dst, b.String())
		} else {
			dst = append(dst, part.values[0].val)
		}
	}
	return dst
}

// appendCSVValues 把 CSV 各列渲染进 dst，复用其底层数组；dst 本身保持不变，
// 返回（可能已扩容的）切片。
func (m *ProtoProducerMessage) appendCSVValues(dst []string) []string {
	sp := acquireFieldParts(len(m.fieldPlan().fields))
	parts := m.renderFieldParts(sp, false)
	out := csvValuesFromParts(parts, dst[:0])
	releaseFieldParts(sp)
	return out
}

// CSVValues returns the rendered values in formatter.Fields() order. Missing
// (non-virtual) fields map to empty strings so columns stay aligned.
func (m *ProtoProducerMessage) CSVValues() []string {
	return m.appendCSVValues(nil)
}

// csvNeedsQuote 与 CSV 转义规则一致：当值包含分隔符、引号、CR/LF，
// 或首尾为空格/制表符时需要加引号。
func csvNeedsQuote(v string, delim string) bool {
	if strings.Contains(v, delim) || strings.ContainsAny(v, "\"\r\n") {
		return true
	}
	if len(v) == 0 {
		return false
	}
	if c := v[0]; c == ' ' || c == '\t' {
		return true
	}
	if c := v[len(v)-1]; c == ' ' || c == '\t' {
		return true
	}
	return false
}

// appendCSVField 按 RFC 4180 规则转义并追加单个 CSV 列。
func appendCSVField(dst []byte, v string, delim string) []byte {
	if !csvNeedsQuote(v, delim) {
		return append(dst, v...)
	}
	dst = append(dst, '"')
	for i := 0; i < len(v); i++ {
		if v[i] == '"' {
			dst = append(dst, '"')
		}
		dst = append(dst, v[i])
	}
	return append(dst, '"')
}

// AppendCSVRow 将整条消息渲染为一行转义后的 CSV 并追加到 dst。
// 输出与优化前 CSVValues + strings.Join 的结果逐字节一致，但不产生中间字符串。
func (m *ProtoProducerMessage) AppendCSVRow(dst []byte, delim string) []byte {
	plan := m.fieldPlan()
	sp := acquireFieldParts(len(plan.fields))
	parts := m.renderFieldParts(sp, true)

	vfm := reflect.ValueOf(m).Elem()
	var arr []byte
	for i := range parts {
		if i > 0 {
			dst = append(dst, delim...)
		}
		part := &parts[i]
		switch {
		case !part.present:
			// missing column: empty, unquoted
		case plan.fields[i].appendKind != appendNone:
			// 内建 IP / 时间列：直接追加，不装箱、不建中间字符串，
			// 输出与 renderer 字符串结果逐字节一致（时间格式含空格也无需转义）。
			dst = plan.fields[i].appendColumn(vfm.FieldByIndex(plan.fields[i].fieldIndex), dst)
		case plan.fields[i].directScalar:
			// 原生标量列：数字/布尔无需转义，直接 strconv 追加，不产生中间字符串。
			dst = appendScalar(dst, vfm.FieldByIndex(plan.fields[i].fieldIndex))
		case part.array:
			arr = arr[:0]
			arr = append(arr, '[')
			for j, it := range part.values {
				if j > 0 {
					arr = append(arr, ',')
				}
				arr = append(arr, it.val...)
			}
			arr = append(arr, ']')
			dst = appendCSVField(dst, string(arr), delim)
		default:
			dst = appendCSVField(dst, part.values[0].val, delim)
		}
	}
	releaseFieldParts(sp)

	return dst
}
