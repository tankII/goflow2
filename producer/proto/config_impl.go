package protoproducer

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
)

var (
	isSliceMap = map[string]bool{
		"BgpCommunities":             true,
		"AsPath":                     true,
		"MplsIp":                     true,
		"MplsLabel":                  true,
		"MplsTtl":                    true,
		"LayerStack":                 true,
		"LayerSize":                  true,
		"Ipv6RoutingHeaderAddresses": true,
	}
)

type NetFlowMapField struct {
	PenProvided bool   `yaml:"penprovided"`
	Type        uint16 `yaml:"field"`
	Pen         uint32 `yaml:"pen"`

	Destination string     `yaml:"destination"`
	Endian      EndianType `yaml:"endianness"`
	//DestinationLength uint8  `json:"dlen"` // could be used if populating a slice of uint16 that aren't in protobuf
}

// IPFIXProducerConfig holds IPFIX field mappings.
type IPFIXProducerConfig struct {
	Mapping []NetFlowMapField `yaml:"mapping"`
}

// NetFlowV9ProducerConfig holds NetFlow v9 field mappings.
type NetFlowV9ProducerConfig struct {
	Mapping []NetFlowMapField `yaml:"mapping"`
}

// SFlowMapField defines a mapping from packet offsets to protobuf fields.
type SFlowMapField struct {
	Layer        string `yaml:"layer"`
	Encapsulated bool   `yaml:"encap"`  // only parse if encapsulated
	Offset       int    `yaml:"offset"` // offset in bits
	Length       int    `yaml:"length"` // length in bits

	Destination string     `yaml:"destination"`
	Endian      EndianType `yaml:"endianness"`
	//DestinationLength uint8  `json:"dlen"`
}

// SFlowProtocolParse defines a parser override for a protocol/port.
type SFlowProtocolParse struct {
	Proto  string     `yaml:"proto"`
	Dir    RegPortDir `yaml:"dir"`
	Port   uint16     `yaml:"port"`
	Parser string     `yaml:"parser"`
}

// SFlowProducerConfig holds sFlow mapping configuration.
type SFlowProducerConfig struct {
	Mapping []SFlowMapField      `yaml:"mapping"`
	Ports   []SFlowProtocolParse `yaml:"ports"`
}

// ProtobufFormatterConfig describes a protobuf field for formatting.
type ProtobufFormatterConfig struct {
	Name  string `yaml:"name"`
	Index int32  `yaml:"index"`
	Type  string `yaml:"type"`
	Array bool   `yaml:"array"`
}

// FormatterConfig defines which fields and renderers to use.
type FormatterConfig struct {
	Fields   []string                  `yaml:"fields"`
	Key      []string                  `yaml:"key"`
	Render   map[string]RendererID     `yaml:"render"`
	Rename   map[string]string         `yaml:"rename"`
	Literal  map[string]interface{}    `yaml:"literal"`
	Protobuf []ProtobufFormatterConfig `yaml:"protobuf"`
}

// ProducerConfig is the top-level config for protobuf producers.
type ProducerConfig struct {
	Formatter FormatterConfig `yaml:"formatter"`

	IPFIX     IPFIXProducerConfig     `yaml:"ipfix"`
	NetFlowV9 NetFlowV9ProducerConfig `yaml:"netflowv9"`
	SFlow     SFlowProducerConfig     `yaml:"sflow"` // also used for IPFIX data frames

	// should do a rename map list for when printing
}

// Compile converts ProducerConfig into a mapped configuration.
func (c *ProducerConfig) Compile() (ProtoProducerConfig, error) {
	return mapConfig(c)
}

// Optimized version of a configuration to be used by a protobuf producer
type producerConfigMapped struct {
	Formatter *FormatterConfigMapper

	IPFIX     *NetFlowMapper
	NetFlowV9 *NetFlowMapper
	SFlow     *SFlowMapper
}

func (c *producerConfigMapped) GetFormatter() FormatterMapper {
	return c.Formatter
}

func (c *producerConfigMapped) GetIPFIXMapper() TemplateMapper {
	return c.IPFIX
}

func (c *producerConfigMapped) GetNetFlowMapper() TemplateMapper {
	return c.NetFlowV9
}

func (c *producerConfigMapped) GetPacketMapper() PacketMapper {
	return c.SFlow
}

type DataMap struct {
	MapConfigBase
}

// FormatterConfigMapper implements FormatterMapper from a config.
type FormatterConfigMapper struct {
	fields []string
	key    []string
	reMap  map[string]string // map from a potential json name into the protobuf structure
	rename map[string]string // manually renaming fields
	render map[string]RenderFunc
	// renderIDs 记录每列配置的 renderer 标识（含内建默认列），用于在 compilePlan
	// 阶段判定该列能否走"直写旁路"。自定义 renderer 也会记录，便于识别并跳过。
	renderIDs map[string]RendererID
	pbMap     map[string]ProtobufFormatterConfig
	numToPb   map[int32]ProtobufFormatterConfig
	isSlice   map[string]bool
	literal   map[string]string // constant-valued columns

	planOnce sync.Once
	plan     *compiledPlan
}

func (f *FormatterConfigMapper) compiledPlan() *compiledPlan {
	f.planOnce.Do(func() {
		f.plan = compilePlan(f)
	})
	return f.plan
}

func (f *FormatterConfigMapper) Keys() []string {
	return f.key
}

func (f *FormatterConfigMapper) Fields() []string {
	return f.fields
}

func (f *FormatterConfigMapper) Rename(name string) (string, bool) {
	r, ok := f.rename[name]
	return r, ok
}

func (f *FormatterConfigMapper) Remap(name string) (string, bool) {
	r, ok := f.reMap[name]
	return r, ok
}

func (f *FormatterConfigMapper) Render(name string) (RenderFunc, bool) {
	r, ok := f.render[name]
	return r, ok
}

// RenderID 返回该列配置的 renderer 标识。热路径以可选接口方式探测，
// 不改动 FormatterMapper 公开接口。
func (f *FormatterConfigMapper) RenderID(name string) (RendererID, bool) {
	r, ok := f.renderIDs[name]
	return r, ok
}

// Literal returns the constant value configured for a non-native column.
func (f *FormatterConfigMapper) Literal(name string) (string, bool) {
	r, ok := f.literal[name]
	return r, ok
}

func (f *FormatterConfigMapper) NumToProtobuf(num int32) (ProtobufFormatterConfig, bool) {
	r, ok := f.numToPb[num]
	return r, ok
}

func (f *FormatterConfigMapper) IsArray(name string) bool {
	return f.isSlice[name]
}

// netFlowMapKey 用于在映射器中定位 NetFlow 模板字段。
//
// 取代原先 fmt.Sprintf("%v-%d-%d", ...) 的字符串键：旧实现在每个被映射字段上都要
// 格式化并分配字符串（现网 profile 中占 13.4% CPU / 5.1GB 分配）。改为可比较的
// 结构体键后，热路径为纯查表，零分配。
type netFlowMapKey struct {
	penProvided bool
	pen         uint32
	fieldType   uint16
}

type NetFlowMapper struct {
	data map[netFlowMapKey]*DataMap // maps field to destination
}

func (m *NetFlowMapper) Map(field netflow.DataField) (MappableField, bool) {
	if m == nil {
		return &DataMap{}, false
	}
	mapped, found := m.data[netFlowMapKey{penProvided: field.PenProvided, pen: field.Pen, fieldType: field.Type}]
	return mapped, found
}

type SFlowMapper struct {
	data              map[string][]*DataMapLayer // map layer to list of offsets
	parserEnvironment ParserEnvironment
}

type sflowMapperIterator struct {
	data []*DataMapLayer
	n    int
}

func (i *sflowMapperIterator) Next() MappableByteField {
	if len(i.data) <= i.n {
		return nil
	}
	d := i.data[i.n]
	i.n += 1
	return d
}

func (m *SFlowMapper) Map(layer string) MapLayerIterator {
	if m == nil {
		return nil
	}
	return &sflowMapperIterator{data: m.data[strings.ToLower(layer)], n: 0}
}

func (m *SFlowMapper) ParsePacket(flowMessage ProtoProducerMessageIf, data []byte) (err error) {
	if m == nil {
		return ParsePacket(flowMessage, data, m, DefaultEnvironment)
	}
	return ParsePacket(flowMessage, data, m, m.parserEnvironment)
}

// Structure to help the MapCustom functions
// populate the protobuf data
// MapConfigBase stores destination mapping metadata.
type MapConfigBase struct {
	// Used if the field inside the protobuf exists
	// also serves as the field when rendering with text
	Destination string
	Endianness  EndianType

	// The following fields are used for mapping
	// when the destination field does not exist
	// inside the protobuf
	ProtoIndex int32
	ProtoType  ProtoType
	ProtoArray bool

	// fieldIndex 为 Destination 在 protobuf 消息中的反射下标路径，在映射器构建
	// 期解析一次（见 resolveFieldIndex）；非 nil 表示"已解析"，空切片表示目标不是
	// Go 字段。MapCustom 据此跳过每字段的 reflect.FieldByName。
	fieldIndex []int
}

func (c *MapConfigBase) GetEndianness() EndianType {
	return c.Endianness
}

func (c *MapConfigBase) GetDestination() string {
	return c.Destination
}

func (c *MapConfigBase) GetProtoIndex() int32 {
	return c.ProtoIndex
}

func (c *MapConfigBase) GetProtoType() ProtoType {
	return c.ProtoType
}

func (c *MapConfigBase) IsArray() bool {
	return c.ProtoArray
}

// GetFieldIndex 返回构建期解析好的 Destination 反射下标路径。
// nil 表示未解析（临时配置，调用方需回退 FieldByName）；空切片表示目标不是 Go 字段。
func (c *MapConfigBase) GetFieldIndex() []int {
	return c.fieldIndex
}

// resolveFieldIndex 在配置构建期解析一次目标 protobuf 字段的反射下标并缓存，
// 使 MapCustom 热路径不必对每个字段调用 reflect.FieldByName（线性扫描约 100 个
// 字段且伴随分配）。
//
// 这里总是把 fieldIndex 置为非 nil（解析不到 Go 字段时为空切片），即"已解析"
// 状态被显式记录：现网 mapping 的 destination 常为 snake_case 的手动 protobuf
// 字段（如 flow_direction），这类目标本来就不是 Go 字段，若仍标记为未解析，
// 热路径会对每条记录回退到注定失败的 FieldByName。
func resolveFieldIndex(v *MapConfigBase) {
	v.fieldIndex = []int{}
	if v.Destination == "" {
		return
	}
	if sf, ok := protoMessageType.FieldByName(v.Destination); ok {
		v.fieldIndex = sf.Index
	}
}

// Extended structure for packet mapping
// DataMapLayer describes a packet layer mapping entry.
type DataMapLayer struct {
	MapConfigBase
	Offset       int
	Length       int
	Encapsulated bool
}

func (c *DataMapLayer) GetOffset() int {
	return c.Offset
}

func (c *DataMapLayer) GetLength() int {
	return c.Length
}

func (c *DataMapLayer) IsEncapsulated() bool {
	return c.Encapsulated
}

func mapFieldsSFlow(fields []SFlowMapField) *SFlowMapper {
	ret := make(map[string][]*DataMapLayer)
	for _, field := range fields {
		retLayerEntry := &DataMapLayer{
			Offset:       field.Offset,
			Length:       field.Length,
			Encapsulated: field.Encapsulated,
		}
		retLayerEntry.Destination = field.Destination
		retLayerEntry.Endianness = field.Endian
		resolveFieldIndex(&retLayerEntry.MapConfigBase)
		retLayer := ret[field.Layer]
		retLayer = append(retLayer, retLayerEntry)
		ret[field.Layer] = retLayer
	}
	return &SFlowMapper{data: ret}
}

func mapPortsSFlow(ports []SFlowProtocolParse) (ParserEnvironment, error) {
	e := NewBaseParserEnvironment()
	for _, port := range ports {
		parser, ok := e.GetParser(port.Parser)
		if !ok {
			return e, fmt.Errorf("parser %s not found", port.Parser)
		}
		if err := e.RegisterPort(port.Proto, port.Dir, port.Port, parser); err != nil {
			return e, fmt.Errorf("register port %s/%s/%d: %w", port.Proto, port.Dir, port.Port, err)
		}
	}
	return e, nil
}

func mapFieldsNetFlow(fields []NetFlowMapField) *NetFlowMapper {
	ret := make(map[netFlowMapKey]*DataMap)
	for _, field := range fields {
		dm := &DataMap{}
		dm.Destination = field.Destination
		dm.Endianness = field.Endian
		resolveFieldIndex(&dm.MapConfigBase)
		ret[netFlowMapKey{penProvided: field.PenProvided, pen: field.Pen, fieldType: field.Type}] = dm
	}
	return &NetFlowMapper{data: ret}
}

func (c *producerConfigMapped) finalizemapDest(v *MapConfigBase) error {
	if vv, ok := c.Formatter.pbMap[v.Destination]; ok {
		v.ProtoIndex = vv.Index

		if pt, ok := ProtoTypeMap[vv.Type]; ok {
			v.ProtoType = pt
		} else {
			return fmt.Errorf("could not map %s to a ProtoType", vv.Type)
		}

		v.ProtoArray = vv.Array
	}
	return nil
}

func (c *producerConfigMapped) finalizeSFlowMapper(m *SFlowMapper) error {
	if m == nil {
		return nil
	}
	for k, vlist := range m.data {
		for i, v := range vlist {
			if err := c.finalizemapDest(&(v.MapConfigBase)); err != nil {
				return fmt.Errorf("finalize sflow mapper %s[%d]: %w", k, i, err)
			}
			m.data[k][i] = v
		}
	}
	return nil
}

func (c *producerConfigMapped) finalizeNetFlowMapper(m *NetFlowMapper) error {
	if m == nil {
		return nil
	}
	for k, v := range m.data {
		if err := c.finalizemapDest(&(v.MapConfigBase)); err != nil {
			return fmt.Errorf("finalize netflow mapper %v: %w", k, err)
		}
		m.data[k] = v
	}
	return nil
}

func (c *producerConfigMapped) finalize() error {
	if c.Formatter == nil {
		return nil
	}
	if err := c.finalizeNetFlowMapper(c.IPFIX); err != nil {
		return fmt.Errorf("finalize ipfix mapper: %w", err)
	}
	if err := c.finalizeNetFlowMapper(c.NetFlowV9); err != nil {
		return fmt.Errorf("finalize netflow v9 mapper: %w", err)
	}
	if err := c.finalizeSFlowMapper(c.SFlow); err != nil {
		return fmt.Errorf("finalize sflow mapper: %w", err)
	}

	return nil
}

func mapFormat(cfg *ProducerConfig) (*FormatterConfigMapper, error) {
	formatterMapped := &FormatterConfigMapper{}

	selectorTag := "json"
	var msg ProtoProducerMessage
	msgT := reflect.TypeOf(&msg.FlowMessage).Elem() // required indirect otherwise go vet indicates TypeOf copies lock
	reMap := make(map[string]string)
	numToPb := make(map[int32]ProtobufFormatterConfig)
	var fields []string

	for i := 0; i < msgT.NumField(); i++ {
		field := msgT.Field(i)
		if !field.IsExported() {
			continue
		}
		fieldName := field.Name
		if selectorTag != "" {
			fieldName = ExtractTag(selectorTag, fieldName, field.Tag)
			reMap[fieldName] = field.Name
			fields = append(fields, fieldName)
		}
		//customSelectorTmp[i] = fieldName

	}

	formatterMapped.reMap = reMap
	pbMap := make(map[string]ProtobufFormatterConfig)
	formatterMapped.render = make(map[string]RenderFunc)
	formatterMapped.rename = make(map[string]string)
	formatterMapped.literal = make(map[string]string)
	formatterMapped.isSlice = isSliceMap
	formatterMapped.renderIDs = make(map[string]RendererID)
	for k, v := range defaultRenderers {
		formatterMapped.render[k] = v
	}
	for k, v := range defaultRenderIDs {
		formatterMapped.renderIDs[k] = v
	}

	if cfg != nil {
		cfgFormatter := cfg.Formatter

		// manual protobuf fields to add
		for _, pbField := range cfgFormatter.Protobuf {
			reMap[pbField.Name] = ""      // special dynamic protobuf
			pbMap[pbField.Name] = pbField // todo: check if type is valid

			numToPb[pbField.Index] = pbField
			formatterMapped.isSlice[pbField.Name] = pbField.Array
		}
		// populate manual renames
		for k, v := range cfgFormatter.Rename {
			formatterMapped.rename[k] = v
		}

		// populate constant-valued columns (values normalized to strings)
		for k, v := range cfgFormatter.Literal {
			formatterMapped.literal[k] = fmt.Sprintf("%v", v)
		}

		// populate key
		for _, v := range cfgFormatter.Key {
			if _, ok := reMap[v]; !ok {
				return formatterMapped, fmt.Errorf("key field %s does not exist", v)
			}
			formatterMapped.key = append(formatterMapped.key, v)
		}

		// process renderers
		for k, v := range cfgFormatter.Render {
			if kk, ok := reMap[k]; ok && kk != "" {
				k = kk
			}
			if renderer, ok := resolveRenderer(v); ok {
				formatterMapped.render[k] = renderer
				formatterMapped.renderIDs[k] = v
			} else {
				return formatterMapped, fmt.Errorf("field %s is not a renderer", v) // todo: make proper error
			}
		}

		// if the config does not contain any fields initially, we set with the protobuf ones
		if len(cfgFormatter.Fields) == 0 {
			formatterMapped.fields = fields
		} else {
			for _, field := range cfgFormatter.Fields {
				if _, ok := reMap[field]; !ok {

					// check if it's a virtual field
					if _, ok := formatterMapped.render[field]; !ok {
						// or a literal (constant) column
						if _, ok := formatterMapped.literal[field]; !ok {
							return formatterMapped, fmt.Errorf("field %s in config not found in protobuf", field) // todo: make proper error
						}
					}

				}
			}
			formatterMapped.fields = cfgFormatter.Fields
		}

		formatterMapped.pbMap = pbMap
		formatterMapped.numToPb = numToPb

	} else {
		formatterMapped.fields = fields
	}

	return formatterMapped, nil
}

func mapConfig(cfg *ProducerConfig) (*producerConfigMapped, error) {
	newCfg := &producerConfigMapped{}
	if cfg != nil {
		newCfg.IPFIX = mapFieldsNetFlow(cfg.IPFIX.Mapping)
		newCfg.NetFlowV9 = mapFieldsNetFlow(cfg.NetFlowV9.Mapping)
		newCfg.SFlow = mapFieldsSFlow(cfg.SFlow.Mapping)
		var err error
		newCfg.SFlow.parserEnvironment, err = mapPortsSFlow(cfg.SFlow.Ports)
		if err != nil {
			return nil, fmt.Errorf("map sflow ports: %w", err)
		}
	}
	var err error
	if newCfg.Formatter, err = mapFormat(cfg); err != nil {
		return newCfg, fmt.Errorf("map formatter: %w", err)
	}
	if err := newCfg.finalize(); err != nil {
		return newCfg, fmt.Errorf("finalize config: %w", err)
	}
	return newCfg, nil
}
