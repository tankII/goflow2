package protoproducer

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"
	"reflect"
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
	vfm := reflect.ValueOf(m)
	vfm = reflect.Indirect(vfm)

	unkMap := m.mapUnknown() // todo: should be able to reuse if set in structure

	for _, s := range m.formatter.Keys() {
		fieldName := s

		// get original name from structure
		if fieldNameMap, ok := m.formatter.Remap(fieldName); ok && fieldNameMap != "" {
			fieldName = fieldNameMap
		}

		fieldValue := vfm.FieldByName(fieldName)
		// if does not exist from structure,
		// fetch from unknown (only numbered) fields
		// that were parsed above

		if !fieldValue.IsValid() {
			if unkField, ok := unkMap[s]; ok {
				fieldValue = reflect.ValueOf(unkField)
			} else {
				continue
			}
		}
		if _, err := fmt.Fprintf(h, "%v", fieldValue.Interface()); err != nil {
			return
		}
	}
}

func (m *ProtoProducerMessage) Key() []byte {
	if len(m.formatter.Keys()) == 0 {
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
	unkMap := make(map[string]interface{})

	fmr := m.ProtoReflect()
	unk := fmr.GetUnknown()
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

type fieldPart struct {
	finalName string
	array     bool
	length    int   // original slice length (used to mirror comma placement)
	indices   []int // original indices of appended items
	values    []valueRender
}

// renderFieldParts returns the per-field rendered values in formatter.Fields()
// order. Fields that are missing (and not virtual) map to nil so callers may
// either compact the output (text) or preserve column alignment (CSV).
func (m *ProtoProducerMessage) renderFieldParts() []*fieldPart {
	vfm := reflect.ValueOf(m)
	vfm = reflect.Indirect(vfm)

	fields := m.formatter.Fields()
	parts := make([]*fieldPart, len(fields))
	unkMap := m.mapUnknown()

	for idx, s := range fields {
		fieldName := s

		// constant (non-native) columns take precedence: emit the configured
		// value verbatim, honoring any rename for the display name.
		if lit, ok := m.formatter.Literal(s); ok {
			part := &fieldPart{
				values: []valueRender{{val: lit, isStr: true}},
			}
			if fieldRename, ok := m.formatter.Rename(s); ok && fieldRename != "" {
				part.finalName = fieldRename
			} else {
				part.finalName = s
			}
			parts[idx] = part
			continue
		}

		// get original name from structure
		if fieldNameMap, ok := m.formatter.Remap(fieldName); ok && fieldNameMap != "" {
			fieldName = fieldNameMap
		}

		// get renderer
		renderer, okRenderer := m.formatter.Render(fieldName)
		if !okRenderer {
			renderer = NilRenderer
		}

		fieldValue := vfm.FieldByName(fieldName)
		// if does not exist from structure,
		// fetch from unknown (only numbered) fields
		// that were parsed above

		if !fieldValue.IsValid() {
			if unkField, ok := unkMap[s]; ok {
				fieldValue = reflect.ValueOf(unkField)
			} else if !okRenderer { // not a virtual field
				continue
			}
		}

		isSlice := m.formatter.IsArray(fieldName)
		part := &fieldPart{array: isSlice}

		// render each item of the array independently
		// note: isSlice is necessary to consider certain byte arrays in their entirety
		// eg: IP addresses
		if isSlice {
			if fieldValue.IsValid() {
				c := fieldValue.Len()
				part.length = c
				for i := 0; i < c; i++ {
					fieldValueI := fieldValue.Index(i)
					var val interface{}
					if fieldValueI.IsValid() {
						val = fieldValueI.Interface()
					}

					rendered := renderer(m, fieldName, val)
					if rendered == nil {
						continue
					}
					renderedType := reflect.TypeOf(rendered)
					part.values = append(part.values, valueRender{
						val:   fmt.Sprintf("%v", rendered),
						isStr: renderedType.Kind() == reflect.String,
					})
					part.indices = append(part.indices, i)
				}
			}
		} else {
			var val interface{}
			if fieldValue.IsValid() {
				val = fieldValue.Interface()
			}

			rendered := renderer(m, fieldName, val)
			if rendered == nil {
				continue
			}
			renderedType := reflect.TypeOf(rendered)
			part.values = []valueRender{{
				val:   fmt.Sprintf("%v", rendered),
				isStr: renderedType.Kind() == reflect.String,
			}}
		}

		// rename only affects the display name of the field
		if fieldRename, ok := m.formatter.Rename(s); ok && fieldRename != "" {
			part.finalName = fieldRename
		} else {
			part.finalName = s
		}

		parts[idx] = part
	}
	return parts
}

// FormatMessageReflectCustom renders a custom-delimited representation.
func (m *ProtoProducerMessage) FormatMessageReflectCustom(ext, quotes, sep, sign string, null bool) string {
	fstr := make([]string, 0, len(m.formatter.Fields()))

	for _, part := range m.renderFieldParts() {
		if part == nil {
			continue
		}

		if part.array {
			v := "["

			for j, it := range part.values {
				if it.isStr {
					v += fmt.Sprintf("%s%v%s", quotes, it.val, quotes)
				} else {
					v += fmt.Sprintf("%v", it.val)
				}

				if part.indices[j] < part.length-1 {
					v += ","
				}
			}
			v += "]"
			fstr = append(fstr, fmt.Sprintf("%s%s%s%s%s", quotes, part.finalName, quotes, sign, v))
		} else {
			if part.values[0].isStr {
				fstr = append(fstr, fmt.Sprintf("%s%s%s%s%s%v%s", quotes, part.finalName, quotes, sign, quotes, part.values[0].val, quotes))
			} else {
				fstr = append(fstr, fmt.Sprintf("%s%s%s%s%v", quotes, part.finalName, quotes, sign, part.values[0].val))
			}
		}

	}

	return strings.Join(fstr, sep)
}

// CSVValues returns the rendered values in formatter.Fields() order. Missing
// (non-virtual) fields map to empty strings so columns stay aligned.
func (m *ProtoProducerMessage) CSVValues() []string {
	parts := m.renderFieldParts()
	out := make([]string, len(parts))
	for i, part := range parts {
		if part == nil {
			continue
		}
		if part.array {
			items := make([]string, len(part.values))
			for j, it := range part.values {
				items[j] = it.val
			}
			out[i] = "[" + strings.Join(items, ",") + "]"
		} else {
			out[i] = part.values[0].val
		}
	}
	return out
}
