package protoproducer

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
)

// Using a data slice, returns a chunk corresponding
// GetBytes slices a byte region with optional left shift for bit offsets.
func GetBytes(d []byte, offset, length int, shift bool) []byte {

	/*

		Example with an offset of 4 and length of 6

		initial data:
		0xAA 0x55
		  1010 1010.0101 0101
		       ^--- -^

		with shift
		0x29
		  0010 1001

		without shift (bitwise AND)
		0xa4
		  1010 0100


	*/

	if len(d)*8 < offset {
		return nil
	}
	if length == 0 {
		return nil
	}

	shiftSize := offset % 8      // how much to shift to the left due to offset
	shiftRightSize := length % 8 // for final step

	start := offset / 8
	end := (offset + length) / 8
	if (offset+length)%8 > 0 {
		end += 1
	}

	lengthB := length / 8
	if shiftRightSize > 0 {
		lengthB += 1
	}

	missing := end - len(d) // calculate how many missing bytes
	if missing > 0 {
		end = len(d)
	}

	dUsed := d[start:end]

	if shiftSize == 0 && length%8 == 0 { // simple case
		if missing > 0 {
			dFinal := make([]byte, lengthB)

			copy(dFinal, dUsed)
			return dFinal
		}
		return dUsed
	}

	dFinal := make([]byte, lengthB)

	// first pass, apply offset
	for i := range dFinal {
		if i >= len(dUsed) {
			break
		}
		left := dUsed[i] << shiftSize

		dFinal[i] = left
		if i+1 >= len(dUsed) {
			break
		}
		right := dUsed[i+1] >> (8 - shiftSize)
		dFinal[i] |= right
	}

	// final pass
	if shift {
		dFinal[len(dFinal)-1] >>= (8 - shiftRightSize) % 8
	} else {
		dFinal[len(dFinal)-1] &= (0xFF << ((8 - shiftRightSize) % 8))
	}

	return dFinal
}

// IsUInt reports whether a reflect.Kind is an unsigned integer type.
func IsUInt(k reflect.Kind) bool {
	return k == reflect.Uint8 || k == reflect.Uint16 || k == reflect.Uint32 || k == reflect.Uint64
}

// IsInt reports whether a reflect.Kind is a signed integer type.
func IsInt(k reflect.Kind) bool {
	return k == reflect.Int8 || k == reflect.Int16 || k == reflect.Int32 || k == reflect.Int64
}

// resolvedFieldIndex 返回 cfg 在配置构建期就解析好的反射字段下标。
//
// 返回值 resolved 表示"绑定是否已解析"：
//   - resolved == false：cfg 不是经映射器构建的临时配置，调用方必须回退到
//     reflect.FieldByName（线性扫描 + 分配，仅出现在临时配置/测试路径）；
//   - resolved == true 且 len(idx) > 0：目标是 protobuf 结构体字段，直接按
//     下标取值；
//   - resolved == true 且 len(idx) == 0：目标不是 Go 字段（如 snake_case 的
//     手动 protobuf 字段，或 destination 为空），此时应跳过 FieldByName，
//     直接走 protobuf unknown 路径。构建期与运行期类型一致，故跳过不改变行为。
func resolvedFieldIndex(cfg MappableField) ([]int, bool) {
	if fi, ok := cfg.(interface{ GetFieldIndex() []int }); ok {
		if idx := fi.GetFieldIndex(); idx != nil {
			return idx, true
		}
	}
	return nil, false
}

// MapCustomNetFlow maps a NetFlow data field into a flow message using a mapper.
func MapCustomNetFlow(flowMessage *ProtoProducerMessage, df netflow.DataField, mapper TemplateMapper) error {
	if mapper == nil {
		return nil
	}
	mapped, ok := mapper.Map(df)
	if ok {
		v := df.Value.([]byte)
		if err := MapCustom(flowMessage, v, mapped); err != nil {
			return fmt.Errorf("map custom netflow field %d: %w", df.Type, err)
		}
	}
	return nil
}

// MapCustom maps raw bytes into a flow message field using a mapper.
func MapCustom(flowMessage *ProtoProducerMessage, v []byte, cfg MappableField) error {
	destLabel := cfg.GetDestination()
	if destLabel == "" {
		destLabel = fmt.Sprintf("proto index %d", cfg.GetProtoIndex())
	}

	vfm := reflect.ValueOf(flowMessage)
	vfm = reflect.Indirect(vfm)

	// 优先使用映射器构建期解析好的绑定：已解析且有下标 → 按下标取值（零查找开销）；
	// 已解析但目标不是 Go 字段 → 完全跳过 FieldByName；只有未解析的临时配置才回退
	// 到 FieldByName（线性扫描 + 分配）。
	var fieldValue reflect.Value
	if idx, resolved := resolvedFieldIndex(cfg); resolved {
		if len(idx) > 0 {
			fieldValue = vfm.FieldByIndex(idx)
		}
	} else {
		fieldValue = vfm.FieldByName(cfg.GetDestination())
	}

	if fieldValue.IsValid() {
		typeDest := fieldValue.Type()
		fieldValueAddr := fieldValue.Addr()

		if typeDest.Kind() == reflect.Slice {

			if typeDest.Elem().Kind() == reflect.Uint8 {
				fieldValue.SetBytes(v)
			} else {
				item := reflect.New(typeDest.Elem())

				if IsUInt(typeDest.Elem().Kind()) {
					if cfg.GetEndianness() == LittleEndian {
						if err := DecodeUNumberLE(v, item.Interface()); err != nil {
							return fmt.Errorf("map custom %s: decode uint (LE): %w", destLabel, err)
						}
					} else {
						if err := DecodeUNumber(v, item.Interface()); err != nil {
							return fmt.Errorf("map custom %s: decode uint: %w", destLabel, err)
						}
					}
				} else if IsInt(typeDest.Elem().Kind()) {
					if cfg.GetEndianness() == LittleEndian {
						if err := DecodeNumberLE(v, item.Interface()); err != nil {
							return fmt.Errorf("map custom %s: decode int (LE): %w", destLabel, err)
						}
					} else {
						if err := DecodeNumber(v, item.Interface()); err != nil {
							return fmt.Errorf("map custom %s: decode int: %w", destLabel, err)
						}
					}
				}

				itemi := reflect.Indirect(item)
				tmpFieldValue := reflect.Append(fieldValue, itemi)
				fieldValue.Set(tmpFieldValue)
			}

		} else if fieldValueAddr.IsValid() {
			if IsUInt(typeDest.Kind()) {
				if cfg.GetEndianness() == LittleEndian {
					if err := DecodeUNumberLE(v, fieldValueAddr.Interface()); err != nil {
						return fmt.Errorf("map custom %s: decode uint (LE): %w", destLabel, err)
					}
				} else {
					if err := DecodeUNumber(v, fieldValueAddr.Interface()); err != nil {
						return fmt.Errorf("map custom %s: decode uint: %w", destLabel, err)
					}
				}
			} else if IsInt(typeDest.Kind()) {
				if cfg.GetEndianness() == LittleEndian {
					if err := DecodeNumberLE(v, fieldValueAddr.Interface()); err != nil {
						return fmt.Errorf("map custom %s: decode int (LE): %w", destLabel, err)
					}
				} else {
					if err := DecodeNumber(v, fieldValueAddr.Interface()); err != nil {
						return fmt.Errorf("map custom %s: decode int: %w", destLabel, err)
					}
				}
			}

		}
	} else if cfg.GetProtoIndex() > 0 {

		fmr := flowMessage.ProtoReflect()
		unk := fmr.GetUnknown()

		if !cfg.IsArray() {
			var offset int
			for offset < len(unk) {
				num, _, length := protowire.ConsumeField(unk[offset:])
				offset += length
				if int32(num) == cfg.GetProtoIndex() {
					// only one allowed
					break
				}
			}
		}

		var dstVar uint64
		if cfg.GetProtoType() == ProtoVarint {
			if cfg.GetEndianness() == LittleEndian {
				if err := DecodeUNumberLE(v, &dstVar); err != nil {
					return fmt.Errorf("map custom %s: decode varint (LE): %w", destLabel, err)
				}
			} else {
				if err := DecodeUNumber(v, &dstVar); err != nil {
					return fmt.Errorf("map custom %s: decode varint: %w", destLabel, err)
				}
			}
			// support signed int?
			unk = protowire.AppendTag(unk, protowire.Number(cfg.GetProtoIndex()), protowire.VarintType)
			unk = protowire.AppendVarint(unk, dstVar)
		} else if cfg.GetProtoType() == ProtoString {
			unk = protowire.AppendTag(unk, protowire.Number(cfg.GetProtoIndex()), protowire.BytesType)
			unk = protowire.AppendString(unk, string(v))
		} else {
			return fmt.Errorf("map custom %s: could not insert into protobuf unknown", destLabel)
		}
		fmr.SetUnknown(unk)
	}
	return nil
}
