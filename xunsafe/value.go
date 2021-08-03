package xunsafe

import (
	"fmt"
	"reflect"
	"unsafe"
)

//FieldValue creates Getter function for a field value or error
func FieldValue(structType reflect.Type, fieldPath *Field) (Getter, error) {
	if structType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct but had: %T", reflect.New(structType))
	}
	field := structType.Field(fieldPath.Index)
	offset := field.Offset
	var result Getter
	if fieldPath.Getter != nil {
		return func(structAddr uintptr) interface{} {
			return fieldPath.Getter(structAddr)
		}, nil
	}
	switch field.Type.Kind() {
	case reflect.Int:
		result = func(structAddr uintptr) interface{} {
			result := (*int)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.Uint:
		result = func(structAddr uintptr) interface{} {
			result := (*uint)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.Int64:
		result = func(structAddr uintptr) interface{} {
			result := (*int64)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.Int32:
		result = func(structAddr uintptr) interface{} {
			result := (*int32)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.Int16:
		result = func(structAddr uintptr) interface{} {
			result := (*int16)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.Int8:
		result = func(structAddr uintptr) interface{} {
			result := (*int8)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.Uint64:
		result = func(structAddr uintptr) interface{} {
			result := (*uint64)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.Uint32:
		result = func(structAddr uintptr) interface{} {
			result := (*uint32)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.Uint16:
		result = func(structAddr uintptr) interface{} {
			result := (*uint16)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.Uint8:
		result = func(structAddr uintptr) interface{} {
			result := (*uint8)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.String:
		result = func(structAddr uintptr) interface{} {
			result := (*string)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.Float64:
		result = func(structAddr uintptr) interface{} {
			result := (*float64)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}

	case reflect.Float32:
		result = func(structAddr uintptr) interface{} {
			result := (*float32)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}
	case reflect.Bool:
		result = func(structAddr uintptr) interface{} {
			result := (*bool)(unsafe.Pointer(structAddr + offset))
			if result == nil {
				return nil
			}
			return *result
		}

	case reflect.Struct:
		if fieldPath.Field == nil {
			return func(structAddr uintptr) interface{} {
				fieldValue := reflect.NewAt(field.Type, unsafe.Pointer(structAddr+offset))
				return fieldValue.Interface()
			}, nil
		}

		fn, err := FieldPointer(field.Type, fieldPath.Field)
		if err != nil {
			return nil, fmt.Errorf("failed to get poiner on %v.%v due to %w", structType.String(), field.Name, err)
		}
		result = func(structAddr uintptr) interface{} {
			fieldValue := reflect.NewAt(field.Type, unsafe.Pointer(structAddr+offset))
			addr := fieldValue.Elem().UnsafeAddr()
			return fn(addr)
		}

	case reflect.Ptr:
		switch field.Type.Elem().Kind() {
		case reflect.Struct:
			if fieldPath.Field == nil {
				return func(structAddr uintptr) interface{} {
					fieldValue := reflect.NewAt(field.Type, unsafe.Pointer(structAddr+offset))
					if fieldValue.Elem().IsNil() {
						ptr := reflect.New(fieldValue.Type().Elem().Elem())
						fieldValue.Elem().Set(ptr)
					}
					return fieldValue.Interface()
				}, nil
			}
			fn, err := FieldPointer(field.Type.Elem(), fieldPath.Field)
			if err != nil {
				return nil, fmt.Errorf("failed to get poiner on %v.%v due to %w", structType.String(), field.Name, err)
			}
			result = func(structAddr uintptr) interface{} {
				fieldValue := reflect.NewAt(field.Type, unsafe.Pointer(structAddr+offset))
				if fieldValue.Elem().IsNil() {
					ptr := reflect.New(fieldValue.Type().Elem().Elem())
					fieldValue.Elem().Set(ptr)
				}
				return fn(fieldValue.Elem().Elem().UnsafeAddr())
			}

		case reflect.Int:
			result = func(structAddr uintptr) interface{} {
				result := (**int)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Uint:
			result = func(structAddr uintptr) interface{} {
				result := (**uint)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Int64:
			result = func(structAddr uintptr) interface{} {
				result := (**int64)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Int32:
			result = func(structAddr uintptr) interface{} {
				result := (**int32)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Int16:
			result = func(structAddr uintptr) interface{} {
				result := (**int16)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Int8:
			result = func(structAddr uintptr) interface{} {
				result := (**int8)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Uint64:
			result = func(structAddr uintptr) interface{} {
				result := (**uint64)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Uint32:
			result = func(structAddr uintptr) interface{} {
				result := (**uint32)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Uint16:
			result = func(structAddr uintptr) interface{} {
				result := (**uint16)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Uint8:
			result = func(structAddr uintptr) interface{} {
				result := (**uint8)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.String:
			result = func(structAddr uintptr) interface{} {
				result := (**string)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Float64:
			result = func(structAddr uintptr) interface{} {
				result := (**float64)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}

		case reflect.Float32:
			result = func(structAddr uintptr) interface{} {
				result := (**float32)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Bool:
			result = func(structAddr uintptr) interface{} {
				result := (**bool)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Slice:
			switch field.Type.Elem().Elem().Kind() {
			case reflect.Int:
				result = func(structAddr uintptr) interface{} {
					result := (**[]int)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.Uint:
				result = func(structAddr uintptr) interface{} {
					result := (**[]uint)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.Int64:
				result = func(structAddr uintptr) interface{} {
					result := (**[]int64)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.Int32:
				result = func(structAddr uintptr) interface{} {
					result := (**[]int32)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.Int16:
				result = func(structAddr uintptr) interface{} {
					result := (**[]int16)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.Int8:
				result = func(structAddr uintptr) interface{} {
					result := (**[]int8)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.Uint64:
				result = func(structAddr uintptr) interface{} {
					result := (**[]uint64)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.Uint32:
				result = func(structAddr uintptr) interface{} {
					result := (**[]uint32)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.Uint16:
				result = func(structAddr uintptr) interface{} {
					result := (**[]uint16)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.Uint8:
				result = func(structAddr uintptr) interface{} {
					result := (**[]uint8)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.String:
				result = func(structAddr uintptr) interface{} {
					result := (**[]string)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.Float64:
				result = func(structAddr uintptr) interface{} {
					result := (**[]float64)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}

			case reflect.Float32:
				result = func(structAddr uintptr) interface{} {
					result := (**[]float32)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			case reflect.Bool:
				result = func(structAddr uintptr) interface{} {
					result := (**[]bool)(unsafe.Pointer(structAddr + offset))
					if result == nil {
						return nil
					}
					return *result
				}
			default:
				return raiseUnsupportedTypeError(structType, field)
			}

		default:
			return raiseUnsupportedTypeError(structType, field)
		}
	case reflect.Slice:
		switch field.Type.Elem().Kind() {
		case reflect.Int:
			result = func(structAddr uintptr) interface{} {
				result := (*[]int)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Uint:
			result = func(structAddr uintptr) interface{} {
				result := (*[]uint)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Int64:
			result = func(structAddr uintptr) interface{} {
				result := (*[]int64)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Int32:
			result = func(structAddr uintptr) interface{} {
				result := (*[]int32)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Int16:
			result = func(structAddr uintptr) interface{} {
				result := (*[]int16)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Int8:
			result = func(structAddr uintptr) interface{} {
				result := (*[]int8)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Uint64:
			result = func(structAddr uintptr) interface{} {
				result := (*[]uint64)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Uint32:
			result = func(structAddr uintptr) interface{} {
				result := (*[]uint32)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Uint16:
			result = func(structAddr uintptr) interface{} {
				result := (*[]uint16)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Uint8:
			result = func(structAddr uintptr) interface{} {
				result := (*[]uint8)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.String:
			result = func(structAddr uintptr) interface{} {
				result := (*[]string)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Float64:
			result = func(structAddr uintptr) interface{} {
				result := (*[]float64)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}

		case reflect.Float32:
			result = func(structAddr uintptr) interface{} {
				result := (*[]float32)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		case reflect.Bool:
			result = func(structAddr uintptr) interface{} {
				result := (*[]bool)(unsafe.Pointer(structAddr + offset))
				if result == nil {
					return nil
				}
				return *result
			}
		default:
			return raiseUnsupportedTypeError(structType, field)
		}
	default:
		return raiseUnsupportedTypeError(structType, field)
	}
	return result, nil
}

