package io

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

var timeType = reflect.TypeOf(time.Time{})
var timePtrType = reflect.TypeOf(&time.Time{})

func stringStringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			strPtr := field.StringPtr(pointer)
			if strPtr == nil || (nullifyZeroValue && *strPtr == "") {
				return nullValue, false
			}
			return *strPtr, true
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		str := field.String(pointer)
		if str == "" && nullifyZeroValue {
			return nullValue, false
		}
		return str, true
	}
}

func intStringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			intPtr := field.IntPtr(pointer)
			if intPtr == nil || (nullifyZeroValue && *intPtr == 0) {
				return nullValue, false
			}
			return strconv.Itoa(*intPtr), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		intValue := field.Int(pointer)
		if intValue == 0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.Itoa(intValue), false
	}
}

func int8Stringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			intPtr := field.Int8Ptr(pointer)
			if intPtr == nil || (nullifyZeroValue && *intPtr == 0) {
				return nullValue, false
			}
			return strconv.Itoa(int(*intPtr)), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		intValue := field.Int8(pointer)
		if intValue == 0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.Itoa(int(intValue)), false
	}
}

func int16Stringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			intPtr := field.Int16Ptr(pointer)
			if intPtr == nil || (nullifyZeroValue && *intPtr == 0) {
				return nullValue, false
			}
			return strconv.Itoa(int(*intPtr)), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		intValue := field.Int16(pointer)
		if intValue == 0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.Itoa(int(intValue)), false
	}
}

func int32Stringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			intPtr := field.Int32Ptr(pointer)
			if intPtr == nil || (nullifyZeroValue && *intPtr == 0) {
				return nullValue, false
			}
			return strconv.Itoa(int(*intPtr)), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		intValue := field.Int32(pointer)
		if intValue == 0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.Itoa(int(intValue)), false
	}
}

func int64Stringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			intPtr := field.Int64Ptr(pointer)
			if intPtr == nil || (nullifyZeroValue && *intPtr == 0) {
				return nullValue, false
			}
			return strconv.Itoa(int(*intPtr)), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		intValue := field.Int64(pointer)
		if intValue == 0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.Itoa(int(intValue)), false
	}
}

func uintStringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			intPtr := field.UintPtr(pointer)
			if intPtr == nil || (nullifyZeroValue && *intPtr == 0) {
				return nullValue, false
			}
			return strconv.Itoa(int(*intPtr)), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		intValue := field.Uint(pointer)
		if intValue == 0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.Itoa(int(intValue)), false
	}
}

func uint8Stringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			intPtr := field.Uint8Ptr(pointer)
			if intPtr == nil || (nullifyZeroValue && *intPtr == 0) {
				return nullValue, false
			}
			return strconv.Itoa(int(*intPtr)), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		intValue := field.Uint8(pointer)
		if intValue == 0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.Itoa(int(intValue)), false
	}
}

func uint16Stringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			intPtr := field.Uint16Ptr(pointer)
			if intPtr == nil || (nullifyZeroValue && *intPtr == 0) {
				return nullValue, false
			}
			return strconv.Itoa(int(*intPtr)), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		intValue := field.Uint16(pointer)
		if intValue == 0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.Itoa(int(intValue)), false
	}
}

func uint32Stringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			intPtr := field.Uint32Ptr(pointer)
			if intPtr == nil || (nullifyZeroValue && *intPtr == 0) {
				return nullValue, false
			}
			return strconv.Itoa(int(*intPtr)), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		intValue := field.Uint32(pointer)
		if intValue == 0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.Itoa(int(intValue)), false
	}
}

func uint64Stringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			intPtr := field.Uint64Ptr(pointer)
			if intPtr == nil || (nullifyZeroValue && *intPtr == 0) {
				return nullValue, false
			}
			return strconv.Itoa(int(*intPtr)), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		intValue := field.Uint64(pointer)
		if intValue == 0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.Itoa(int(intValue)), false
	}
}

func boolStringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			valuePtr := field.BoolPtr(pointer)
			if valuePtr == nil || (nullifyZeroValue && *valuePtr == false) {
				return nullValue, false
			}
			return strconv.FormatBool(*valuePtr), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		value := field.Bool(pointer)
		if !value && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.FormatBool(value), false
	}
}

//TODO: Float precision
func float64Stringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			valuePtr := field.Float64Ptr(pointer)
			if valuePtr == nil || (nullifyZeroValue && *valuePtr == 0) {
				return nullValue, false
			}
			return strconv.FormatFloat(*valuePtr, 'f', 64, 64), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		value := field.Float64(pointer)
		if value == 0.0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.FormatFloat(value, 'f', 64, 64), false
	}
}

//TODO: Float precision
func float32Stringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string, wasPointer bool) fieldStringifier {
	if wasPointer {
		return func(pointer unsafe.Pointer) (string, bool) {
			valuePtr := field.Float32Ptr(pointer)
			if valuePtr == nil || (nullifyZeroValue && *valuePtr == 0) {
				return nullValue, false
			}
			return strconv.FormatFloat(float64(*valuePtr), 'f', 64, 64), false
		}
	}

	return func(pointer unsafe.Pointer) (string, bool) {
		value := field.Float32(pointer)
		if value == 0.0 && nullifyZeroValue {
			return nullValue, false
		}
		return strconv.FormatFloat(float64(value), 'f', 64, 64), false
	}
}

//TODO: Time format
func defaultStringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string) fieldStringifier {
	if field.Type == timeType {
		return func(pointer unsafe.Pointer) (string, bool) {
			value := field.Time(pointer)
			if value.IsZero() && nullifyZeroValue {
				return nullValue, false
			}
			return value.Format(time.RFC3339), true
		}
	}

	if field.Type == timePtrType {
		return func(pointer unsafe.Pointer) (string, bool) {
			ptr := field.TimePtr(pointer)
			if ptr == nil || (nullifyZeroValue && *ptr == time.Time{}) {
				return nullValue, false
			}
			return ptr.Format(time.RFC3339), true
		}
	}

	return func(pointer unsafe.Pointer) (value string, wasString bool) {
		i := field.Value(pointer)
		return fmt.Sprintf("%v", i), false
	}
}
