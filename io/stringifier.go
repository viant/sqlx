package io

import (
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

//ObjectStringifier returns stringified object properties values and information if value was string before
type ObjectStringifier = func(val interface{}) ([]string, []bool)
type fieldStringifier = func(pointer unsafe.Pointer) (string, bool)

//TypeStringifier returns ObjectStringifier for a given Type.
//It will replace nil values with nullValue for properties with tag: "nullify" and omit (if specified) transient properties
//results are shared, no new arrays are returned
func TypeStringifier(rType reflect.Type, nullValue string, omitTransient bool) ObjectStringifier {
	fieldLen := rType.NumField()

	stringifiers := make([]fieldStringifier, 0)

	for i := 0; i < fieldLen; i++ {
		field := rType.Field(i)
		tag := ParseTag(field.Tag.Get(option.TagSqlx))
		if tag.Transient && omitTransient {
			continue
		}
		stringifiers = append(stringifiers, stringifier(xunsafe.NewField(field), tag.NullifyEmpty, nullValue))
	}

	stringifiersLen := len(stringifiers)
	strings := make([]string, stringifiersLen)
	wasStrings := make([]bool, stringifiersLen)
	return func(val interface{}) ([]string, []bool) {
		ptr := xunsafe.AsPointer(val)
		for i := 0; i < stringifiersLen; i++ {
			strings[i], wasStrings[i] = stringifiers[i](ptr)
		}

		return strings, wasStrings
	}
}

func stringifier(xField *xunsafe.Field, nullifyEmpty bool, nullValue string) fieldStringifier {
	preparedStringifier := prepareStringifier(xField, nullifyEmpty, nullValue)
	return func(val unsafe.Pointer) (value string, wasString bool) {
		return preparedStringifier(val)
	}
}

func prepareStringifier(field *xunsafe.Field, nullifyZeroValue bool, nullValue string) fieldStringifier {
	wasPointer := field.Type.Kind() == reflect.Ptr
	var rType reflect.Type
	if wasPointer {
		rType = field.Elem()
	} else {
		rType = field.Type
	}

	switch rType.Kind() {
	case reflect.String:
		return stringStringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Int:
		return intStringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Int8:
		return int8Stringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Int16:
		return int16Stringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Int32:
		return int32Stringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Int64:
		return int64Stringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Uint:
		return uintStringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Uint8:
		return uint8Stringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Uint16:
		return uint16Stringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Uint32:
		return uint32Stringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Uint64:
		return uint64Stringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Bool:
		return boolStringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Float64:
		return float64Stringifier(field, nullifyZeroValue, nullValue, wasPointer)
	case reflect.Float32:
		return float32Stringifier(field, nullifyZeroValue, nullValue, wasPointer)
	default:
		return defaultStringifier(field, nullifyZeroValue, nullValue)
	}
}
