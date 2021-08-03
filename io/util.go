package io

import (
	"reflect"
)


func asDereferenceSlice(aSlice []interface{}) {
	for i, value := range aSlice {
		if value == nil {
			continue
		}
		aSlice[i] = reflect.ValueOf(value).Elem().Interface()

	}
}

func updateMap(columns []Column, values []interface{}, target map[string]interface{}) {
	for i, column := range columns {
		target[column.Name()] = values[i]
	}
}

func holderPointer(record interface{}) uintptr {
	value := reflect.ValueOf(record)
	if value.Kind() != reflect.Ptr { //convert to a pointer
		vp := reflect.New(value.Type())
		vp.Elem().Set(value)
		value = vp
	}
	holderPtr := value.Elem().UnsafeAddr()
	return holderPtr
}

//IsBaseType return true if base type
func IsBaseType(aType reflect.Type) bool {
	if aType.Kind() == reflect.Ptr {
		aType = aType.Elem()
	}
	switch aType.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Uint, reflect.Uint64, reflect.Uint8, reflect.Uint16, reflect.Uint32,
		reflect.Float32, reflect.Float64,
		reflect.Bool, reflect.String, reflect.Slice:
		return true
	default:
		if byteType.AssignableTo(aType) || timeType.AssignableTo(aType) {
			return true
		}
	}
	return false
}
