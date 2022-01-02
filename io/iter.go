package io

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
)

//Iterator creates an iterator for any data structure, it returns next function, len, or error
func Iterator(any interface{}) (func() interface{}, int, error) {
	switch actual := any.(type) {
	case []interface{}:
		i := 0
		return func() interface{} {
			if i >= len(actual) {
				return nil
			}
			result := actual[i]
			i++
			return result
		}, len(actual), nil
	case func(any interface{}) (func() interface{}, int, error):
		return actual(any)
	default:
		anyValue := reflect.ValueOf(any)
		switch anyValue.Kind() {
		case reflect.Ptr, reflect.Struct:
			val := actual
			return func() interface{} {
				result := val
				val = nil
				return result
			}, 1, nil

		case reflect.Slice:
			ptr := xunsafe.AsPointer(actual)
			aSliceType := xunsafe.NewSlice(reflect.TypeOf(actual))
			sliceLen := aSliceType.Len(ptr)
			i := 0
			return func() interface{} {
				if i >= sliceLen {
					return nil
				}
				result := aSliceType.ValuePointerAt(ptr, i)
				i++
				return result
			}, sliceLen, nil
		}
	}
	return nil, 0, fmt.Errorf("usnupported :%T", any)
}
