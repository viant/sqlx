package io

import (
	"fmt"
	"reflect"
)

func AnyProvider(any interface{}) (func() interface{}, error) {
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
		}, nil
	case func() interface{}:
		return actual, nil
	default:
		anyValue := reflect.ValueOf(any)
		switch anyValue.Kind() {
		case reflect.Ptr, reflect.Struct:
			val := actual
			return func() interface{} {
				result := val
				val = nil
				return result
			}, nil

		case reflect.Slice:
			anyLength := anyValue.Len()
			i := 0
			return func() interface{} {
				if i >= anyLength {
					return nil
				}
				resultValue := anyValue.Index(i)
				if resultValue.Kind() != reflect.Ptr {
					resultValue = resultValue.Addr()
				}
				result := resultValue.Interface()
				i++
				return result
			}, nil
		}
	}
	return nil, fmt.Errorf("usnupported :%T", any)
}


