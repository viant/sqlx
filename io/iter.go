package io

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
)

//Iterator creates an iterator for any data structure, it returns next function, len, or error
func Iterator(any interface{}) (func() interface{}, int, error) {
	next, size, err := Values(any)
	if err != nil {
		return nil, 0, err
	}

	i := -1
	return func() interface{} {
		i++
		if i >= size {
			return nil
		}

		return next(i)
	}, size, err
}

//Values return function to access value at position
func Values(any interface{}) (func(index int) interface{}, int, error) {
	switch actual := any.(type) {
	case []interface{}:
		return func(index int) interface{} {
			return actual[index]
		}, len(actual), nil
	case func(any interface{}) (func(index int) interface{}, int, error):
		return actual(any)
	default:
		anyValue := reflect.ValueOf(any)
		switch anyValue.Kind() {
		case reflect.Ptr, reflect.Struct:
			val := actual
			return func(index int) interface{} {
				result := val
				return result
			}, 1, nil

		case reflect.Slice:
			ptr := xunsafe.AsPointer(actual)
			aSliceType := xunsafe.NewSlice(reflect.TypeOf(actual))
			sliceLen := aSliceType.Len(ptr)
			return func(index int) interface{} {
				result := aSliceType.ValuePointerAt(ptr, index)
				return result
			}, sliceLen, nil
		}
	}
	return nil, 0, fmt.Errorf("usnupported :%T", any)
}
