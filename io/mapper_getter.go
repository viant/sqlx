package io

import (
	"reflect"
	"unsafe"

	"github.com/viant/xunsafe"
)

// columnGetter follows the mapper's normalized holder path before applying the
// unchanged leaf getter/encoder. Missing pointer holders bind SQL NULL and must
// not allocate or modify the caller's object graph.
func columnGetter(tag *Tag, fields []*xunsafe.Field, recordType reflect.Type) (xunsafe.Getter, error) {
	leaf, err := fieldGetter(tag, fields[len(fields)-1], recordType)
	if err != nil || len(fields) == 1 {
		return leaf, err
	}
	holders := append([]*xunsafe.Field(nil), fields[:len(fields)-1]...)
	return func(pointer unsafe.Pointer) interface{} {
		for _, holder := range holders {
			if pointer == nil {
				return nil
			}
			pointer = holder.Pointer(pointer)
			for typ := holder.Type; typ.Kind() == reflect.Ptr; typ = typ.Elem() {
				pointer = xunsafe.DerefPointer(pointer)
				if pointer == nil {
					return nil
				}
			}
		}
		if pointer == nil {
			return nil
		}
		return leaf(pointer)
	}, nil
}
