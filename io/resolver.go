package io

import (
	"reflect"
	"unsafe"
)

//Resolve Resolver handler unresolved columns
type Resolve func(column Column) func(pointer unsafe.Pointer) interface{}

//Resolver represents unmatched column resolver
type Resolver struct {
	columns []Column
	data    [][]interface{}
	ptrs    []unsafe.Pointer
}

//Index returns column index or -1
func (r *Resolver) Index(column string) int {
	for i, candidate := range r.columns {
		if candidate.Name() == column {
			return i
		}
	}
	return -1
}

//Data returns column data
func (r *Resolver) Data(index int) []interface{} {
	if index >= len(r.data) {
		return []interface{}{}
	}

	return r.data[index]
}

//Resolve resolved unmapped column
func (r *Resolver) Resolve(column Column) func(ptr unsafe.Pointer) interface{} {
	index := len(r.columns)
	r.columns = append(r.columns, column)
	r.data = append(r.data, make([]interface{}, 0))
	return func(ptr unsafe.Pointer) interface{} {
		if index == 0 {
			r.ptrs = append(r.ptrs, ptr)
		}
		value := reflect.New(column.ScanType())
		result := value.Interface()
		r.data[index] = append(r.data[index], result)
		return result
	}
}

func (r *Resolver) OnSkip(values []interface{}) error {
	if len(r.data) == 0 && len(r.data[0]) == 0 {
		return nil
	}

	r.data[0] = r.data[0][:len(r.data[0])-1]
	return nil
}

//NewResolver creates a resolver
func NewResolver() *Resolver {
	return &Resolver{}
}
