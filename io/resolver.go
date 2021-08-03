package io

import "reflect"

//Resolve Resolver handler unresolved columns
type Resolve func(column Column) func(ptr uintptr) interface{}


//Resolver represents unmatched column resolver
type Resolver struct {
	columns []Column
	data    [][]interface{}
	ptrs    []uintptr
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
	return r.data[index]
}

func (r *Resolver) Resolve(column Column) func(ptr uintptr) interface{} {
	index := len(r.columns)
	r.columns = append(r.columns, column)
	r.data = append(r.data, make([]interface{}, 0))
	return func(ptr uintptr) interface{} {
		if index == 0 {
			r.ptrs = append(r.ptrs, ptr)
		}
		value := reflect.New(column.ScanType())
		result := value.Interface()
		r.data[index] = append(r.data[index], result)
		return result
	}
}

//NewResolver creates a resolver
func NewResolver() *Resolver {
	return &Resolver{}
}
