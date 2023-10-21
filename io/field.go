package io

import (
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

// Field represents column mapped field
type (
	Field struct {
		Tag
		Column
		*xunsafe.Field
		EvalAddr    func(pointer unsafe.Pointer) interface{}
		Info        *sink.Column
		MatchesType bool
	}

	// Fields represents slice of Field
	Fields []Field
)

// ExtractColumnNames returns slice of column names for given Fields
func (f Fields) ColumnNames() []string {
	var result = make([]string, len(f))
	for i, field := range f {
		result[i] = field.Column.Name()
	}
	return result
}

// XFields returns slice of xunsafe.Field for given Fields
func (f Fields) XFields() []xunsafe.Field {
	var result = make([]xunsafe.Field, len(f))
	for i, field := range f {
		result[i] = *field.Field
	}
	return result
}

// Addr returns field pointer
func (f *Field) Addr(pointer unsafe.Pointer) interface{} {
	result := f.EvalAddr
	if f.EvalAddr == nil {
		return f.Field.Addr(pointer)
	}
	return result(pointer)
}

func (f *Field) buildEvalAddr(owner *Field) {
	if owner == nil {
		return
	}
	ownerType := owner.Type
	if ownerType.Kind() == reflect.Ptr {
		ownerType = ownerType.Elem()
	}
	addr := f.Field.Addr

	switch owner.Type.Kind() {
	case reflect.Struct:
		f.EvalAddr = func(pointer unsafe.Pointer) interface{} {
			ownerAddr := owner.Pointer(pointer)
			return addr(ownerAddr)
		}
	case reflect.Ptr:
		f.EvalAddr = func(pointer unsafe.Pointer) interface{} {
			ownerAddr := owner.Pointer(pointer)
			ptr := (*unsafe.Pointer)(ownerAddr)
			if *ptr == nil {
				newInstance := reflect.New(ownerType)
				*ptr = xunsafe.ValuePointer(&newInstance)
			}
			return addr(*ptr)
		}
	}
}
