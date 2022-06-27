package io

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

//Matcher implements column to struct filed mapper
type Matcher struct {
	resolver Resolve
	tagName  string
}

//Match matches field with columns
func (f *Matcher) Match(targetType reflect.Type, columns []Column) ([]Field, error) {
	if len(columns) == 0 {
		var err error
		if columns, err = StructColumns(targetType, f.tagName); err != nil {
			return nil, fmt.Errorf("failed to create column for struct: %v, %w", targetType.String(), err)
		}
	}
	xStruct := xunsafe.NewStruct(targetType)
	var matched = make([]Field, len(columns))
	return matched, f.matchedColumns(xStruct, matched, columns)
}

func (f *Matcher) matchedColumns(xStruct *xunsafe.Struct, matched []Field, columns []Column) error {
	var idx = make(index, len(xStruct.Fields)*3)       //create index to map various version of field name to the column name
	var fields = make([]Field, 0, len(xStruct.Fields)) //all struct field matching candidates
	if err := f.indexFields(idx, nil, xStruct, &fields); err != nil {
		return err
	}
	var unmatched []int
	for i := range matched {
		candidate := &matched[i]
		column := columns[i]
		candidate.Column = column
		pos := idx.match(column.Name())
		if pos == -1 {
			unmatched = append(unmatched, i)
			continue
		}
		if column.ScanType() != nil {
			if column.ScanType().Kind() == reflect.Ptr {

			}
			fmt.Printf("%v %v %v\n", column.Name(), column.ScanType(), fields[pos].Type)
		}
		fields[pos].Column = column
		matched[i] = fields[pos]
	}
	if len(unmatched) == 0 {
		return nil
	}
	var unmatchedColumn = make([]string, 0)
	if f.resolver == nil {
		for _, pos := range unmatched {
			unmatchedColumn = append(unmatchedColumn, matched[pos].Column.Name())
		}
		return fmt.Errorf("failed to match columns: %v", unmatchedColumn)
	}
	for _, pos := range unmatched {
		candidate := &matched[pos]
		candidate.Field = &xunsafe.Field{
			Name: candidate.Column.Name(),
			Type: candidate.Column.ScanType(),
		}
		candidate.EvalAddr = f.resolver(candidate.Column)
		if candidate.EvalAddr == nil {
			return fmt.Errorf("failed to match column: %v", candidate.Column.Name())
		}
	}
	return nil
}

func (f *Matcher) indexFields(idx index, owner *Field, xStruct *xunsafe.Struct, fields *[]Field) error {
	ns := ""
	if owner != nil {
		ns = owner.Tag.Ns
	}
	for i := range xStruct.Fields {
		structField := &xStruct.Fields[i]
		field := Field{
			Field: structField,
		}
		field.buildEvalAddr(owner)
		tag := structField.Tag.Get(f.tagName)
		if parsed := ParseTag(tag); parsed != nil {
			field.Tag = *parsed
		}
		if field.Transient {
			continue
		}
		if field.CanExpand() {
			if err := f.indexFieldStructFields(&field, idx, fields); err != nil {
				return err
			}
			continue
		}
		f.indexField(idx, ns, &field, len(*fields))
		*fields = append(*fields, field)
	}
	return nil
}

func (f *Matcher) indexFieldStructFields(owner *Field, idx index, dest *[]Field) error {
	structType := owner.Type
	if owner.Type.Kind() == reflect.Ptr {
		structType = structType.Elem()
	}
	xStruct := xunsafe.NewStruct(structType)
	return f.indexFields(idx, owner, xStruct, dest)
}

func (f *Matcher) indexField(idx index, ns string, field *Field, pos int) {
	if field.Tag.Transient {
		return
	}
	if field.Tag.Column != "" {
		for _, name := range strings.Split(field.Tag.Column, "|") {
			idx.add(ns+name, pos)
		}
	}
	idx.add(ns+field.Field.Name, pos)
}

//NewMatcher creates a fields to column matcher
func NewMatcher(tagName string, resolver Resolve) *Matcher {
	fields := &Matcher{
		resolver: resolver,
		tagName:  tagName,
	}
	return fields
}
