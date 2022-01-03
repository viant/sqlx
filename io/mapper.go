package io

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

//ColumnMapper maps src to columns and its placeholders
type ColumnMapper func(src interface{}, tagName string) ([]Column, PlaceholderBinder, error)

//GenericColumnMapper returns genertic column mapper
func GenericColumnMapper(src interface{}, tagName string) ([]Column, PlaceholderBinder, error) {
	recordType := reflect.TypeOf(src)
	if recordType.Kind() == reflect.Ptr {
		recordType = recordType.Elem()
	}
	if recordType.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("invalid record type: %v", recordType.Kind())
	}

	var columns []Column
	var identityColumns []Column
	var getters []xunsafe.Getter

	for i := 0; i < recordType.NumField(); i++ {
		field := recordType.Field(i)
		if isExported := field.PkgPath == ""; !isExported {
			continue
		}
		tag := ParseTag(field.Tag.Get(tagName))
		if tag.Transient {
			continue
		}
		columnName := field.Name
		if tag.Column != "" {
			columnName = tag.Column
		}
		if tag.Autoincrement || tag.PrimaryKey || strings.ToLower(columnName) == "id" {
			if tag == nil {
				tag = &Tag{Column: columnName, PrimaryKey: true}
			}
			tag.FieldIndex = i
			identityColumns = append(identityColumns, NewColumn(columnName, "", field.Type, tag))
			continue
		}
		columns = append(columns, NewColumn(columnName, "", field.Type, tag))
		getter := xunsafe.FieldByIndex(recordType, i).Addr
		getters = append(getters, getter)
	}

	//make sure identity columns are at the end
	if len(identityColumns) > 0 {
		for i, item := range identityColumns {
			fieldIndex := item.Tag().FieldIndex
			getter := xunsafe.FieldByIndex(recordType, fieldIndex).Addr
			getters = append(getters, getter)
			columns = append(columns, identityColumns[i])
		}
	}
	return columns, func(src interface{}, params []interface{}, offset, limit int) {
		holderPtr := xunsafe.AsPointer(src)
		end := offset + limit
		for i, ptr := range getters[offset:end] {
			params[i] = ptr(holderPtr)
		}
	}, nil
}
