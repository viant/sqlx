package io

import (
	"fmt"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
	"reflect"
)

//ColumnMapper maps src to columns and its placeholders
type ColumnMapper func(src interface{}, tagName string, options ...option.Option) ([]Column, PlaceholderBinder, error)

//StructColumnMapper returns genertic column mapper
func StructColumnMapper(src interface{}, tagName string, options ...option.Option) ([]Column, PlaceholderBinder, error) {
	recordType := reflect.TypeOf(src)
	if recordType.Kind() == reflect.Ptr {
		recordType = recordType.Elem()
	}

	identityOnly := option.Options(options).IdentityOnly()
	var columnRestriction option.ColumnRestriction
	if val := option.Options(options).Columns(); len(val) > 0 {
		columnRestriction = val.Restriction()
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
		if err := tag.validateWithField(field); err != nil {
			return nil, nil, err
		}
		if tag.Transient {
			continue
		}

		columnName := tag.getColumnName(field)
		if tag.isIdentity(columnName) {
			tag.PrimaryKey = true
			tag.Column = columnName
			tag.FieldIndex = i
			identityColumns = append(identityColumns, NewColumn(columnName, "", field.Type, tag))
			continue
		}

		if identityOnly {
			continue
		}

		if columnRestriction.CanUse(columnName) {
			columns = append(columns, NewColumn(columnName, "", field.Type, tag))
			getter := xunsafe.FieldByIndex(recordType, i).Addr
			getters = append(getters, getter)
		}
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
