package io

import (
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/opts"
	"reflect"
	"strings"
)

//Columns represents insertColumns
type Columns []Column

//Autoincrement returns position of autoincrement column or -1
func (c Columns) Autoincrement() int {
	if len(c) == 0 {
		return -1
	}
	for i, item := range c {
		if tag := item.Tag(); tag != nil && tag.Autoincrement {
			return i
		}
	}
	return -1
}

//Names returns column names
func (c Columns) Names() []string {
	var result = make([]string, len(c))
	for i, item := range c {
		result[i] = item.Name()
	}
	return result
}

//TypesToColumns converts []*sql.ColumnType type to []sqlx.column
func TypesToColumns(columns []*sql.ColumnType) []Column {
	var result = make([]Column, len(columns))
	for i := range columns {
		result[i] = &columnType{columns[i]}
	}
	return result
}

//NamesToColumns converts []string to []sqlx.column
func NamesToColumns(columns []string) []Column {
	var result = make([]Column, len(columns))
	for i := range columns {
		result[i] = &column{name: columns[i]}
	}
	return result
}

//StructColumns returns column for the struct
func StructColumns(recordType reflect.Type) ([]Column, error) {
	var result []Column
	if recordType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, but had: %v", recordType.Name())
	}
	for i := 0; i < recordType.NumField(); i++ {
		field := recordType.Field(i)
		if isExported := field.PkgPath == ""; !isExported {
			continue
		}
		fieldType := field.Type
		if ! IsBaseType(fieldType) {
			continue
		}

		fieldName := field.Name
		aTag := ParseTag(field.Tag.Get(opts.TagSqlx))
		if aTag.Transient {
			continue
		}
		columnName := fieldName
		if names := aTag.Column; names != "" {
			columns := strings.Split(names, "|")
			columnName = columns[0]
		}
		result = append(result, &column{
			name:     columnName,
			scanType: field.Type,
		})
	}
	return result, nil
}


