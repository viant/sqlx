package io

import (
	"fmt"
	"github.com/viant/sqlx"
	"reflect"
	"strings"
)


const (
	tagName = "name"
	tagColumn = "column"
)

//columnPositions maps column into field index in record type
func columnPositions(columns []sqlx.Column, recordType reflect.Type) ([]int, error) {
	var indexedFields = map[string]int{}
	for i := 0; i < recordType.NumField(); i++ {
		if isExported := recordType.Field(i).PkgPath == ""; !isExported {
			continue
		}
		fieldName := recordType.Field(i).Name
		indexedFields[fieldName] = i
		indexedFields[strings.ToLower(fieldName)] = i //to account for various matching strategies
		aTag := recordType.Field(i).Tag
		if column := aTag.Get(tagColumn); column != "" {
			indexedFields[column] = i
		} else if  column := aTag.Get(tagName); column != "" {
			indexedFields[column] = i
		}
	}
	var mappedFieldIndex = make([]int, len(columns))
	for i, column := range columns {
		columnName := column.Name()
		fieldIndex, ok := indexedFields[column.Name()]
		if !ok {
			fieldIndex, ok = indexedFields[strings.ToLower(columnName)]
		}
		if !ok {
			fieldIndex, ok = indexedFields[strings.Replace(strings.ToLower(columnName), "_", "", strings.Count(columnName, "_"))]
		}
		if !ok {
			return nil, fmt.Errorf("failed to matched a %v field for SQL column: %v", recordType, column)
		}
		mappedFieldIndex[i] = fieldIndex
	}
	return mappedFieldIndex, nil
}




func asDereferenceSlice(aSlice []interface{}) {
	for i, value := range aSlice {
		if value == nil {
			continue
		}
		aSlice[i] = reflect.ValueOf(value).Elem().Interface()

	}
}

func updateMap(columns []sqlx.Column, values []interface{}, target map[string]interface{}) {
	for i, column := range columns {
		target[column.Name()] = values[i]
	}
}




