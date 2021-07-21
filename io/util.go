package io

import (
	"fmt"
	"reflect"
	"strings"
)

//columnPositions maps column into field index in record type
func columnPositions(columns []Column, recordType reflect.Type, tag string) ([]int, error) {
	var indexedFields = map[string]int{}
	for i := 0; i < recordType.NumField(); i++ {
		if isExported := recordType.Field(i).PkgPath == ""; !isExported {
			continue
		}
		fieldName := recordType.Field(i).Name
		indexedFields[fieldName] = i
		indexedFields[strings.ToLower(fieldName)] = i //to account for various matching strategies
		aTag := recordType.Field(i).Tag
		isTransient := aTag.Get(tag) == "-"
		if isTransient {
			continue
		}
		if names := aTag.Get(tag); names != "" {
			for _, column := range strings.Split(names, "|") {
				column = strings.TrimSpace(column)
				if column == "" {
					continue
				}
				indexedFields[column] = i
			}
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
			return nil, fmt.Errorf("failed to match %v field for column: %v", recordType, column.Name())
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


func updateMap(columns []Column, values []interface{}, target map[string]interface{}) {
	for i, column := range columns {
		target[column.Name()] = values[i]
	}
}

func holderPointer(record interface{}) uintptr {
	value := reflect.ValueOf(record)
	if value.Kind() != reflect.Ptr { //convert to a pointer
		vp := reflect.New(value.Type())
		vp.Elem().Set(value)
		value = vp
	}
	holderPtr := value.Elem().UnsafeAddr()
	return holderPtr
}
