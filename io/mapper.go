package io

import (
	"github.com/viant/sqlx/opt"
	"github.com/viant/sqlx/xunsafe"
	"reflect"
	"strings"
	"time"
)

const (
	dbTypeNameString  = "string"
	dbTypeNameText    = "text"
	dbTypeNameChar    = "char"
	dbTypeNameInt     = "int"
	dbTypeNameDecimal = "decimal"
	dbTypeNameNumeric = "numeric"
	dbTypeNameFloat   = "float"
	dbTypeNameTime    = "time"
	dbTypeNameDate    = "date"
	dbTypeNameBool    = "bool"
	dbTypeNameBytes   = "bytes"
	dbTypeNameBlob    = "blob"
)


//RowMapper represents a target values mapped to pointer of slice
type RowMapper func(target interface{}) ([]interface{}, error)

//RowMapperProvider represents new a row mapper
type RowMapperProvider func(columns []Column, targetType reflect.Type, tagName string) (RowMapper, error)

//newQueryMapper creates a new record mapped
func newQueryMapper(columns []Column, targetType reflect.Type, tagName string) (RowMapper, error) {
	if tagName == "" {
		tagName = opt.TagSqlx
	}
	if targetType.Kind() == reflect.Struct {
		return newQueryStructMapper(columns, targetType, tagName)
	}
	return genericMapper(columns)
}

//newQueryStructMapper creates a new record mapper for supplied struct type
func newQueryStructMapper(columns []Column, recordType reflect.Type, tagName string) (RowMapper, error) {
	mappedFieldIndex, err := columnPositions(columns, recordType, tagName)
	if err != nil {
		return nil, err
	}
	var record = make([]interface{}, len(mappedFieldIndex))

	var pointers = make([]xunsafe.Pointer, len(mappedFieldIndex))
	for i, fieldIndex := range mappedFieldIndex {
		pointers[i], err = xunsafe.FieldPointer(recordType, fieldIndex)
		if err != nil {
			return nil, err
		}
	}
	var mapper = func(target interface{}) ([]interface{}, error) {
		value := reflect.ValueOf(target)
		holderPtr := value.Elem().UnsafeAddr()
		for i, ptr := range pointers {
			record[i] = ptr(holderPtr)
		}
		return record, nil
	}
	return mapper, nil
}

//newQueryStructMapper creates a new record mapper for supplied struct type
func genericMapper(columns []Column) (RowMapper, error) {
	var valueProviders = make([]func(index int, values []interface{}), len(columns))
	defaultProvider := func(index int, values []interface{}) {
		val := new(interface{})
		values[index] = &val
	}
	for i := range columns {
		valueProviders[i] = defaultProvider
		dbTypeName := strings.ToLower(columns[i].DatabaseTypeName())
		if strings.Contains(dbTypeName, dbTypeNameChar) || strings.Contains(dbTypeName, dbTypeNameString) || strings.Contains(dbTypeName, dbTypeNameText) {
			valueProviders[i] = func(index int, values []interface{}) {
				val := ""
				values[index] = &val
			}
		} else if strings.Contains(dbTypeName, dbTypeNameInt) {
			valueProviders[i] = func(index int, values []interface{}) {
				val := 0
				values[index] = &val
			}
		} else if strings.Contains(dbTypeName, dbTypeNameNumeric) || strings.Contains(dbTypeName, dbTypeNameDecimal) || strings.Contains(dbTypeName, dbTypeNameFloat) {
			valueProviders[i] = func(index int, values []interface{}) {
				val := 0.0
				values[index] = &val
			}
		} else if strings.Contains(dbTypeName, dbTypeNameTime) || strings.Contains(dbTypeName, dbTypeNameDate) {
			valueProviders[i] = func(index int, values []interface{}) {
				val := time.Now()
				values[index] = &val
			}
		} else if strings.Contains(dbTypeName, dbTypeNameBool) {
			valueProviders[i] = func(index int, values []interface{}) {
				val := false
				values[index] = &val
			}
		} else if strings.Contains(dbTypeName, dbTypeNameBytes) || strings.Contains(dbTypeName, dbTypeNameBlob) {
			valueProviders[i] = func(index int, values []interface{}) {
				val := make([]byte, 0)
				values[index] = &val
			}
		} else {
			valueProviders[i] = func(index int, values []interface{}) {
				val := reflect.New(columns[i].ScanType()).Elem().Interface()
				values[index] = &val
			}
		}
	}
	mapper := func(target interface{}) ([]interface{}, error) {
		var record = make([]interface{}, len(columns))
		for i := range columns {
			valueProviders[i](i, record)
		}
		return record, nil
	}
	return mapper, nil
}
