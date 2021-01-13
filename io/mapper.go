package io

import (
	"fmt"
	"github.com/viant/sqlx"
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

type Foo struct {
	ID   int
	Name string
}

//RowMapper represents a target values mapped to pointer of slice
type RowMapper func(target interface{}) ([]interface{}, error)

//newQueryMapper creates a new record mapped
func newQueryMapper(columns []sqlx.Column, targetType reflect.Type) (RowMapper, error) {
	if targetType.Kind() == reflect.Struct {
		return newQueryStructMapper(columns, targetType)
	}
	return genericMapper(columns)
}

//newQueryStructMapper creates a new record mapper for supplied struct type
func newQueryStructMapper(columns []sqlx.Column, recordType reflect.Type) (RowMapper, error) {
	mappedFieldIndex, err := columnPositions(columns, recordType)
	if err != nil {
		return nil, err
	}
	var record = make([]interface{}, recordType.NumField())
	var mapper = func(target interface{}) ([]interface{}, error) {
		//value, ok := target.(reflect.Value)
		value := reflect.ValueOf(target)
		//if !ok {
		//	return nil, fmt.Errorf("expected %T, but had: %T", value, target)
		//}
		if value.Kind() != reflect.Ptr {
			return nil, fmt.Errorf("expected pointer, but had: %T", value.Kind())
		}
		value = value.Elem() //T = *T
		for i, fieldIndex := range mappedFieldIndex {
			record[i] = value.Field(fieldIndex).Addr().Interface()
		}
		return record, nil
	}
	return mapper, nil
}

//newQueryStructMapper creates a new record mapper for supplied struct type
func genericMapper(columns []sqlx.Column) (RowMapper, error) {
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
