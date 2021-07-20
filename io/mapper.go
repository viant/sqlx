package io

import (
	"fmt"
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
	return genericRowMapper(columns)
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
func genericRowMapper(columns []Column) (RowMapper, error) {
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

//PlaceholderBinder copies source values to params starting with offset
type PlaceholderBinder func(src interface{}, params []interface{}, offset int)

//ColumnMapper maps src to columns and its placeholders
type ColumnMapper func(src interface{}, tagName string) ([]Column, PlaceholderBinder,  error)

func genericColumnMapper(src interface{}, tagName string) ([]Column, PlaceholderBinder,  error) {
	recordType := reflect.TypeOf(src)
	if recordType.Kind() == reflect.Ptr {
		recordType = recordType.Elem()
	}
	if recordType.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("invalid record type: %v", recordType.Kind())
	}
	var columns []Column
	var identityColumns []Column
	var pointers []xunsafe.Pointer
	for i := 0; i < recordType.NumField(); i++ {
		field := recordType.Field(i)
		if isExported := field.PkgPath == ""; !isExported {
			continue
		}

		tag := ParseTag(field.Tag.Get(tagName))
		isTransient := tag.Column == "-"
		if isTransient {
			continue
		}
		columnName := field.Name
		if tag.Column != "" {
			columnName = tag.Column
		}
		if tag.PrimaryKey || strings.ToLower(columnName) == "id" {
			if tag == nil {
				tag = &Tag{Column: columnName, PrimaryKey: true}
			}
			tag.FieldIndex = i
			identityColumns  = append(identityColumns, &column{
				name: columnName,
				scanType: field.Type,
				tag: tag,
			})
			continue
		}
		columns  = append(columns, &column{
			name: columnName,
			scanType: field.Type,
			tag: tag,
		})
		pointer, err := xunsafe.FieldPointer(recordType, i)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get filed: %T.%v pointer", src, field.Name)
		}
		pointers = append(pointers, pointer)
	}

	//make sure id column are at the end
	if len(identityColumns) > 0 {
		for i, item := range identityColumns {
			fieldIndex := item.Tag().FieldIndex
			field := recordType.Field(fieldIndex)
			pointer, err := xunsafe.FieldPointer(recordType, fieldIndex)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get filed: %T.%v pointer", src, field.Name)
			}
			pointers = append(pointers, pointer)
			columns = append(columns, identityColumns[i])
		}
	}
	return columns, func(src interface{}, params []interface{}, offset int) {
		holderPtr := holderPointer(src)
		for i, ptr := range pointers {
			params[offset+i] = ptr(holderPtr)
		}
	}, nil
}
