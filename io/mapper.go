package io

import (
	"fmt"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"time"
	"unsafe"
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
type RowMapperProvider func(columns []Column, targetType reflect.Type, tagName string, resolver Resolve) (RowMapper, error)

//newQueryMapper creates a new record mapped
func newQueryMapper(columns []Column, targetType reflect.Type, tagName string, resolver Resolve) (RowMapper, error) {
	if tagName == "" {
		tagName = option.TagSqlx
	}
	if targetType.Kind() == reflect.Struct {
		return newQueryStructMapper(columns, targetType, tagName, resolver)
	}
	return genericRowMapper(columns)
}

//newQueryStructMapper creates a new record mapper for supplied struct type
func newQueryStructMapper(columns []Column, recordType reflect.Type, tagName string, resolver Resolve) (RowMapper, error) {
	fildGetters, err := columnPositions("", columns, recordType, tagName, resolver)
	if err != nil {
		return nil, err
	}
	var record = make([]interface{}, len(fildGetters))

	var getters = make([]xunsafe.Getter, len(fildGetters))
	for i, item := range fildGetters {
		getters[i] = item.AddrGetter()
	}
	var mapper = func(target interface{}) ([]interface{}, error) {
		value := reflect.ValueOf(target)
		holderPtr := unsafe.Pointer(value.Elem().UnsafeAddr())
		for i, ptr := range getters {
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
type PlaceholderBinder func(src interface{}, params []interface{}, offset, limit int)

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
		if tag.PrimaryKey || strings.ToLower(columnName) == "id" {
			if tag == nil {
				tag = &Tag{Column: columnName, PrimaryKey: true}
			}
			tag.FieldIndex = i
			identityColumns = append(identityColumns, &column{
				name:     columnName,
				scanType: field.Type,
				tag:      tag,
			})
			continue
		}
		columns = append(columns, &column{
			name:     columnName,
			scanType: field.Type,
			tag:      tag,
		})
		getter := xunsafe.FieldByIndex(recordType, i).AddrGetter()
		getters = append(getters, getter)
	}

	//make sure id column are at the end
	if len(identityColumns) > 0 {
		for i, item := range identityColumns {
			fieldIndex := item.Tag().FieldIndex
			getter := xunsafe.FieldByIndex(recordType,  fieldIndex).AddrGetter()
			getters = append(getters, getter)
			columns = append(columns, identityColumns[i])
		}
	}
	return columns, func(src interface{}, params []interface{}, offset, limit int) {
		holderPtr := xunsafe.Addr(src)
		end := offset + limit
		for i, ptr := range getters[offset:end] {
			params[i] = ptr(holderPtr)
		}
	}, nil
}

var byteType = reflect.TypeOf([]byte{})
var timeType = reflect.TypeOf(time.Time{})

//columnPositions maps column into field index in record type
func columnPositions(ns string, columns []Column, recordType reflect.Type, tag string, resolver Resolve) ([]*xunsafe.Field, error) {
	var indexedFields = map[string]*xunsafe.Field{}
	for i := 0; i < recordType.NumField(); i++ {
		field := recordType.Field(i)
		matchFieldWithColumn(recordType, field, indexedFields, i, tag, ns)
	}

	var unmappedColumns []Column
	var mappedFieldIndex = make([]*xunsafe.Field, len(columns))
	for i, column := range columns {
		columnName := column.Name()
		aField, ok := indexedFields[column.Name()]
		if !ok {
			aField, ok = indexedFields[strings.ToLower(columnName)]
		}
		if !ok {
			aField, ok = indexedFields[strings.Replace(strings.ToLower(columnName), "_", "", strings.Count(columnName, "_"))]
		}
		if !ok {
			if resolver != nil {
				mappedFieldIndex[i] = xunsafe.FieldWithGetters(resolver(columns[i]), resolver(columns[i]))
			}
			unmappedColumns = append(unmappedColumns, columns[i])
			continue
		}
		mappedFieldIndex[i] = aField
	}

	if len(unmappedColumns) > 0 {
		if resolver == nil {
			var columns []string
			for _, col := range unmappedColumns {
				columns = append(columns, col.Name())
			}
			return nil, fmt.Errorf("failed to match %v field for columns: %v", recordType, columns)
		}
	}

	return mappedFieldIndex, nil
}

func matchFieldWithColumn(structType reflect.Type, field reflect.StructField, indexedFields map[string]*xunsafe.Field, index int, tag string, ns string) {
	if isExported := field.PkgPath == ""; !isExported && !field.Anonymous {
		return
	}
	aTag := ParseTag(field.Tag.Get(tag))
	if aTag.Transient {
		return
	}
	if IsBaseType(field.Type) {
		fieldName := field.Name
		aField := xunsafe.FieldByIndex(structType, index)
		indexedFields[ns+fieldName] = aField
		indexedFields[ns+strings.ToLower(fieldName)] = aField //to account for various matching strategies
	}

	switch field.Type.Kind() {
	case reflect.Struct:
		subFields := make(map[string]*xunsafe.Field)
		for i := 0; i < field.Type.NumField(); i++ {
			matchFieldWithColumn(field.Type, field.Type.Field(i), subFields, i, tag, aTag.Ns)
		}
		if len(subFields) > 0 {
			for k, v := range subFields {
				field := xunsafe.FieldByIndex(structType, index)
				field.Field = v
				indexedFields[k] = field
			}
		}
	case reflect.Ptr:
		fieldType := field.Type.Elem()
		if fieldType.Kind() == reflect.Struct {
			subFields := make(map[string]*xunsafe.Field)
			for i := 0; i < fieldType.NumField(); i++ {
				matchFieldWithColumn(fieldType, fieldType.Field(i), subFields, i, tag, aTag.Ns)
			}
			if len(subFields) > 0 {
				for k, v := range subFields {
					field := xunsafe.FieldByIndex(structType, index)
					field.Field = v
					indexedFields[k] = field
				}
			}
		}
	}
	if names := aTag.Column; names != "" {
		for _, column := range strings.Split(names, "|") {
			column = strings.TrimSpace(column)
			if column == "" {
				continue
			}
			indexedFields[ns+column] = xunsafe.FieldByIndex(structType, index)
		}
	}
}
