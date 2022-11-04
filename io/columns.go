package io

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

//Columns represents columns
type Columns []Column

//Autoincrement returns position of autoincrement column position or -1
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

func (c Columns) IdentityColumnPos() int {
	if len(c) == 0 {
		return -1
	}
	for i, item := range c {

		if tag := item.Tag(); tag.isIdentity(item.Name()) {
			return i
		}
	}
	return -1
}

//PrimaryKeys returns position of primary key position or -1
func (c Columns) PrimaryKeys() int {
	if len(c) == 0 {
		return -1
	}
	for i, item := range c {
		if tag := item.Tag(); tag != nil && tag.PrimaryKey {
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

var sqlNullStringType = reflect.TypeOf(sql.NullString{})
var goNullStringType = reflect.PtrTo(reflect.TypeOf(""))

var sqlNullTimeType = reflect.TypeOf(sql.NullTime{})
var goNullTimeType = reflect.PtrTo(reflect.TypeOf(time.Time{}))

var sqlNullByteType = reflect.TypeOf(sql.NullByte{})
var goNullByteType = reflect.PtrTo(reflect.TypeOf(byte(0)))

var sqlNullBoolType = reflect.TypeOf(sql.NullBool{})
var goNullBoolType = reflect.PtrTo(reflect.TypeOf(true))

var sqlNullInt16Type = reflect.TypeOf(sql.NullInt16{})
var goNullInt16Type = reflect.PtrTo(reflect.TypeOf(int16(0)))

var sqlNullInt32Type = reflect.TypeOf(sql.NullInt32{})
var goNullInt32Type = reflect.PtrTo(reflect.TypeOf(int32(0)))

var sqlNullInt64Type = reflect.TypeOf(sql.NullInt64{})
var goNullInt64Type = reflect.PtrTo(reflect.TypeOf(int64(0)))

var sqlNullFloat64Type = reflect.TypeOf(sql.NullFloat64{})
var goNullFloat64Type = reflect.PtrTo(reflect.TypeOf(float64(0)))

var sqlRawBytesType = reflect.TypeOf(sql.RawBytes{})
var goRawBytesType = reflect.PtrTo(reflect.TypeOf(""))

func normalizeScanType(scanType reflect.Type) reflect.Type {
	switch scanType {
	case sqlNullStringType:
		return goNullStringType
	case sqlNullTimeType:
		return goNullTimeType
	case sqlNullBoolType:
		return goNullBoolType
	case sqlNullByteType:
		return goNullByteType
	case sqlRawBytesType:
		return goRawBytesType
	case sqlNullInt16Type:
		return goNullInt16Type
	case sqlNullInt32Type:
		return goNullInt32Type
	case sqlNullInt64Type:
		return goNullInt64Type
	case sqlNullFloat64Type:
		return goNullFloat64Type
	}
	return scanType
}

//TypesToColumns converts []*sql.ColumnType type to []sqlx.column
func TypesToColumns(columns []*sql.ColumnType) []Column {
	var result = make([]Column, len(columns))
	for i := range columns {
		result[i] = &columnType{ColumnType: columns[i], scanType: normalizeScanType(columns[i].ScanType())}
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
func StructColumns(recordType reflect.Type, tagName string) ([]Column, error) {
	var result []Column
	if recordType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, but had: %v", recordType.Name())
	}
	for i := 0; i < recordType.NumField(); i++ {
		field := recordType.Field(i)
		if isExported := field.PkgPath == ""; !isExported {
			continue
		}

		aTag := ParseTag(field.Tag.Get(tagName))
		aTag.FieldIndex = i
		if aTag.Transient {
			continue
		}
		columnName := aTag.getColumnName(field)
		aTag.PrimaryKey = aTag.isIdentity(columnName)

		result = append(result, &column{
			name:     columnName,
			scanType: field.Type,
			tag:      aTag,
		})
	}
	return result, nil
}
