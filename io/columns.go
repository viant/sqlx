package io

import (
	"database/sql"
	"github.com/viant/sqlx/option"
	"github.com/viant/sqlx/types"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
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

// IdentityColumnPos returns identity column position in []Column
func (c Columns) IdentityColumnPos() int {
	if len(c) == 0 {
		return -1
	}

	for i, item := range c {
		if IsIdentityColumn(item) {
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

func ParseType(columnType string) (reflect.Type, bool) {
	switch strings.ToLower(columnType) {
	case "int", "integer", "bigint", "smallint", "unsiged tinyint", "tinyint", "int64", "int32", "int16", "int8", "uint", "uint8", "uint16", "uint32", "uint64", "binary":
		return xreflect.IntType, true
	case "float", "float64", "numeric", "decimal", "double":
		return xreflect.Float64Type, true
	case "bool", "boolean":
		return xreflect.BoolType, true
	case "bit", "bitbool":
		return reflect.TypeOf(types.BitBool(true)), true
	case "string", "varchar", "char", "text", "longtext", "longblob", "mediumblob", "mediumtext", "blob", "tinytext":
		return reflect.TypeOf(""), true
	case "date", "time", "timestamp", "datetime":
		return xreflect.TimeType, true
	case "sql.rawbytes", "rawbytes":
		return reflect.TypeOf(""), true
	case "interface":
		t := xreflect.InterfaceType
		return t, true
	}

	return nil, false
}

func NormalizeColumnType(scanType reflect.Type, name string) reflect.Type {
	rType, ok := ParseType(name)
	if ok {
		return normalizeTypeRange(rType)
	}

	return normalizeTypeRange(normalizeScanType(scanType))
}

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

func normalizeTypeRange(rType reflect.Type) reflect.Type {
	actualType := rType
	ptrCounter := 0

	for actualType.Kind() == reflect.Ptr {
		actualType = actualType.Elem()
		ptrCounter++
	}

	switch actualType.Kind() {
	case reflect.Int8, reflect.Uint8, reflect.Int, reflect.Uint, reflect.Uint32, reflect.Int32, reflect.Uint16, reflect.Int16, reflect.Int64, reflect.Uint64:
		return pointerify(xreflect.IntType, ptrCounter)
	case reflect.Float64, reflect.Float32:
		return pointerify(xreflect.Float64Type, ptrCounter)
	}

	return rType
}

func pointerify(rType reflect.Type, counter int) reflect.Type {
	for i := 0; i < counter; i++ {
		rType = reflect.PtrTo(rType)
	}

	return rType
}

//TypesToColumns converts []*sql.ColumnType type to []sqlx.column
func TypesToColumns(columns []*sql.ColumnType) []Column {
	var result = make([]Column, len(columns))
	for i := range columns {
		dbType := columns[i].DatabaseTypeName()
		dbType = strings.Replace(dbType, "UNSIGNED", "", 1)
		dbType = strings.TrimSpace(dbType)
		result[i] = &columnType{databaseTypeName: dbType, ColumnType: columns[i], scanType: NormalizeColumnType(columns[i].ScanType(), columns[i].DatabaseTypeName())}
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
func StructColumns(recordType reflect.Type, tagName string, opts ...option.Option) ([]Column, error) {
	columns, _, err := StructColumnMapper(recordType, tagName, opts...)
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func IsIdentityColumn(col Column) bool {
	if col.Tag() == nil {
		return false
	}

	return col.Tag().isIdentity(col.Name())
}
