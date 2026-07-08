package io

import (
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

var (
	typeString    = reflect.TypeOf("")
	typeInt       = reflect.TypeOf(0)
	typeFloat64   = reflect.TypeOf(0.0)
	typeTime      = reflect.TypeOf(time.Time{})
	typeBool      = reflect.TypeOf(false)
	typeBytes     = reflect.TypeOf([]byte{})
	interfaceType = reflect.TypeOf(new(interface{}))
)

// ensureScanType ensure that scan type returns type matching database type name
func ensureScanType(columnTypeName string, scanType reflect.Type) reflect.Type {

	if scanType != nil && scanType.Kind() != reflect.Interface {
		return scanType
	}
	dbTypeName := strings.ToLower(columnTypeName)
	if strings.Contains(dbTypeName, dbTypeNameChar) || strings.Contains(dbTypeName, dbTypeNameString) || strings.Contains(dbTypeName, dbTypeNameText) {
		return typeString
	}
	if strings.Contains(dbTypeName, dbTypeNameInt) {
		return typeInt
	}
	if strings.Contains(dbTypeName, dbTypeNameNumeric) || strings.Contains(dbTypeName, dbTypeNameDecimal) || strings.Contains(dbTypeName, dbTypeNameFloat) {
		return typeFloat64
	}
	if strings.Contains(dbTypeName, dbTypeNameTime) || strings.Contains(dbTypeName, dbTypeNameDate) {
		return typeTime
	}
	if strings.Contains(dbTypeName, dbTypeNameBool) {
		return typeBool
	}
	if strings.Contains(dbTypeName, dbTypeNameBytes) || strings.Contains(dbTypeName, dbTypeNameBlob) {
		return typeBytes
	}
	return interfaceType
}

// EnsureDereference returns Type of value dereferenced e.g. if any is type of *Foo, it will return Foo
func EnsureDereference(value interface{}) reflect.Type {
	rType := reflect.TypeOf(value)
	if rType.Kind() == reflect.Ptr {
		return rType.Elem()
	}
	return rType
}
