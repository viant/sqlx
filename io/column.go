package io

import (
	"database/sql"
	"reflect"
)

//ColumnLength represents column length
type ColumnLength int64

//ColumnDecimalScale represents column decimal scale
type ColumnDecimalScale int64

//ColumnDecimalPrecision represents column decimal precision
type ColumnDecimalPrecision int64

//ColumnNullable represents column nullable option
type ColumnNullable bool

//Column represents a column
type Column interface {
	Name() string
	Length() (length int64, ok bool)
	DecimalSize() (precision, scale int64, ok bool)
	ScanType() reflect.Type
	Nullable() (nullable, ok bool)
	DatabaseTypeName() string
	Tag() *Tag
}

type columnType struct {
	*sql.ColumnType
	scanType         reflect.Type
	databaseTypeName string
}

func (t *columnType) ScanType() reflect.Type {
	if t.scanType == nil {
		t.scanType = ensureScanType(t.DatabaseTypeName(), t.ColumnType.ScanType())
	}
	return t.scanType
}

func (t *columnType) Tag() *Tag {
	return nil
}

//DatabaseTypeName returns database type name Common type include "VARCHAR", "TEXT", "NVARCHAR", "DECIMAL", "BOOL", "INT", "BIGINT".
func (c *columnType) DatabaseTypeName() string {
	if c.databaseTypeName != "" {
		return c.databaseTypeName
	}
	return c.ColumnType.DatabaseTypeName()
}

//column represents a column
type column struct {
	name             string
	databaseTypeName string
	length           *int64
	decimalPrecision *int64
	decimalScale     *int64
	nullable         *bool
	position         int
	scanType         reflect.Type
	tag              *Tag
}

func (c *column) Name() string {
	return c.name
}

func (c *column) Length() (length int64, ok bool) {
	if c.length == nil {
		return 0, false
	}
	return *c.length, true
}

func (c *column) DecimalSize() (precision, scale int64, ok bool) {
	if c.decimalPrecision == nil || c.decimalScale == nil {
		return 0, 0, false
	}
	return *c.decimalPrecision, *c.decimalScale, true
}

//ScanType returns scan type
func (c *column) ScanType() reflect.Type {
	return c.scanType
}

//Nullable returns nullable flag
func (c *column) Nullable() (nullable, ok bool) {
	if c.nullable == nil {
		return false, false
	}
	return *c.nullable, true
}

//Tag returns column tag
func (c *column) Tag() *Tag {
	return c.tag
}

//DatabaseTypeName returns database type name Common type include "VARCHAR", "TEXT", "NVARCHAR", "DECIMAL", "BOOL", "INT", "BIGINT".
func (c *column) DatabaseTypeName() string {
	return c.databaseTypeName
}

func (c *column) applyOptions(opts []interface{}) {
	if len(opts) == 0 {
		return
	}
	for _, opt := range opts {
		switch actual := opt.(type) {
		case ColumnLength:
			value := int64(actual)
			c.length = &value
		case ColumnNullable:
			value := bool(actual)
			c.nullable = &value
		case ColumnDecimalScale:
			value := int64(actual)
			c.decimalScale = &value
		case ColumnDecimalPrecision:
			value := int64(actual)
			c.decimalPrecision = &value
		case *Tag:
			c.tag = actual
		}
	}
}

//NewColumn creates a column
func NewColumn(name, databaseTypeName string, rType reflect.Type, opts ...interface{}) Column {
	result := &column{
		name:             name,
		databaseTypeName: databaseTypeName,
		scanType:         rType,
	}
	result.applyOptions(opts)
	result.scanType = ensureScanType(result.databaseTypeName, result.scanType)
	return result
}
