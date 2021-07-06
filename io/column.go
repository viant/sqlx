package io

import "reflect"

//Column represents a column
type Column interface {
	Name() string
	Length() (length int64, ok bool)
	DecimalSize() (precision, scale int64, ok bool)
	ScanType() reflect.Type
	Nullable() (nullable, ok bool)
	DatabaseTypeName() string
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

func (c *column) ScanType() reflect.Type {
	return c.scanType
}

func (c *column) Nullable() (nullable, ok bool) {
	if c.nullable == nil {
		return false, false
	}
	return *c.nullable, true
}


// Common type include "VARCHAR", "TEXT", "NVARCHAR", "DECIMAL", "BOOL", "INT", "BIGINT".
func (c *column) DatabaseTypeName() string {
	return c.databaseTypeName
}

