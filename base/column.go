package base

import "reflect"

type Column struct {
	name             string
	databaseTypeName string
	length           *int64
	decimalPrecision *int64
	decimalScale     *int64
	nullable         *bool
	position         int
	scanType         reflect.Type
}

func (c *Column) Name() string {
	return c.name
}

func (c *Column) Length() (length int64, ok bool) {
	if c.length == nil {
		return 0, false
	}
	return *c.length, true
}

func (c *Column) DecimalSize() (precision, scale int64, ok bool) {
	if c.decimalPrecision == nil || c.decimalScale == nil {
		return 0, 0, false
	}
	return *c.decimalPrecision, *c.decimalScale, true
}

func (c *Column) ScanType() reflect.Type {
	return c.scanType
}

func (c *Column) Nullable() (nullable, ok bool) {
	if c.nullable == nil {
		return false, false
	}
	return *c.nullable, true
}


// Common type include "VARCHAR", "TEXT", "NVARCHAR", "DECIMAL", "BOOL", "INT", "BIGINT".
func (c *Column) DatabaseTypeName() string {
	return c.databaseTypeName
}

