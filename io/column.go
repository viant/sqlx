package io

import (
	"database/sql"
	"github.com/viant/xunsafe"
	"reflect"
)

type (
	//Column represents a column
	Column interface {
		Name() string
		Length() (length int64, ok bool)
		DecimalSize() (precision, scale int64, ok bool)
		ScanType() reflect.Type
		Nullable() (nullable, ok bool)
		DatabaseTypeName() string
		Tag() *Tag
	}

	ColumnWithFields interface {
		Column
		Fielder
	}

	Fielder interface {
		Fields() []*xunsafe.Field
	}

	ColumnOption func(o *column)
)

func (c *column) apply(opts []ColumnOption) {
	for _, opt := range opts {
		opt(c)
	}
}

type columnType struct {
	*sql.ColumnType
	scanType         reflect.Type
	databaseTypeName string
}

func (c *columnType) ScanType() reflect.Type {
	if c.scanType == nil {
		c.scanType = ensureScanType(c.DatabaseTypeName(), c.ColumnType.ScanType())
	}
	return c.scanType
}

func (c *columnType) Tag() *Tag {
	return nil
}

// DatabaseTypeName returns database type name Common type include "VARCHAR", "TEXT", "NVARCHAR", "DECIMAL", "BOOL", "INT", "BIGINT".
func (c *columnType) DatabaseTypeName() string {
	if c.databaseTypeName != "" {
		return c.databaseTypeName
	}
	return c.ColumnType.DatabaseTypeName()
}

// column represents a column
type (
	column struct {
		name             string
		databaseTypeName string
		length           *int64
		decimalPrecision *int64
		decimalScale     *int64
		nullable         *bool
		position         int
		scanType         reflect.Type
		tag              *Tag
		options          []interface{}
	}

	columnWithFields struct {
		*column
		fields []*xunsafe.Field
	}
)

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

// ScanType returns scan type
func (c *column) ScanType() reflect.Type {
	return c.scanType
}

// Nullable returns nullable flag
func (c *column) Nullable() (nullable, ok bool) {
	if c.nullable == nil {
		return false, false
	}
	return *c.nullable, true
}

// Tag returns column tag
func (c *column) Tag() *Tag {
	return c.tag
}

// DatabaseTypeName returns database type name Common type include "VARCHAR", "TEXT", "NVARCHAR", "DECIMAL", "BOOL", "INT", "BIGINT".
func (c *column) DatabaseTypeName() string {
	return c.databaseTypeName
}

// NewColumn creates a column
func NewColumn(name, databaseTypeName string, rType reflect.Type, opts ...ColumnOption) Column {
	result := newColumn(name, databaseTypeName, rType, opts)
	var fields []*xunsafe.Field
	for _, opt := range result.options {
		switch actual := opt.(type) {
		case *xunsafe.Field:
			fields = append(fields, actual)
		case []*xunsafe.Field:
			fields = append(fields, actual...)
		}
	}
	if len(fields) > 0 {
		return newColumnWithFields(result, fields)
	}
	return result
}

func newColumnWithFields(result *column, fields []*xunsafe.Field) Column {
	return &columnWithFields{
		fields: fields,
		column: result,
	}
}

func newColumn(name string, databaseTypeName string, rType reflect.Type, opts []ColumnOption) *column {
	nullable := rType.Kind() == reflect.Ptr
	result := &column{
		name:             name,
		databaseTypeName: databaseTypeName,
		scanType:         rType,
		nullable:         &nullable,
	}

	result.apply(opts)
	result.scanType = ensureScanType(result.databaseTypeName, result.scanType)
	return result
}

func NewColumnWithFields(name string, databaseTypeName string, rType reflect.Type, fields []*xunsafe.Field, opts ...ColumnOption) ColumnWithFields {
	return &columnWithFields{
		column: newColumn(name, databaseTypeName, rType, opts),
		fields: fields,
	}
}

func (c *columnWithFields) Fields() []*xunsafe.Field {
	return c.fields
}

// WithColumnLength  returns column length set option
func WithColumnLength(l int64) ColumnOption {
	return func(o *column) {
		o.length = &l
	}
}

// WithColumnDecimalScale  returns column decimal scale set option
func WithColumnDecimalScale(l int64) ColumnOption {
	return func(o *column) {
		o.decimalScale = &l
	}
}

// WithColumnDecimalPrecision  returns column decimal precision set option
func WithColumnDecimalPrecision(l int64) ColumnOption {
	return func(o *column) {
		o.decimalPrecision = &l
	}
}

// WithColumnNullable  returns column nullable set option
func WithColumnNullable(l bool) ColumnOption {
	return func(o *column) {
		o.nullable = &l
	}
}

// WithTag  returns column tag option
func WithTag(t *Tag) ColumnOption {
	return func(o *column) {
		o.tag = t
	}
}

// WithCustomOption  returns column tag option
func WithCustomOption(opts ...interface{}) ColumnOption {
	return func(o *column) {
		o.options = append(o.options, opts...)
	}
}
