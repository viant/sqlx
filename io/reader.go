package io

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/option"
	"reflect"
)

//Reader represents generic query reader
type Reader struct {
	query        string
	newRow       func() interface{}
	targetType   reflect.Type
	tagName      string
	stmt         *sql.Stmt
	rows         *sql.Rows
	newRowMapper RowMapperProvider
	unmappedFn   Resolve
}

//QuerySingle returns single row
func (r *Reader) QuerySingle(ctx context.Context, emit func(row interface{}) error, args ...interface{}) error {
	rows, err := r.stmt.QueryContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("failed to run query: %v, due to %s", r.query, err)
	}
	defer rows.Close()
	var mapper RowMapper
	var columns []Column
	if rows.Next() {
		if err = r.read(&mapper, rows, &columns, emit); err != nil {
			return err
		}
	}
	return nil
}

//QueryAll query all
func (r *Reader) QueryAll(ctx context.Context, emit func(row interface{}) error, args ...interface{}) error {
	rows, err := r.stmt.QueryContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("failed to run query: %v, due to %s", r.query, err)
	}
	defer rows.Close()
	return r.ReadAll(rows, emit)
}

//ReadAll read all
func (r *Reader) ReadAll(rows *sql.Rows, emit func(row interface{}) error) error {
	var mapper RowMapper
	var columns []Column
	for rows.Next() {
		if err := r.read(&mapper, rows, &columns, emit); err != nil {
			return err
		}
	}
	return nil
}

//QueryAllWithSlice query all with a slice
func (r *Reader) QueryAllWithSlice(ctx context.Context, emit func(row []interface{}) error, args ...interface{}) error {
	return r.QueryAll(ctx, func(row interface{}) error {
		aSlice, ok := row.([]interface{})
		if !ok {
			return fmt.Errorf("expected %T, but had %T", aSlice, row)
		}
		return emit(aSlice)
	}, args...)
}

//QueryAllWithMap query all with a map
func (r *Reader) QueryAllWithMap(ctx context.Context, emit func(row map[string]interface{}) error, args ...interface{}) error {
	return r.QueryAll(ctx, func(row interface{}) error {
		aMap, ok := row.(map[string]interface{})
		if !ok {
			return fmt.Errorf("expected %T, but had %T", aMap, row)
		}
		return emit(aMap)
	}, args...)
}

func (r *Reader) read(mapperPtr *RowMapper, rows *sql.Rows, columnsPtr *[]Column, emit func(row interface{}) error) error {
	row := r.newRow()
	columns := *columnsPtr
	mapper := *mapperPtr
	if r.targetType == nil {
		r.targetType = reflect.TypeOf(row)
		if r.targetType.Kind() == reflect.Ptr {
			r.targetType = r.targetType.Elem()
		}
	}
	if mapper == nil {
		columnNames, err := rows.Columns()
		if err != nil {
			return err
		}
		columns = NamesToColumns(columnNames)
		if columnsTypes, _ := rows.ColumnTypes(); len(columnNames) > 0 {
			columns = TypesToColumns(columnsTypes)
		}
		*columnsPtr = columns
		if mapper, err = r.newRowMapper(columns, r.targetType, r.tagName, r.unmappedFn); err != nil {
			return fmt.Errorf("creating rowValues mapper, due to %w", err)
		}
		*mapperPtr = mapper
	}

	rowValues, err := mapper(row)
	if err != nil {
		return err
	}
	err = rows.Scan(rowValues...)
	if err != nil {
		return fmt.Errorf("failed to scan %v, due to %w", r.query, err)
	}
	switch actual := row.(type) {
	case map[string]interface{}:
		asDereferenceSlice(rowValues)
		updateMap(columns, rowValues, actual)
	case []interface{}:
		asDereferenceSlice(rowValues)
		copy(actual, rowValues)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return emit(row)
}

func NewReader(ctx context.Context, db *sql.DB, query string, newRow func() interface{}, options ...option.Option) (*Reader, error) {
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare query: %v, due to %w", query, err)
	}
	return NewStmtReader(stmt, newRow, options...), err
}

func NewStmtReader(stmt *sql.Stmt, newRow func() interface{}, options ...option.Option) *Reader {
	var newRowMapper RowMapperProvider
	var unmappedFn Resolve
	if !option.Assign(options, &newRowMapper) {
		newRowMapper = newQueryMapper
	}
	option.Assign(options, &unmappedFn)

	return &Reader{newRow: newRow, stmt: stmt, tagName: option.Options(options).Tag(), newRowMapper: newRowMapper, unmappedFn: unmappedFn}
}

func NewMapReader(ctx context.Context, db *sql.DB, query string, options ...option.Option) (*Reader, error) {
	return NewReader(ctx, db, query, func() interface{} {
		return make(map[string]interface{})
	}, options...)
}

func NewSliceReader(ctx context.Context, db *sql.DB, query string, columns int, options ...option.Option) (*Reader, error) {
	return NewReader(ctx, db, query, func() interface{} {
		return make([]interface{}, columns)
	}, options...)
}
