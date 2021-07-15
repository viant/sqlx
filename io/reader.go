package io

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/opt"
	"reflect"
)

//Reader represents generic query reader
type Reader struct {
	query      string
	newRow     func() interface{}
	targetType reflect.Type
	tagName    string
	stmt       *sql.Stmt
	rows       *sql.Rows
}

func (r *Reader) QuerySingle(ctx context.Context, emit func(row interface{}) error, args ...interface{}) error {
	rows, err := r.stmt.QueryContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("failed to run query: %v, due to %s", r.query, err)
	}
	defer rows.Close()
	var mapper RowMapper
	var columns []Column
	if rows.Next() {
		if err = r.read(mapper, rows, &columns, emit); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reader) QueryAll(ctx context.Context, emit func(row interface{}) error, args ...interface{}) error {
	rows, err := r.stmt.QueryContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("failed to run query: %v, due to %s", r.query, err)
	}
	defer rows.Close()
	return r.ReadAll(rows, emit)
}

func (r *Reader) ReadAll(rows *sql.Rows, emit func(row interface{}) error) error {
	var mapper RowMapper
	var columns []Column
	for rows.Next() {
		if err := r.read(mapper, rows, &columns, emit); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reader) QueryAllWithSlice(ctx context.Context, emit func(row []interface{}) error, args ...interface{}) error {
	return r.QueryAll(ctx, func(row interface{}) error {
		aSlice, ok := row.([]interface{})
		if !ok {
			return fmt.Errorf("expected %T, but had %T", aSlice, row)
		}
		return emit(aSlice)
	}, args...)
}

func (r *Reader) QueryAllWithMap(ctx context.Context, emit func(row map[string]interface{}) error, args ...interface{}) error {
	return r.QueryAll(ctx, func(row interface{}) error {
		aMap, ok := row.(map[string]interface{})
		if !ok {
			return fmt.Errorf("expected %T, but had %T", aMap, row)
		}
		return emit(aMap)
	}, args...)
}

func (r *Reader) read(mapper RowMapper, rows *sql.Rows, columnsPtr *[]Column, emit func(row interface{}) error) error {
	row := r.newRow()
	columns := *columnsPtr
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
		if mapper, err = newQueryMapper(columns, r.targetType, r.tagName); err != nil {
			return fmt.Errorf("creating rowValues mapper, due to %w", err)
		}
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

func NewReader(ctx context.Context, db *sql.DB, query string, newRow func() interface{}, options ...opt.Option) (*Reader, error) {
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare query: %v, due to %w", query, err)
	}
	return NewStmtReader(stmt, newRow, options...), err
}

func NewStmtReader(stmt *sql.Stmt, newRow func() interface{}, options ...opt.Option) *Reader {
	targetType := reflect.TypeOf(newRow())
	if targetType.Kind() == reflect.Ptr {
		targetType = targetType.Elem()
	}
	return &Reader{newRow: newRow, targetType: targetType, stmt: stmt, tagName: opt.Options(options).Tag()}
}

func NewMapReader(ctx context.Context, db *sql.DB, query string, options ...opt.Option) (*Reader, error) {
	return NewReader(ctx, db, query, func() interface{} {
		return make(map[string]interface{})
	}, options...)
}

func NewSliceReader(ctx context.Context, db *sql.DB, query string, columns int, options ...opt.Option) (*Reader, error) {
	return NewReader(ctx, db, query, func() interface{} {
		return make([]interface{}, columns)
	}, options...)
}
