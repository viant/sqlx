package read

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
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
	getRowMapper NewRowMapper
	unmappedFn   io.Resolve
	shallDeref   bool
}

//QuerySingle returns single row
func (r *Reader) QuerySingle(ctx context.Context, emit func(row interface{}) error, args ...interface{}) error {
	rows, err := r.stmt.QueryContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("failed to run query: %v, due to %s", r.query, err)
	}
	defer rows.Close()
	var mapper RowMapper
	var columns []io.Column
	var types []xunsafe.Type
	if rows.Next() {
		if err = r.read(rows, &mapper, &columns, &types, emit); err != nil {
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
	var columns []io.Column
	var types []xunsafe.Type
	for rows.Next() {
		if err := r.read(rows, &mapper, &columns, &types, emit); err != nil {
			return err
		}
	}
	return rows.Err()
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

func (r *Reader) read(rows *sql.Rows, mapperPtr *RowMapper, columnsPtr *[]io.Column, columnTypes *[]xunsafe.Type, emit func(row interface{}) error) error {
	row := r.newRow()
	if r.targetType == nil {
		r.targetType = reflect.TypeOf(row)
		r.shallDeref = r.targetType.Kind() == reflect.Map || r.targetType.Kind() == reflect.Slice
	}
	mapper, err := r.ensureRowMapper(rows, mapperPtr, columnsPtr)
	if err != nil {
		return err
	}
	rowValues, err := mapper(row)
	if err != nil {
		return err
	}
	err = rows.Scan(rowValues...)
	if err != nil {
		return fmt.Errorf("failed to scan %v, due to %w", r.query, err)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to read records: %w", err)
	}
	r.ensureDereferences(row, rowValues, columnsPtr, columnTypes)
	return emit(row)
}

func (r *Reader) ensureDereferences(row interface{}, rowValues []interface{}, columnsPtr *[]io.Column, typesPtr *[]xunsafe.Type) {
	if !r.shallDeref {
		return
	}
	if len(*typesPtr) == 0 {
		*typesPtr = make([]xunsafe.Type, len(*columnsPtr))
		for i, column := range *columnsPtr {
			(*typesPtr)[i] = *xunsafe.NewType(column.ScanType())
		}
	}
	for i, value := range rowValues {
		rowValues[i] = (*typesPtr)[i].Deref(value)
	}
	switch actual := row.(type) {
	case map[string]interface{}:
		for i, column := range *columnsPtr {
			actual[column.Name()] = rowValues[i]
		}
	case []interface{}:
		copy(actual, rowValues)
	}
}

func (r *Reader) ensureRowMapper(rows *sql.Rows, mapperPtr *RowMapper, columnsPtr *[]io.Column) (RowMapper, error) {
	if *mapperPtr != nil {
		return *mapperPtr, nil
	}
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columns := io.NamesToColumns(columnNames)
	if columnsTypes, _ := rows.ColumnTypes(); len(columnNames) > 0 {
		columns = io.TypesToColumns(columnsTypes)
	}
	*columnsPtr = columns
	var mapper RowMapper
	if mapper, err = r.getRowMapper(columns, r.targetType, r.tagName, r.unmappedFn); err != nil {
		return nil, fmt.Errorf("failed to get row mapper, due to %w", err)
	}
	*mapperPtr = mapper
	return mapper, nil
}

//Stmt returns *sql.Stmt associated with Reader
func (r *Reader) Stmt() *sql.Stmt {
	return r.stmt
}

//New creates a records to a structs reader
func New(ctx context.Context, db *sql.DB, query string, newRow func() interface{}, options ...option.Option) (*Reader, error) {
	dialect := ensureDialect(options, db)
	if dialect != nil {
		query = dialect.EnsurePlaceholders(query)
	}
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare query: %v, due to %w", query, err)
	}
	return NewStmt(stmt, newRow, options...), err
}

func ensureDialect(options []option.Option, db *sql.DB) *info.Dialect {
	dialect := option.Options(options).Dialect()
	if dialect == nil {
		product := registry.MatchProduct(db)
		if product == nil {
			return nil
		}
		dialect = registry.LookupDialect(product)
	}
	return dialect
}

//NewStmt creates a statement reader
func NewStmt(stmt *sql.Stmt, newRow func() interface{}, options ...option.Option) *Reader {
	var getRowMapper NewRowMapper
	var unmappedFn io.Resolve
	if !option.Assign(options, &getRowMapper) {
		getRowMapper = newRowMapper
	}
	option.Assign(options, &unmappedFn)
	return &Reader{newRow: newRow, stmt: stmt, tagName: option.Options(options).Tag(), getRowMapper: newRowMapper, unmappedFn: unmappedFn}
}

//NewMap creates records to map reader
func NewMap(ctx context.Context, db *sql.DB, query string, options ...option.Option) (*Reader, error) {
	return New(ctx, db, query, func() interface{} {
		return make(map[string]interface{})
	}, options...)
}

//NewSlice create records to a slice reader
func NewSlice(ctx context.Context, db *sql.DB, query string, columns int, options ...option.Option) (*Reader, error) {
	return New(ctx, db, query, func() interface{} {
		return make([]interface{}, columns)
	}, options...)
}
