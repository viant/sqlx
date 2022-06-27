package read

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache"
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
	cache        *cache.Cache
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

	cacheEntry, err := r.cacheEntry(ctx, r.query, args)
	if err != nil {
		return err
	}

	if rows.Next() {
		if err = r.read(rows, &mapper, &columns, &types, emit, cacheEntry); err != nil {
			return err
		}
	}

	if err = r.updateCacheIfNeeded(ctx, cacheEntry); err != nil {
		return err
	}

	return nil
}

//QueryAll query all
func (r *Reader) QueryAll(ctx context.Context, emit func(row interface{}) error, args ...interface{}) error {
	rows, err := r.stmt.QueryContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("failed to run query: %v, due to %s", r.query, err)
	}

	entry, err := r.cacheEntry(ctx, r.query, args)
	if err != nil {
		return err
	}

	defer rows.Close()
	err = r.ReadAll(rows, emit, entry)
	if err != nil {
		return err
	}

	if err = r.updateCacheIfNeeded(ctx, entry); err != nil {
		return err
	}

	return err
}

//ReadAll read all
func (r *Reader) ReadAll(rows *sql.Rows, emit func(row interface{}) error, options ...option.Option) error {
	cacheEntry := r.getCacheEntry(options)

	var mapper RowMapper
	var columns []io.Column
	var types []xunsafe.Type

	next := r.nexter(cacheEntry, rows)
	for next() {
		if err := r.read(rows, &mapper, &columns, &types, emit, cacheEntry); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (r *Reader) getCacheEntry(options []option.Option) *cache.Entry {
	var cacheEntry *cache.Entry
	for _, o := range options {
		switch actual := o.(type) {
		case *cache.Entry:
			cacheEntry = actual
		}
	}
	return cacheEntry
}

func (r *Reader) nexter(entry *cache.Entry, rows *sql.Rows) func() bool {
	if entry != nil && entry.Has() {
		return entry.Next
	}

	return rows.Next
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

func (r *Reader) read(rows *sql.Rows, mapperPtr *RowMapper, columnsPtr *[]io.Column, columnTypes *[]xunsafe.Type, emit func(row interface{}) error, cacheEntry *cache.Entry) error {
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

	scanner := r.scanner(cacheEntry, rows)
	err = scanner(rowValues...)

	if err != nil {
		return fmt.Errorf("failed to scan %v, due to %w", r.query, err)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to read records: %w", err)
	}

	r.ensureDereferences(row, rowValues, columnsPtr, columnTypes)
	if cacheEntry != nil && !cacheEntry.Has() {
		cacheEntry.AddRow(rowValues)
	}

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

func (r *Reader) cacheEntry(ctx context.Context, sql string, args []interface{}) (*cache.Entry, error) {
	if r.cache != nil {
		return r.cache.Get(ctx, sql, args)
	}

	return nil, nil
}

func (r *Reader) scanner(entry *cache.Entry, rows *sql.Rows) func(args ...interface{}) error {
	if entry != nil && entry.Has() {
		return func(args ...interface{}) error {
			r.cache.CreateXTypes(args)
			return entry.Scan(args...)
		}
	}

	if r.cache != nil {
		return func(args ...interface{}) error {
			r.cache.CreateXTypes(args)
			return rows.Scan(args...)
		}
	}

	return rows.Scan
}

func (r *Reader) updateCacheIfNeeded(ctx context.Context, entry *cache.Entry) error {
	if entry == nil || entry.Has() {
		return nil
	}

	return r.cache.Put(ctx, entry)
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
	newStmt := NewStmt(stmt, newRow, options...)
	newStmt.query = query
	return newStmt, err
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
	var readerCache *cache.Cache
	if !option.Assign(options, &getRowMapper) {
		getRowMapper = newRowMapper
	}
	option.Assign(options, &unmappedFn)

	for _, anOption := range options {
		switch actual := anOption.(type) {
		case *cache.Cache:
			readerCache = actual
		}
	}

	return &Reader{newRow: newRow, stmt: stmt, tagName: option.Options(options).Tag(), getRowMapper: newRowMapper, unmappedFn: unmappedFn, cache: readerCache}
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
