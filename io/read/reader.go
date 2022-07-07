package read

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/mapper"
	source2 "github.com/viant/sqlx/io/read/source"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/option"
	"reflect"
)

//Reader represents generic query reader
type Reader struct {
	query          string
	newRow         func() interface{}
	targetType     reflect.Type
	tagName        string
	stmt           *sql.Stmt
	rows           *sql.Rows
	getRowMapper   NewRowMapper
	unmappedFn     io.Resolve
	shallDeref     bool
	cache          *cache.Service
	mapperCache    *mapper.Cache
	targetDatatype string
}

//QuerySingle returns single row
func (r *Reader) QuerySingle(ctx context.Context, emit func(row interface{}) error, args ...interface{}) error {
	rows, err := r.stmt.QueryContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("failed to run query: %v, due to %s", r.query, err)
	}

	defer rows.Close()
	newRows, err := NewRows(rows, nil, nil)
	if err != nil {
		return err
	}

	var mapper RowMapper
	if rows.Next() {
		if err = r.read(ctx, newRows, &mapper, emit, nil); err != nil {
			return err
		}
	}

	return nil
}

//QueryAll query all
func (r *Reader) QueryAll(ctx context.Context, emit func(row interface{}) error, args ...interface{}) error {
	entry, err := r.cacheEntry(ctx, r.query, args)
	if err != nil {
		return err
	}

	rows, source, err := r.createSource(ctx, entry, args)
	if err != nil {
		return err
	}

	if err = r.applyRowsIfNeeded(entry, rows); err != nil {
		return err
	}

	err = r.readAll(ctx, emit, entry, source)
	if err != nil {
		return err
	}

	return err
}

func (r *Reader) createSource(ctx context.Context, entry *cache.Entry, args []interface{}) (*sql.Rows, source2.Source, error) {
	if entry == nil || !entry.Has() || len(entry.Meta.Fields) == 0 {
		rows, err := r.stmt.QueryContext(ctx, args...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to run query: %v, due to %s", r.query, err)
		}

		source, err := NewRows(rows, r.cache, entry)
		if err != nil {
			return nil, nil, err
		}

		return rows, source, nil
	}

	source, err := r.cache.AsSource(ctx, entry)
	if err != nil {
		return nil, nil, err
	}

	return nil, source, nil
}

//ReadAll read all
func (r *Reader) ReadAll(ctx context.Context, rows *sql.Rows, emit func(row interface{}) error, options ...option.Option) error {
	cacheEntry := r.getCacheEntry(options)
	readerRows, err := NewRows(rows, r.cache, cacheEntry)
	if err != nil {
		return err
	}

	if err = r.readAll(ctx, emit, cacheEntry, readerRows); err != nil {
		return err
	}

	return rows.Err()
}

func (r *Reader) readAll(ctx context.Context, emit func(row interface{}) error, cacheEntry *cache.Entry, source source2.Source) error {
	var err error
	var mapper RowMapper

	defer source.Close(ctx) //TODO: Should we log it?
	for source.Next() {
		if err = r.read(ctx, source, &mapper, emit, cacheEntry); err != nil {
			return err
		}
	}
	return nil
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

func (r *Reader) read(ctx context.Context, source source2.Source, mapperPtr *RowMapper, emit func(row interface{}) error, cacheEntry *cache.Entry) error {
	row := r.newRow()
	if r.targetType == nil {
		r.targetType = reflect.TypeOf(row)
		r.targetDatatype = r.targetType.String()
		r.shallDeref = r.targetType.Kind() == reflect.Map || r.targetType.Kind() == reflect.Slice
	}

	mapper, err := r.ensureRowMapper(source, mapperPtr)
	if err != nil {
		return err
	}

	rowValues, err := mapper(row)
	if err != nil {
		return err
	}

	typeMatches, err := source.CheckType(ctx, rowValues)
	if !typeMatches {
		return fmt.Errorf("invalid cache type")
	}

	scanner := source.Scanner(ctx)
	if err = scanner(rowValues...); err != nil {
		return fmt.Errorf("failed to scan %v, due to %w", r.query, err)
	}

	r.ensureDereferences(row, source, rowValues)
	if cacheEntry != nil && !cacheEntry.Has() {
		if err = r.cache.AddValues(ctx, cacheEntry, rowValues); err != nil {
			return err
		}
	}

	return emit(row)
}

func (r *Reader) ensureDereferences(row interface{}, source source2.Source, rowValues []interface{}) {
	if !r.shallDeref {
		return
	}

	columns := source.ConvertColumns()
	xTypes := source.XTypes()
	for i, value := range rowValues {
		rowValues[i] = (xTypes)[i].Deref(value)
	}

	switch actual := row.(type) {
	case map[string]interface{}:
		for i, column := range columns {
			actual[column.Name()] = rowValues[i]
		}
	case []interface{}:
		copy(actual, rowValues)
	}
}

func (r *Reader) ensureRowMapper(source source2.Source, mapperPtr *RowMapper) (RowMapper, error) {
	if *mapperPtr != nil {
		return *mapperPtr, nil
	}

	columns := source.ConvertColumns()

	var mapper RowMapper
	var err error

	options := make(option.Options, 0)
	if r.mapperCache != nil {
		options = append(options, r.mapperCache)
	}

	if mapper, err = r.getRowMapper(columns, r.targetType, r.tagName, r.unmappedFn, options); err != nil {
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
		entry, err := r.cache.Get(ctx, sql, args)
		return entry, err
	}

	return nil, nil
}

func (r *Reader) applyRowsIfNeeded(entry *cache.Entry, rows *sql.Rows) error {
	if entry == nil {
		return nil
	}

	return r.cache.AssignRows(entry, rows)
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
	var readerCache *cache.Service
	if !option.Assign(options, &getRowMapper) {
		getRowMapper = newRowMapper
	}
	option.Assign(options, &unmappedFn)

	var mapperCache *mapper.Cache
	for _, anOption := range options {
		switch actual := anOption.(type) {
		case *cache.Service:
			readerCache = actual
		case *mapper.Cache:
			mapperCache = actual
		}
	}

	return &Reader{newRow: newRow, stmt: stmt, tagName: option.Options(options).Tag(), getRowMapper: newRowMapper, unmappedFn: unmappedFn, cache: readerCache, mapperCache: mapperCache}
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
