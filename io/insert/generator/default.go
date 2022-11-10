package generator

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"reflect"
	"strings"
	"unsafe"
)

// Default represents generator for default strategy
// TODO: Add order to union
// TODO: Refresh session when rType changes
type Default struct {
	builder     *Builder
	dialect     *info.Dialect
	db          *sql.DB
	session     *sink.Session
	queryMapper read.RowMapper
	columns     []sink.Column
}

// NewDefault creates a default generator
func NewDefault(ctx context.Context, dialect *info.Dialect, db *sql.DB, session *sink.Session) (*Default, error) {
	if session == nil {
		var err error
		if session, err = config.Session(ctx, db); err != nil {
			return nil, err
		}
	}
	return &Default{
		dialect: dialect,
		db:      db,
		session: session,
	}, nil
}

// Apply generated values to the any
func (d *Default) Apply(ctx context.Context, any interface{}, table string, batchSize int) error {
	valueAt, size, err := io.Values(any)
	if err != nil || size == 0 {
		return err
	}

	aRecord := valueAt(0)
	columns, rowMapper, err := d.prepare(ctx, reflect.TypeOf(aRecord), table)
	if err != nil || len(columns) == 0 {
		return err
	}

	d.ensureBuilder(columns, batchSize)
	inBatchSoFar := 0
	values := make([]interface{}, (len(columns)+1)*batchSize) // +1 - Order By column value
	valuesOffset := 0
	batchCount := 0
	i := 0
	for i < size {
		record := valueAt(i)
		ptrs, err := rowMapper(record)
		if err != nil {
			return err
		}

		for j := 0; j < len(columns); j++ {
			values[valuesOffset] = sqlNil(ptrs[j])
			valuesOffset++
		}
		values[valuesOffset] = i
		valuesOffset++
		inBatchSoFar++
		i++

		if inBatchSoFar >= batchSize {
			err = d.flush(ctx, values, batchSize*batchCount, i, valueAt)
			if err != nil {
				return err
			}
			batchCount++
			inBatchSoFar = 0
			valuesOffset = 0
			continue
		}
	}

	if inBatchSoFar > 0 {
		err = d.flush(ctx, values[:valuesOffset], batchCount*batchSize, i, valueAt)
	}
	return err
}

func (d *Default) ensureBuilder(columns []sink.Column, batchSize int) {
	if d.builder == nil {
		d.builder = NewBuilder(columns, batchSize)
		d.builder.Build()
	}
}

func (d *Default) prepare(ctx context.Context, rType reflect.Type, table string) ([]sink.Column, read.RowMapper, error) {
	if d.queryMapper != nil && d.columns != nil {
		return d.columns, d.queryMapper, nil
	}

	if !d.shouldLoadColumnInfo(rType) {
		d.columns = []sink.Column{}
		d.queryMapper = func(target interface{}) ([]interface{}, error) {
			return []interface{}{}, nil
		}

		return d.columns, d.queryMapper, nil
	}

	columns, err := d.loadColumnsInfo(ctx, table)
	if err != nil {
		return nil, nil, err
	}

	ioColumns := make([]io.Column, 0)
	genColumns := make([]sink.Column, 0)
	for i, column := range columns {
		if column.Default == nil || strings.HasPrefix(*column.Default, d.dialect.AutoincrementFunc) {
			continue
		}
		ioColumns = append(ioColumns, io.NewColumn(column.Name, column.Type, nil))
		genColumns = append(genColumns, columns[i])
	}

	ioColumns = append(ioColumns, io.NewColumn(sqlxOrderColumn, "", reflect.TypeOf(0)))

	queryMapper, err := read.NewStructMapper(ioColumns, rType.Elem(), option.TagSqlx, resolveSqlxPosition)

	if err != nil {
		return nil, nil, err
	}

	d.columns = genColumns
	d.queryMapper = queryMapper

	return genColumns, queryMapper, nil
}

func (d *Default) loadColumnsInfo(ctx context.Context, table string) ([]sink.Column, error) {
	return config.Columns(ctx, d.session, d.db, table)
}

func (d *Default) flush(ctx context.Context, values []interface{}, offset int, limit int, at func(index int) interface{}) error {
	batchSize := limit - offset
	SQL := d.builder.Build(option.BatchSize(batchSize))
	dataReader, err := read.New(ctx, d.db, SQL, func() interface{} {
		result := at(offset)
		offset++
		return result
	}, io.Resolve(resolveSqlxPosition))

	if err != nil {
		return err
	}

	err = dataReader.QueryAll(ctx, func(row interface{}) error {
		return nil
	}, values...)

	return err
}

func (d *Default) shouldLoadColumnInfo(rType reflect.Type) bool {
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	for i := 0; i < rType.NumField(); i++ {
		tag := io.ParseTag(rType.Field(i).Tag.Get(option.TagSqlx))
		if tag.Generator != "" && !(tag.PrimaryKey && tag.Autoincrement) {
			return true
		}
	}
	return false
}

func resolveSqlxPosition(_ io.Column) func(pointer unsafe.Pointer) interface{} {
	i := 0
	return func(pointer unsafe.Pointer) interface{} {
		return &i
	}
}
