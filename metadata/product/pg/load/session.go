package load

import (
	"context"
	"database/sql"
	"github.com/lib/pq"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	goIo "io"
	"strings"
	"unsafe"
)

//Session represents Postgres load session
type Session struct {
	dialect *info.Dialect
	reader  goIo.Reader
}

//Exec inserts data to table using "Copy in"
func (s *Session) Exec(ctx context.Context, data interface{}, db *sql.DB, tableName string) (sql.Result, error) {
	dataAccessor, size, err := io.Values(data)
	if err != nil {
		return nil, err
	}

	actualStructType := io.EnsureDereference(dataAccessor(0))
	columns, err := io.StructColumns(actualStructType, option.TagSqlx)
	if err != nil {
		return nil, err
	}

	mapper, err := read.NewStructMapper(columns, actualStructType, option.TagSqlx, columnResolver)
	if err != nil {
		return nil, err
	}

	names := s.mapColumnsToLowerCasedNames(columns)
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	stmt, err := tx.Prepare(pq.CopyIn(tableName, names...))
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	result, err := s.load(ctx, dataAccessor, size, mapper, tx, stmt)
	if err != nil {
		return result, err
	}

	exec, err := stmt.ExecContext(ctx)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	tx.Commit()
	return exec, err
}

func (s *Session) load(ctx context.Context, dataAccessor io.ValueAccessor, size int, mapper read.RowMapper, tx *sql.Tx, stmt *sql.Stmt) (sql.Result, error) {
	var ptrs []interface{}
	var err error
	for i := 0; i < size; i++ {
		ptrs, err = mapper(dataAccessor(i))
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		_, err = stmt.ExecContext(ctx, ptrs...)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	return nil, err
}

func (s *Session) mapColumnsToLowerCasedNames(columns []io.Column) []string {
	names := make([]string, len(columns))
	for i := 0; i < len(columns); i++ {
		// pq.CopyInSchema put column names into quotes - it makes SQL statement case sensitive
		names[i] = strings.ToLower(columns[i].Name())
	}
	return names
}

//NewSession returns new Postgres load session
func NewSession(dialect *info.Dialect) io.Session {
	return &Session{
		dialect: dialect,
	}
}

func columnResolver(_ io.Column) func(pointer unsafe.Pointer) interface{} {
	columnIndex := 0
	return func(pointer unsafe.Pointer) interface{} {
		return columnIndex
	}
}
