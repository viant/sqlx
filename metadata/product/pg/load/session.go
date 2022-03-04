package load

import (
	"context"
	"database/sql"
	"fmt"
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
	dialect       *info.Dialect
	reader        goIo.Reader
	transactional bool
	tx            *sql.Tx
}

//Exec inserts data to table using "Copy in"
func (s *Session) Exec(ctx context.Context, data interface{}, db *sql.DB, tableName string, options ...option.Option) (sql.Result, error) {
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

	if err = s.begin(ctx, db, options); err != nil {
		return nil, err
	}

	stmt, err := s.tx.Prepare(pq.CopyIn(tableName, names...))
	if err != nil {
		return nil, s.end(err)
	}
	result, err := s.load(ctx, dataAccessor, size, mapper, stmt)
	if err != nil {
		return result, s.end(err)
	}
	exec, err := stmt.ExecContext(ctx)
	return exec, s.end(err)

}

func (s *Session) load(ctx context.Context, dataAccessor io.ValueAccessor, size int, mapper read.RowMapper, stmt *sql.Stmt) (sql.Result, error) {
	var ptrs []interface{}
	var err error
	for i := 0; i < size; i++ {
		ptrs, err = mapper(dataAccessor(i))
		if err != nil {
			return nil, err
		}
		_, err = stmt.ExecContext(ctx, ptrs...)
		if err != nil {
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

func (s *Session) begin(ctx context.Context, db *sql.DB, options []option.Option) error {
	var err error
	s.transactional = s.dialect.Transactional
	if option.Assign(options, s.tx) { //transaction supply as option, do not manage locally transaction
		s.transactional = false
	}
	if s.transactional {
		if s.tx, err = db.BeginTx(ctx, nil); err != nil {
			if rErr := s.tx.Rollback(); rErr != nil {
				return fmt.Errorf("%w, %v", err, rErr)
			}
		}
	}
	return nil
}

func (s *Session) end(err error) error {
	if err != nil && s.tx != nil {
		if rErr := s.tx.Rollback(); rErr != nil {
			return fmt.Errorf("failed to rollback: %w, %v", err, rErr)
		}
		return err
	}
	if s.transactional {
		err = s.tx.Commit()
	}
	return err
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
