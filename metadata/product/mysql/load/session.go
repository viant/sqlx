package load

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/load/reader/csv"
	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	goIo "io"
)

var mysqlLoadConfig = &csv.Config{
	FieldSeparator:  `,`,
	ObjectSeparator: `#`,
	EncloseBy:       `*`,
	EscapeBy:        `^`,
	NullValue:       "^N",
	RawNullValue:    true,
}

// Session represents MySQL session
type Session struct {
	*io.Transaction
	dialect *info.Dialect
	columns io.Columns
	loption.Options
}

type loadConnection interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

// NewSession returns new MySQL session
func NewSession(dialect *info.Dialect) io.LoadExecutor {
	return &Session{
		dialect: dialect,
	}
}

// Exec inserts given data to database using "LOAD DATA LOCAL INFILE"
// note: local_infile=1 must be enabled on database
func (s *Session) Exec(ctx context.Context, data interface{}, db *sql.DB, tableName string, options ...loption.Option) (sql.Result, error) {
	loadConfig := *mysqlLoadConfig
	dataReader, dataType, err := csv.NewReader(data, &loadConfig)
	if err != nil {
		return nil, err
	}

	columns, err := io.StructColumns(dataType, io.TagSqlx, option.StructOrderedColumns(true))
	if err != nil {
		return nil, err
	}

	readerResolver := func() goIo.Reader {
		return dataReader
	}

	readerID := uuid.New().String()
	mysql.RegisterReaderHandler(readerID, readerResolver)
	defer mysql.DeregisterReaderHandler(readerID)
	if err = s.begin(ctx, db, options); err != nil {
		return nil, err
	}

	SQL := BuildSQL(&loadConfig, readerID, tableName, columns, options...)

	result := &io.QueryResult{}
	var connection loadConnection
	if s.Transaction != nil {
		connection = s.Transaction
	} else {
		// Diagnostics belong to the connection that executed LOAD DATA.
		conn, connErr := db.Conn(ctx)
		if connErr != nil {
			return nil, connErr
		}
		defer conn.Close()
		connection = conn
	}
	result.Result, err = connection.ExecContext(ctx, SQL)
	if err == nil {
		err = s.checkWarnings(ctx, connection)
	}
	err = s.end(err)

	if err != nil {
		return result, err
	}

	// Omitting bug: 0 affected rows
	result.Rows, err = result.Result.RowsAffected()
	if err == nil && result.Rows == 0 && dataReader.ItemCount() > 0 {
		result.Rows = int64(dataReader.ItemCount()) //assigning explicitly value
		return result, nil
	}

	return result, err
}

func (s *Session) begin(ctx context.Context, db *sql.DB, options []loption.Option) error {
	var err error
	loadOpts := loption.NewOptions(options...)
	var opts []option.Option
	if tx := loadOpts.GetTransaction(); tx != nil {
		opts = append(opts, tx)
	}

	s.Transaction, err = io.TransactionFor(ctx, s.dialect, db, opts)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) end(err error) error {
	if s.Transaction == nil || s.Tx == nil {
		return err
	}

	if err != nil {
		return s.Transaction.RollbackWithErr(err)
	}

	return s.Transaction.Commit()
}

// LOCAL INFILE can downgrade duplicate keys and data coercion to warnings.
// Read diagnostics on the owning transaction before any commit, and make such
// partial loads fail rather than claiming successful insertion of every row.
// Notes alone are allowed only when the complete diagnostic list is available.
func (s *Session) checkWarnings(ctx context.Context, connection loadConnection) error {
	var total uint64
	// Unlike SELECT @@warning_count, this does not clear the diagnostic list.
	if err := connection.QueryRowContext(ctx, "SHOW COUNT(*) WARNINGS").Scan(&total); err != nil {
		return err
	}
	rows, err := connection.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		return err
	}
	defer rows.Close()
	var seen uint64
	for rows.Next() {
		var level, message string
		var code int
		if err := rows.Scan(&level, &code, &message); err != nil {
			return err
		}
		seen++
		if level != "Note" {
			return fmt.Errorf("LOAD DATA %s %d: %s", level, code, message)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if seen < total {
		return fmt.Errorf("LOAD DATA diagnostics incomplete: received %d of %d", seen, total)
	}
	return nil
}
