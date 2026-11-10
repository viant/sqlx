package load

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/load/reader/csv"
	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	goIo "io"
	"time"
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
// Caller-owned transactions retain their sql_notes setting. Configure sql_notes=0
// on their connection for large loads that generate informational rounding notes.
func (s *Session) Exec(ctx context.Context, data interface{}, db *sql.DB, tableName string, options ...loption.Option) (sql.Result, error) {
	loadConfig := *mysqlLoadConfig
	dataReader, dataType, err := csv.NewReader(data, &loadConfig, io.StringifierTimeLayout("2006-01-02T15:04:05-07:00"))
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
	SQL := BuildSQL(&loadConfig, readerID, tableName, columns, options...)

	result := &io.QueryResult{}
	result.Result, err = s.executeLoad(ctx, db, SQL, options)

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

func (s *Session) executeLoad(ctx context.Context, db *sql.DB, SQL string, options []loption.Option) (result sql.Result, err error) {
	if tx := loption.NewOptions(options...).GetTransaction(); tx != nil {
		s.Transaction = &io.Transaction{Tx: tx, Global: true}
		result, err = tx.ExecContext(ctx, SQL)
		if err == nil {
			err = s.checkWarnings(ctx, tx)
		}
		return result, err
	}
	s.Transaction = nil
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if !s.dialect.Transactional {
		result, err = conn.ExecContext(ctx, SQL)
		if err == nil {
			err = s.checkWarnings(ctx, conn)
		}
		return result, err
	}

	var originalNotes int
	if err = conn.QueryRowContext(ctx, "SELECT @@session.sql_notes").Scan(&originalNotes); err != nil {
		return nil, err
	}
	var tx *sql.Tx
	restore := originalNotes != 0
	discard := false
	complete := false
	// Retain the connection beyond the transaction: cancellation can end the
	// transaction before restoration, but session settings are not rolled back.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if tx != nil {
			if complete && err == nil && restore {
				_, restoreErr := tx.ExecContext(cleanup, "SET SESSION sql_notes=1")
				if restoreErr != nil {
					err = fmt.Errorf("restore LOAD DATA sql_notes: %w", restoreErr)
					discard = true
				} else {
					restore = false
				}
			}
			if complete && err == nil {
				err = tx.Commit()
			} else if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				err = errors.Join(err, fmt.Errorf("rollback LOAD DATA: %w", rollbackErr))
			}
		}
		if restore {
			if _, restoreErr := conn.ExecContext(cleanup, "SET SESSION sql_notes=1"); restoreErr != nil {
				err = errors.Join(err, fmt.Errorf("restore LOAD DATA sql_notes: %w", restoreErr))
				discard = true
			}
		}
		if discard {
			if discardErr := conn.Raw(func(interface{}) error { return driver.ErrBadConn }); discardErr != nil && !errors.Is(discardErr, driver.ErrBadConn) && !errors.Is(discardErr, sql.ErrConnDone) {
				err = errors.Join(err, fmt.Errorf("discard LOAD DATA connection: %w", discardErr))
			}
		}
	}()
	if restore {
		if _, err = conn.ExecContext(ctx, "SET SESSION sql_notes=0"); err != nil {
			return nil, err
		}
	}
	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	s.Transaction = &io.Transaction{Tx: tx}
	result, err = tx.ExecContext(ctx, SQL)
	if err == nil {
		err = s.checkWarnings(ctx, tx)
	}
	if err == nil {
		err = ctx.Err()
	}
	complete = err == nil
	return result, err
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
