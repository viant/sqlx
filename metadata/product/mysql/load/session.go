package load

import (
	"context"
	"database/sql"
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
	NullValue:       "null",
}

// Session represents MySQL session
type Session struct {
	*io.Transaction
	dialect *info.Dialect
	columns io.Columns
	loption.Options
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
	dataReader, dataType, err := csv.NewReader(data, mysqlLoadConfig)
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

	SQL := BuildSQL(mysqlLoadConfig, readerID, tableName, columns, options...)

	result := &io.QueryResult{}
	if s.Transaction != nil {
		result.Result, err = s.Transaction.ExecContext(ctx, SQL)
	} else {
		result.Result, err = db.ExecContext(ctx, SQL)
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
