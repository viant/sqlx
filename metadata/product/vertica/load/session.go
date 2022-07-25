package load

import (
	"context"
	"database/sql"
	vcontext "github.com/vertica/vertica-sql-go"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/load/reader"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

// To specify the ObjectSeparator (RECORD TERMINATOR) as non-printing characters,
// use either the extended string syntax or Unicode string literals.
// Examples for line feed: E'\n' or U&'\000a'
var verticaLoadConfig = &reader.Config{
	FieldSeparator:  `,`,
	ObjectSeparator: `#`,
	EncloseBy:       `*`,
	EscapeBy:        `^`,
	NullValue:       "null",
}

//Session represents Vertica session
type Session struct {
	*io.Transaction
	dialect *info.Dialect
	columns io.Columns
}

//NewSession returns new MySQL session
func NewSession(dialect *info.Dialect) io.Session {
	return &Session{
		dialect: dialect,
	}
}

//Exec inserts given data to database using "COPY FROM STDIN "
func (s *Session) Exec(ctx context.Context, data interface{}, db *sql.DB, tableName string, options ...option.Option) (sql.Result, error) {
	dataReader, dataType, err := reader.NewReader(data, verticaLoadConfig)
	if err != nil {
		return nil, err
	}

	columns, err := io.StructColumns(dataType, option.TagSqlx)
	if err != nil {
		return nil, err
	}

	vCtx := vcontext.NewVerticaContext(ctx)
	err = vCtx.SetCopyInputStream(dataReader)
	if err != nil {
		return nil, err
	}

	_ = vCtx.SetCopyBlockSizeBytes(32768)

	if err = s.begin(ctx, db, options); err != nil {
		return nil, err
	}

	SQL := BuildSQL(verticaLoadConfig, tableName, columns)

	result := &io.QueryResult{}
	if s.Transaction != nil {
		result.Result, err = s.Transaction.ExecContext(vCtx, SQL)
		err = s.end(err)
	}

	if err != nil {
		return result, err
	}

	// Omitting bug: 0 affected rows
	result.Rows, err = result.Result.RowsAffected()
	if err != nil && err.Error() == "no RowsAffected available after DDL statement" {
		result.Rows = int64(dataReader.ItemCount()) //assigning explicitly value
		return result, nil
	}

	return result, err
}

func (s *Session) begin(ctx context.Context, db *sql.DB, options []option.Option) error {
	var err error
	s.Transaction, err = io.TransactionFor(ctx, s.dialect, db, options)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) end(err error) error {
	if s.Tx == nil {
		return err
	}

	if err != nil {
		return s.Transaction.RollbackWithErr(err)
	}

	return s.Transaction.Commit()
}
