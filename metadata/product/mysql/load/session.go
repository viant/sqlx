package load

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/load/reader"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	goIo "io"
)

var mysqlLoadConfig = &reader.Config{
	FieldSeparator:  `,`,
	ObjectSeparator: `#`,
	EncloseBy:       `*`,
	EscapeBy:        `^`,
	NullValue:       "null",
}

//Session represents MySQL session
type Session struct {
	dialect       *info.Dialect
	readerID      string
	tx            *sql.Tx
	reader        goIo.Reader
	columns       io.Columns
	transactional bool
}

//NewSession returns new MySQL session
func NewSession(dialect *info.Dialect) io.Session {
	return &Session{
		dialect: dialect,
	}
}

//Exec inserts given data to database using "LOAD DATA LOCAL INFILE"
//note: local_infile=1 must be enabled on database
func (s *Session) Exec(ctx context.Context, data interface{}, db *sql.DB, tableName string, options ...option.Option) (sql.Result, error) {
	dataReader, dataType, err := reader.NewReader(data, mysqlLoadConfig)
	if err != nil {
		return nil, err
	}

	columns, err := io.StructColumns(dataType, option.TagSqlx)
	if err != nil {
		return nil, err
	}

	readerResolver := func() goIo.Reader {
		return dataReader
	}

	s.readerID = uuid.New().String()
	mysql.RegisterReaderHandler(s.readerID, readerResolver)
	defer mysql.DeregisterReaderHandler(s.readerID)
	if err = s.begin(ctx, db, options); err != nil {
		return nil, err
	}
	SQL := BuildSQL(mysqlLoadConfig, s.readerID, tableName, columns)
	var result sql.Result
	if s.tx != nil {
		result, err = s.tx.Exec(SQL)
	} else {
		result, err = db.Exec(SQL)
	}
	err = s.end(err)
	return result, err
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
	if err != nil {
		if s.transactional {
			if rErr := s.tx.Rollback(); rErr != nil {
				return fmt.Errorf("failed to rollback: %w, %v", err, rErr)
			}
		}
		return err
	}
	if s.transactional {
		err = s.tx.Commit()
	}
	return err
}
