package load

import (
	"context"
	"database/sql"
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
	dialect  *info.Dialect
	readerID string
	reader   goIo.Reader
	columns  io.Columns
}

//NewSession returns new MySQL session
func NewSession(dialect *info.Dialect) io.Session {
	return &Session{
		dialect: dialect,
	}
}

//Exec inserts given data to database using "LOAD DATA LOCAL INFILE"
//note: local_infile=1 must be enabled on database
func (s *Session) Exec(_ context.Context, data interface{}, db *sql.DB, tableName string) (sql.Result, error) {
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

	return db.Exec(BuildSQL(mysqlLoadConfig, s.readerID, tableName, columns))
}
