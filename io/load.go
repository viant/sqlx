package io

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

//SessionResolver returns new Session configured with given Dialect
type SessionResolver = func(dialect *info.Dialect) Session

//Session represents load session e.g. MySQL "LOAD DATA LOCAL INFILE"
type Session interface {
	Exec(context context.Context, data interface{}, db *sql.DB, tableName string, options ...option.Option) (sql.Result, error)
}
