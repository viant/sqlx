package io

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/metadata/info"
)

//SessionResolver returns new Session configured with given Dialect
type SessionResolver = func(dialect *info.Dialect) Session

//Session represents load session e.g. MySQL "LOAD DATA LOCAL INFILE"
type Session interface {
	Exec(context context.Context, data interface{}, db *sql.DB, tableName string) (sql.Result, error)
}
