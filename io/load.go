package io

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/metadata/info"
)

// LoadExecutorResolver returns new LoadExecutor configured with given Dialect
type LoadExecutorResolver = func(dialect *info.Dialect) LoadExecutor

// LoadExecutor represents load executor interface
type LoadExecutor interface {
	Exec(context context.Context, data interface{}, db *sql.DB, tableName string, options ...loption.Option) (sql.Result, error)
}
