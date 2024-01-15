package io

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/moption"
)

// MergeExecutorResolver returns a MergeExecutor configured with given Dialect and MergeConfig
type MergeExecutorResolver = func(dialect *info.Dialect, config info.MergeConfig) (MergeExecutor, error)

// MergeExecutor represents merge executor interface
type MergeExecutor interface {
	Exec(context context.Context, data interface{}, db *sql.DB, tableName string, options ...moption.Option) (info.MergeResult, error)
}
