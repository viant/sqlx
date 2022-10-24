package cache

import (
	"context"
	"database/sql"
)

type ScannerFn func(args ...interface{}) error
type Cache interface {
	AsSource(ctx context.Context, entry *Entry) (Source, error)
	AddValues(ctx context.Context, entry *Entry, values []interface{}) error
	Get(ctx context.Context, sql string, args []interface{}, options ...interface{}) (*Entry, error)
	AssignRows(entry *Entry, rows *sql.Rows) error
	UpdateType(ctx context.Context, entry *Entry, args []interface{}) (bool, error)
	Close(ctx context.Context, entry *Entry) error
	Delete(todo context.Context, entry *Entry) error
	Rollback(ctx context.Context, entry *Entry) error
	IndexBy(ctx context.Context, db *sql.DB, column, SQL string, args []interface{}) (int, error)
}
