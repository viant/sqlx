package cache

import (
	"context"
	"github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
)

type Source interface {
	ConvertColumns() ([]io.Column, error)
	Scanner(context.Context) ScannerFn
	XTypes() []*xunsafe.Type
	CheckType(ctx context.Context, values []interface{}) (bool, error)
	Close(ctx context.Context) error
	Next() bool
	Rollback(ctx context.Context) error
	Err() error
}
