package source

import (
	"context"
	"github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
)

type Source interface {
	ConvertColumns() []io.Column
	Scanner(context.Context) func(args ...interface{}) error
	XTypes() []*xunsafe.Type
	CheckType(ctx context.Context, values []interface{}) (bool, error)
	Close(ctx context.Context) error
	Next() bool
}
