package validator

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

func (o *Options) resolveDialect(ctx context.Context, db *sql.DB) (*info.Dialect, error) {
	if o.dialect != nil {
		return o.dialect, nil
	}
	if db == nil {
		return nil, fmt.Errorf("constraint validation requires a database")
	}
	var native []option.Option
	if o.transaction != nil {
		native = append(native, o.transaction)
	}
	dialect, err := config.Dialect(ctx, db, native...)
	if err != nil {
		return nil, err
	}
	o.dialect = dialect
	return dialect, nil
}

// reader retains typed SQLX mapping while using the exact caller-owned
// transaction. It must not acquire a second connection to inspect dialects.
func (o *Options) reader(ctx context.Context, db *sql.DB, query string, rowType reflect.Type) (*read.Reader, error) {
	dialect, err := o.resolveDialect(ctx, db)
	if err != nil {
		return nil, err
	}
	newRow := func() interface{} { return reflect.New(rowType).Interface() }
	if o.transaction == nil {
		return read.New(ctx, db, query, newRow, read.WithDialect(dialect))
	}
	statement, err := o.transaction.PrepareContext(ctx, dialect.EnsurePlaceholders(query))
	if err != nil {
		return nil, err
	}
	return read.NewStmt(statement, newRow, read.WithDialect(dialect)), nil
}
