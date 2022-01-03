package updater

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/internal"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

//Updater represents updater
type Updater struct {
	db        *sql.DB
	tableName string
	dialect   *info.Dialect
	tagName   string
	builder   io.Builder
	mapper    io.ColumnMapper
	columns   io.Columns
	binder    io.PlaceholderBinder
}

//New creates an updater
func New(ctx context.Context, db *sql.DB, tableName string, options ...option.Option) (*Updater, error) {
	dialect, err := internal.Dialect(ctx, db, options)
	if err != nil {
		return nil, err
	}
	result := &Updater{
		db:        db,
		dialect:   dialect,
		tableName: tableName,
		tagName:   option.Options(options).Tag(),
	}

	return result, nil
}
