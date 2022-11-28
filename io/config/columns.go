package config

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

//Columns returns table columns
func Columns(ctx context.Context, session *sink.Session, db *sql.DB, table string, options ...option.Option) ([]sink.Column, error) {
	meta := metadata.New()

	tableColumns := make([]sink.Column, 0)
	if options == nil {
		options = make(option.Options, 0)
	}
	options = append(options, option.NewArgs(session.Catalog, session.Schema, table))
	err := meta.Info(ctx, db, info.KindTable, &tableColumns, options...)

	return tableColumns, err
}
