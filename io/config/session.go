package config

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

//Session retrieve basic data from the database connection
func Session(ctx context.Context, db *sql.DB, options ...option.Option) (*sink.Session, error) {
	meta := metadata.New()
	session := new(sink.Session)
	err := meta.Info(ctx, db, info.KindSession, session, options...)
	return session, err
}
