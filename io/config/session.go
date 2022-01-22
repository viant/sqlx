package config

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
)

//Session retrieve basic data from the database connection
func Session(ctx context.Context, db *sql.DB) (*sink.Session, error) {
	meta := metadata.New()
	session := new(sink.Session)
	err := meta.Info(ctx, db, info.KindSession, session)
	return session, err
}
