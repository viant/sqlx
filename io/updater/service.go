package updater

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

//Service represents updater
type Service struct {
	db        *sql.DB
	tableName string
	dialect   *info.Dialect
	tagName   string
	builder   io.Builder
	mapper    io.ColumnMapper
	columns   io.Columns
	binder    io.PlaceholderBinder
}

func (u *Service) Update() {

}

//New creates an updater
func New(ctx context.Context, db *sql.DB, tableName string, options ...option.Option) (*Service, error) {
	dialect, err := config.Dialect(ctx, db, options)
	if err != nil {
		return nil, err
	}
	var columnMapper io.ColumnMapper
	if !option.Assign(options, &columnMapper) {
		columnMapper = io.StructColumnMapper
	}
	result := &Service{
		db:        db,
		dialect:   dialect,
		tableName: tableName,
		mapper:    columnMapper,
		tagName:   option.Options(options).Tag(),
	}

	return result, nil
}
