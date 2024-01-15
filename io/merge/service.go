package merge

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/moption"
)

// Service represents merge service
type Service struct {
	dialect   *info.Dialect
	tableName string
	columns   []sink.Column
	db        *sql.DB
}

// New creates instance of Service
func New(ctx context.Context, db *sql.DB, table string) (*Service, error) {
	dialect, err := config.Dialect(ctx, db)
	if err != nil {
		return nil, err
	}

	return &Service{
		tableName: table,
		db:        db,
		dialect:   dialect,
	}, nil
}

// Exec performs database-specific merge operations
func (s *Service) Exec(ctx context.Context, any interface{}, mConfig info.MergeConfig, options ...moption.Option) (info.MergeResult, error) {
	dialect, err := s.ensureDialect(ctx)
	if err != nil {
		return nil, err
	}
	executor, err := config.MergeExecutor(dialect, mConfig)
	if err != nil {
		return nil, err
	}

	return executor.Exec(ctx, any, s.db, s.tableName, options...)
}

func (s *Service) ensureDialect(ctx context.Context) (*info.Dialect, error) {
	if s.dialect != nil {
		return s.dialect, nil
	}
	dialect, err := config.Dialect(ctx, s.db)
	s.dialect = dialect
	return dialect, err
}
