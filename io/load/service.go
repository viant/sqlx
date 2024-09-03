package load

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
)

// Service represents service used to
type Service struct {
	dialect   *info.Dialect
	tableName string
	columns   []sink.Column
	db        *sql.DB
}

// New creates instance of Service
func New(ctx context.Context, db *sql.DB, tableName string) (*Service, error) {
	dialect, err := config.Dialect(ctx, db)
	if err != nil {
		return nil, err
	}

	return &Service{
		tableName: tableName,
		db:        db,
		dialect:   dialect,
	}, nil

}

// Exec executes load statement specific for database
func (s *Service) Exec(ctx context.Context, any interface{}, options ...loption.Option) (int, error) {
	dialect, err := s.ensureDialect(ctx)
	if err != nil {
		return 0, err
	}
	session := config.LoadSession(dialect)
	if session == nil {
		return 0, fmt.Errorf("failed to lookup load session for dialect %v", dialect.Name)
	}

	exec, err := session.Exec(ctx, any, s.db, s.tableName, options...)
	if err != nil {
		return 0, err
	}

	affected, err := exec.RowsAffected()
	return int(affected), err
}

func (s *Service) ensureDialect(ctx context.Context) (*info.Dialect, error) {
	if s.dialect != nil {
		return s.dialect, nil
	}
	dialect, err := config.Dialect(ctx, s.db)
	s.dialect = dialect
	return dialect, err
}
