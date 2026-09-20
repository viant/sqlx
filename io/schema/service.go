package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/viant/sqlx/metadata/info"
)

// Service provisions on the caller's native database. It does not own its
// lifetime, cache schema results, or begin a transaction (DDL may auto-commit).
type Service struct {
	DB      *sql.DB
	Dialect *info.Dialect
}

// EnsureTable preserves an accessible existing table. Failed probes are followed
// by CREATE IF NOT EXISTS and another probe, never ALTER or DROP. A concurrent
// creator may win even when the vendor reports a catalog conflict on CREATE.
func (s *Service) EnsureTable(ctx context.Context, table Table) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.DB == nil {
		return fmt.Errorf("schema database is required")
	}
	name, err := s.Dialect.TableIdentifier(table.Name, table.Dataset)
	if err != nil {
		return err
	}
	probeErr := s.probe(ctx, name)
	if probeErr == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	statement, err := table.CreateSQL(s.Dialect)
	if err != nil {
		return fmt.Errorf("provision table %s: %w", name, errors.Join(probeErr, err))
	}
	_, createErr := s.DB.ExecContext(ctx, statement)
	if err := ctx.Err(); err != nil {
		return err
	}
	verifyErr := s.probe(ctx, name)
	if verifyErr == nil {
		return nil
	}
	return fmt.Errorf("ensure table %s: %w", name, errors.Join(probeErr, createErr, verifyErr))
}

func (s *Service) probe(ctx context.Context, name string) error {
	rows, err := s.DB.QueryContext(ctx, "SELECT 1 FROM "+name+" WHERE 1 = 0")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return rows.Close()
}
