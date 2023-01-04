package update

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/option"
	"reflect"
)

type session struct {
	*io.Transaction
	presenceProvider *option.PresenceProvider
	rType            reflect.Type
	*config.Config
	binder        io.PlaceholderBinder
	columns       io.Columns
	identityIndex int
	db            *sql.DB
	stmt          *sql.Stmt
}

func (s *session) init(record interface{}, options ...option.Option) (err error) {
	for _, anOption := range options {
		switch actual := anOption.(type) {
		case *option.PresenceProvider:
			s.presenceProvider = actual
		}
	}

	if s.presenceProvider == nil {
		s.presenceProvider = &option.PresenceProvider{}
		options = append(options, s.presenceProvider)
	}

	if s.columns, s.binder, err = s.Mapper(record, s.TagName, options...); err != nil {
		return err
	}

	if identityIndex := s.columns.PrimaryKeys(); identityIndex != -1 {
		s.identityIndex = identityIndex
	}

	s.Builder, err = NewBuilder(s.TableName, s.columns.Names(), s.identityIndex, s.Dialect)
	return err
}

func (s *session) begin(ctx context.Context, db *sql.DB, options []option.Option) error {
	var err error
	s.Transaction, err = io.TransactionFor(ctx, s.Dialect, db, options)
	if err != nil {
		return err
	}

	return nil
}

func (s *session) prepare(ctx context.Context, record interface{}, dml *string) (bool, error) {
	SQL := s.Builder.Build(record, s.presenceProvider)
	if SQL == "" {
		return false, nil
	}
	if *dml == SQL {
		return true, nil
	}
	*dml = SQL
	var err error
	if s.stmt != nil {
		if err = s.stmt.Close(); err != nil {
			return false, fmt.Errorf("failed to close stetement: %w", err)
		}
	}
	if s.Transaction != nil {
		s.stmt, err = s.Transaction.Prepare(SQL)
		return err == nil, err
	}
	s.stmt, err = s.db.PrepareContext(ctx, SQL)
	return err == nil, err
}

func (s *session) update(ctx context.Context, record interface{}) (int64, error) {

	var placeholders = make([]interface{}, len(s.columns))
	s.binder(record, placeholders, 0, len(s.columns))

	placeholders = s.presenceProvider.Placeholders(record, placeholders)
	result, err := s.stmt.ExecContext(ctx, placeholders...)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return affected, nil
}

func (s *session) end(err error) error {
	if s.stmt != nil {
		if sErr := s.stmt.Close(); sErr != nil {
			err = fmt.Errorf("%w, %v", sErr, err)
		}
	}

	if s.Transaction == nil {
		return nil
	}

	if err != nil {
		return s.Transaction.RollbackWithErr(err)
	}

	return s.Transaction.Commit()
}
