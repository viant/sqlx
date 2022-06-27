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
	rType reflect.Type
	*config.Config
	binder        io.PlaceholderBinder
	columns       io.Columns
	identityIndex int
	db            *sql.DB
	stmt          *sql.Stmt
}

func (s *session) init(record interface{}, options ...option.Option) (err error) {
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

func (s *session) prepare(ctx context.Context) error {
	SQL := s.Builder.Build()
	var err error
	if s.stmt != nil {
		if err = s.stmt.Close(); err != nil {
			return fmt.Errorf("failed to close stetement: %w", err)
		}
	}
	if s.Transaction != nil {
		s.stmt, err = s.Transaction.Prepare(SQL)
		return err
	}
	s.stmt, err = s.db.PrepareContext(ctx, SQL)
	return err
}

func (s *session) update(ctx context.Context, record interface{}, recordsFn func() interface{}) (int64, error) {
	var recValues = make([]interface{}, len(s.columns))
	affectedRecords := int64(0)
	for ; record != nil; record = recordsFn() {
		s.binder(record, recValues, 0, len(s.columns))
		result, err := s.stmt.ExecContext(ctx, recValues...)
		if err != nil {
			return 0, err
		}
		affected, _ := result.RowsAffected()
		affectedRecords += affected
	}
	return affectedRecords, nil
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
