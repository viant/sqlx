package delete

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
	rType     reflect.Type
	batchSize int
	*config.Config
	binder        io.PlaceholderBinder
	columns       io.Columns
	transactional bool
	db            *sql.DB
	stmt          *sql.Stmt
}

func (s *session) init(record interface{}) (err error) {
	if len(s.Config.Columns) > 0 {
		s.columns = s.Config.Columns
		return nil
	}
	if s.columns, s.binder, err = s.Mapper(record, s.TagName, option.IdentityOnly(true)); err != nil {
		return err
	}
	s.Builder, err = NewBuilder(s.TableName, s.columns.Names(), s.Dialect, s.batchSize)
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

func (s *session) prepare(ctx context.Context, batchSize int) error {
	SQL := s.Builder.Build(option.BatchSize(batchSize))
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

func (s *session) delete(ctx context.Context, record interface{}, recordsFn func() interface{}, batchSize int) (int64, error) {
	var recValues = make([]interface{}, batchSize*len(s.columns))
	totalRowsAffected := int64(0)
	inBatchCount := 0

	for ; record != nil; record = recordsFn() {
		offset := inBatchCount * len(s.columns)
		s.binder(record, recValues[offset:], 0, len(s.columns))
		inBatchCount++
		if inBatchCount == batchSize {
			rowsAffected, err := s.flush(ctx, recValues)
			if err != nil {
				return 0, err
			}
			totalRowsAffected += rowsAffected
			inBatchCount = 0
		}
	}

	if inBatchCount > 0 { //overflow
		err := s.prepare(ctx, inBatchCount)
		if err != nil {
			return 0, nil
		}
		rowsAffected, err := s.flush(ctx, recValues[0:inBatchCount*len(s.columns)])
		if err != nil {
			return 0, nil
		}
		totalRowsAffected += rowsAffected
	}
	return totalRowsAffected, nil
}

func (s *session) end(err error) error {
	if s.stmt != nil {
		if sErr := s.stmt.Close(); sErr != nil {
			err = fmt.Errorf("%w, %v", sErr, err)
		}
	}

	if s.Transaction == nil {
		return err
	}

	if err != nil {
		return s.Transaction.RollbackWithErr(err)
	}

	return s.Transaction.Commit()
}

func (s *session) flush(ctx context.Context, values []interface{}) (int64, error) {
	result, err := s.stmt.ExecContext(ctx, values...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}
