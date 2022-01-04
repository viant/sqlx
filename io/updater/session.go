package updater

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
	rType reflect.Type
	*config.Config
	binder        io.PlaceholderBinder
	columns       io.Columns
	identityIndex int
	transactional bool
	db            *sql.DB
	tx            *sql.Tx
	stmt          *sql.Stmt
}

func (s *session) init(record interface{}) (err error) {
	if s.columns, s.binder, err = s.Mapper(record, s.TagName); err != nil {
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
	s.db = db
	s.transactional = s.Dialect.Transactional
	if option.Assign(options, s.tx) { //transaction supply as option, do not manage locally transaction
		s.transactional = false
	}
	if s.transactional {
		if s.tx, err = db.BeginTx(ctx, nil); err != nil {
			if rErr := s.tx.Rollback(); rErr != nil {
				return fmt.Errorf("%w, %v", err, rErr)
			}
		}
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
	if s.tx != nil {
		s.stmt, err = s.tx.Prepare(SQL)
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

func (w *session) end(err error) error {
	if w.stmt != nil {
		if sErr := w.stmt.Close(); sErr != nil {
			err = fmt.Errorf("%w, %v", sErr, err)
		}
	}
	if err != nil {
		if w.transactional {
			if rErr := w.tx.Rollback(); rErr != nil {
				return fmt.Errorf("failed to rollback: %w, %v", err, rErr)
			}
		}
		return err
	}
	if w.transactional {
		err = w.tx.Commit()
	}
	return err
}
