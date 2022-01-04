package insert

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
	rType     reflect.Type
	batchSize int
	*config.Config
	binder              io.PlaceholderBinder
	columns             io.Columns
	autoIncrementColumn io.Column
	autoIncrement       *int
	transactional       bool
	db                  *sql.DB
	tx                  *sql.Tx
	stmt                *sql.Stmt
}

func (s *session) init(record interface{}) (err error) {
	if s.columns, s.binder, err = s.Mapper(record, s.TagName); err != nil {
		return err
	}
	if autoIncrement := s.columns.Autoincrement(); autoIncrement != -1 {
		s.autoIncrement = &autoIncrement
		s.autoIncrementColumn = s.columns[autoIncrement]
		s.columns = s.columns[:autoIncrement]
	}
	s.Builder, err = NewBuilder(s.TableName, s.columns.Names(), s.Dialect, s.Identity, s.batchSize)
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

func (s *session) prepare(ctx context.Context, batchSize int) error {
	SQL := s.Builder.Build(batchSize)
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

func (s *session) insert(ctx context.Context, batchSize int, record interface{}, recordsFn func() interface{}) (int64, int64, error) {
	var recValues = make([]interface{}, batchSize*len(s.columns))
	var identities = make([]interface{}, batchSize)
	inBatchCount := 0
	identityIndex := 0
	var err error
	var rowsAffected, totalRowsAffected, lastInsertedID int64
	//ToDo: get real lastInsertedID
	hasAutoIncrement := s.autoIncrement != nil
	for ; record != nil; record = recordsFn() {
		offset := inBatchCount * len(s.columns)
		s.binder(record, recValues[offset:], 0, len(s.columns))
		if s.autoIncrement != nil {
			if autoIncrement := s.autoIncrement; autoIncrement != nil {
				s.binder(record, identities[identityIndex:], *s.autoIncrement, 1)
				identityIndex++
			}
		}
		inBatchCount++
		if inBatchCount == batchSize {
			rowsAffected, lastInsertedID, err = s.flush(ctx, recValues, lastInsertedID, identities[:identityIndex], hasAutoIncrement)
			if err != nil {
				return 0, 0, err
			}
			totalRowsAffected += rowsAffected
			inBatchCount = 0
			identityIndex = 0
		}
	}

	if inBatchCount > 0 { //overflow
		err = s.prepare(ctx, inBatchCount)
		if err != nil {
			return 0, 0, nil
		}
		rowsAffected, lastInsertedID, err = s.flush(ctx, recValues[0:inBatchCount*len(s.columns)], lastInsertedID, identities[:identityIndex], hasAutoIncrement)
		if err != nil {
			return 0, 0, nil
		}
		totalRowsAffected += rowsAffected
	}
	return totalRowsAffected, lastInsertedID, err
}

func (s *session) flush(ctx context.Context, values []interface{}, prevInsertedID int64, identities []interface{}, hasAutoIncrement bool) (int64, int64, error) {
	var rowsAffected, newLastInsertedID int64
	if hasAutoIncrement && !s.Dialect.CanLastInsertID {
		rows, err := s.stmt.QueryContext(ctx, values...)
		if err != nil {
			return 0, 0, err
		}
		defer rows.Close()
		for rows.Next() {
			if err = rows.Scan(identities[rowsAffected]); err != nil {
				return 0, 0, err
			}
			rowsAffected++
		}
		return rowsAffected, newLastInsertedID, err
	}

	result, err := s.stmt.ExecContext(ctx, values...)
	if err != nil {
		return 0, 0, err
	}
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	if hasAutoIncrement && s.Dialect.CanLastInsertID {
		newLastInsertedID, err = result.LastInsertId()
		if err != nil {
			return 0, 0, err
		}
	}
	lastInsertedID := prevInsertedID
	if lastInsertedID == 0 {
		lastInsertedID = newLastInsertedID - int64(len(identities))
	}

	if len(identities) > 0 { // update autoinc fields
		//ToDo: check: newLastInsertedID-prevInsertedID>len(values)
		for i, ID := range identities {
			switch val := ID.(type) {
			case *int64:
				*val = lastInsertedID + int64(i+1)
			case *int:
				*val = int(lastInsertedID + int64(i+1))
			default:
				return 0, 0, fmt.Errorf("expected *int or *int64 for autoinc, got %T", val)
			}
		}
	}
	return rowsAffected, newLastInsertedID, err
}
