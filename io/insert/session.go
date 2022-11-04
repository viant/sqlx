package insert

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"reflect"
	"strings"
)

type session struct {
	*io.Transaction
	rType     reflect.Type
	batchSize int
	*config.Config
	binder                io.PlaceholderBinder
	columns               io.Columns
	identityColumn        io.Column
	identityColumnPos     *int
	db                    *sql.DB
	stmt                  *sql.Stmt
	shallPresetIdentities bool
	incrementBy           int
}

func (s *session) init(record interface{}) (err error) {
	if s.columns, s.binder, err = s.Mapper(record, s.TagName); err != nil {
		return err
	}

	var identityColumnPos = s.columns.IdentityColumnPos()

	if identityColumnPos != -1 {
		s.identityColumnPos = &identityColumnPos
		s.identityColumn = s.columns[identityColumnPos]
		s.Identity = s.identityColumn.Name()
		s.shallPresetIdentities = s.identityColumn.Tag().Autoincrement || s.identityColumn.Tag().Sequence != ""
	}

	s.Builder, err = NewBuilder(s.TableName, s.columns.Names(), s.Dialect, s.Identity, s.batchSize)
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

func (s *session) end(err error) error {
	if s.stmt != nil {
		if sErr := s.stmt.Close(); sErr != nil {
			if !isClosedError(err) {
				err = fmt.Errorf("%w, %v", sErr, err)
			}
		}
		s.stmt = nil
	}

	if s.Transaction == nil {
		return err
	}
	if err != nil {
		return s.RollbackWithErr(err)
	}
	if s.Transaction.Global {
		return nil
	}
	return s.Transaction.Commit()
}

func (s *session) prepare(ctx context.Context, batchSize int) error {
	SQL := s.Builder.Build(option.BatchSize(batchSize))
	SQL = s.Dialect.EnsurePlaceholders(SQL)

	var err error
	if s.stmt != nil {
		if err = s.stmt.Close(); err != nil {
			if !isClosedError(err) {
				return err
			}
		}
		s.stmt = nil
	}
	if s.Transaction != nil {
		s.stmt, err = s.Transaction.Prepare(SQL)
		return err
	}
	s.stmt, err = s.db.PrepareContext(ctx, SQL)
	return err
}

func isClosedError(err error) bool {
	return strings.Contains(err.Error(), "closed")
}

func (s *session) insert(ctx context.Context, recValues []interface{}, valueAt io.ValueAccessor, size int, minSeqNextValue int64, sequence *sink.Sequence, presetIdentities bool, identitiesBatched []interface{}) (int64, int64, error) {
	inBatchCount := 0
	var err error
	var rowsAffected, totalRowsAffected, lastInsertedID int64
	var newId = minSeqNextValue

	for i := 0; i < size; i++ {
		record := valueAt(i)
		offset := inBatchCount * len(s.columns)
		s.binder(record, recValues[offset:], 0, len(s.columns))
		if s.identityColumnPos != nil {
			idIndex := offset + *s.identityColumnPos
			identitiesBatched[inBatchCount] = recValues[idIndex]
			idPtr, err := io.Int64Ptr(identitiesBatched, inBatchCount)
			if presetIdentities {
				if err != nil {
					return 0, 0, err
				}
				*idPtr = newId
				newId += sequence.IncrementBy
			}
			if *idPtr == 0 {
				recValues[idIndex] = nil
			}
		}

		inBatchCount++

		if inBatchCount == s.batchSize {
			rowsAffected, lastInsertedID, err = s.flush(ctx, recValues, identitiesBatched)
			if err != nil {
				return 0, 0, err
			}
			totalRowsAffected += rowsAffected
			inBatchCount = 0
		}
	}

	if inBatchCount > 0 {
		err = s.prepare(ctx, inBatchCount)
		if err != nil {
			return 0, 0, nil
		}
		rowsAffected, lastInsertedID, err = s.flush(ctx, recValues[0:inBatchCount*len(s.columns)], identitiesBatched)
		if err != nil {
			return 0, 0, nil
		}
		totalRowsAffected += rowsAffected
	}
	return totalRowsAffected, lastInsertedID, err
}

func (s *session) flush(ctx context.Context, values []interface{}, identities []interface{}) (int64, int64, error) {

	if s.Dialect.CanReturning {
		return s.flushQuery(ctx, values, identities)
	}

	result, err := s.stmt.ExecContext(ctx, values...)
	if err != nil {
		return 0, 0, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, 0, err
	}

	lastInsertedID, _ := s.lastInsertedIdentity(values, result)
	if !s.shallPresetIdentities && lastInsertedID > 0 {
		id := lastInsertedID
		for i := rowsAffected - 1; i >= 0; i-- { //this would only work for single thread running insert
			// since driver lastInsertedID does guarantee continuity in generated IDs
			intPtr, _ := io.Int64Ptr(identities, int(i)) //we can only set next id back for single records, batch insert is not reliable
			if *intPtr > 0 {
				break
			}
			*intPtr = id
			id -= int64(s.incrementBy)
		}
	}
	return rowsAffected, lastInsertedID, nil
}

func (s *session) flushQuery(ctx context.Context, values []interface{}, identities []interface{}) (int64, int64, error) {
	var rowsAffected, newLastInsertedID int64
	rows, err := s.stmt.QueryContext(ctx, values...)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	rows.NextResultSet()
	newLastInsertedID = 0

	for rows.Next() {
		if err = rows.Scan(&newLastInsertedID); err != nil {
			return 0, 0, err
		}
		idPtr, err := io.Int64Ptr(identities, int(rowsAffected))
		if err != nil {
			return 0, 0, err
		}
		*idPtr = newLastInsertedID
		rowsAffected++
	}
	return rowsAffected, newLastInsertedID, err
}

func (s *session) lastInsertedIdentity(values []interface{}, result sql.Result) (int64, error) {
	if s.identityColumnPos != nil && *s.identityColumnPos != -1 && !s.Dialect.CanLastInsertID {
		value, err := io.Int64Ptr(values, len(values)-1)
		if err != nil {
			return 0, err
		}
		return *value, nil
	}
	return result.LastInsertId()

}
