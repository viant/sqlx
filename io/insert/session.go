package insert

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"reflect"
	"strings"
)

type session struct {
	*io.Transaction
	info      *sink.Session
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
	inBatchCount          int
	sequence              sink.Sequence
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

func (s *session) insert(ctx context.Context, recValues []interface{}, valueAt io.ValueAccessor, size int, minSeqNextValue int64, identitiesBatched []interface{}) (int64, int64, error) {
	s.inBatchCount = 0
	var err error
	var rowsAffected, totalRowsAffected, lastInsertedID int64
	var newID = minSeqNextValue

	for i := 0; i < size; i++ {
		record := valueAt(i)
		offset := s.inBatchCount * len(s.columns)
		s.binder(record, recValues[offset:], 0, len(s.columns))
		if s.identityColumnPos != nil {
			idIndex := offset + *s.identityColumnPos
			identitiesBatched[s.inBatchCount] = recValues[idIndex]
			idPtr, _ := io.Int64Ptr(identitiesBatched, s.inBatchCount)
			if s.shallPresetIdentities {
				*idPtr = newID
				newID += s.sequence.IncrementBy
			}
			if *idPtr == 0 {
				recValues[idIndex] = nil
			}
		}

		s.inBatchCount++
		if s.inBatchCount == s.batchSize {
			rowsAffected, lastInsertedID, err = s.flush(ctx, recValues, identitiesBatched)
			if err != nil {
				return 0, 0, err
			}
			totalRowsAffected += rowsAffected
			s.inBatchCount = 0
		}
	}

	if s.inBatchCount > 0 {
		err = s.prepare(ctx, s.inBatchCount)
		if err != nil {
			return 0, 0, nil
		}
		rowsAffected, lastInsertedID, err = s.flush(ctx, recValues[0:s.inBatchCount*len(s.columns)], identitiesBatched)
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
	lastInsertedID, _ := s.lastInsertedIdentity(identities, result)
	if !s.shallPresetIdentities && lastInsertedID > 0 {
		s.updateInsertIDs(ctx, lastInsertedID, rowsAffected, identities)
	}
	return rowsAffected, lastInsertedID, nil
}

func (s *session) updateInsertIDs(ctx context.Context, lastInsertedID int64, rowsAffected int64, identities []interface{}) {
	if rowsAffected == 0 {
		return
	}
	if intPtr, _ := io.Int64Ptr(identities, 0); intPtr != nil && *intPtr > 0 {
		return
	}

	if s.sequence.Value == 0 { //no info about sequence
		for i := 0; i < int(rowsAffected); i++ {
			intPtr, _ := io.Int64Ptr(identities, int(i)) //we can only set next id back for single records, batch insert is not reliable
			if *intPtr > 0 {
				continue
			}
			*intPtr = lastInsertedID
			lastInsertedID += s.sequence.IncrementBy
		}
		return
	}

	if s.sequence.Value == lastInsertedID {
		incrementBy := s.sequence.IncrementBy
		s.updateSequence(ctx, s.sequence.Name)
		expectedNextInsertID := (1 + rowsAffected) * incrementBy
		if expectedNextInsertID != s.sequence.Value { //race condition during batch insert, skip updating IDs
			return
		}
		for i := 0; i < int(rowsAffected); i++ {
			intPtr, _ := io.Int64Ptr(identities, int(i)) //we can only set next id back for single records, batch insert is not reliable
			*intPtr = lastInsertedID
			lastInsertedID += s.sequence.IncrementBy
		}
		return
	}

}

func (s *session) flushQuery(ctx context.Context, values []interface{}, identities []interface{}) (int64, int64, error) {
	var rowsAffected, newLastInsertedID int64
	rows, err := s.stmt.QueryContext(ctx, values...)
	if err != nil {
		return 0, 0, err
	}
	defer io.MergeErrorIfNeeded(rows.Close, &err)
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
	idx := s.lastBatchPos(values)
	value, err := io.Int64Ptr(values, idx)
	if s.shallPresetIdentities {
		if err != nil {
			return 0, err
		}
		return *value, nil
	} else if s.identityColumn != nil {
		if value != nil && *value > 0 {
			return *value, nil
		}
	}
	return result.LastInsertId()
}

//updateSequence updates session sequence
func (s *session) updateSequence(ctx context.Context, sequenceName string) {
	meta := metadata.New()
	options := append([]option.Option{}, option.NewArgs(s.info.Catalog, s.info.Schema, sequenceName))
	_ = meta.Info(ctx, s.db, info.KindSequences, &s.sequence, options...)
}

func (s *session) lastBatchPos(values []interface{}) int {
	idx := len(values) - 1
	if s.inBatchCount != 0 {
		idx = s.inBatchCount - 1
	}
	return idx
}
