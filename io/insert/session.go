package insert

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info/dialect"
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
	binder         io.PlaceholderBinder
	columns        io.Columns
	db             *sql.DB
	stmt           *sql.Stmt
	recordUpdaters []recordUpdater
}

func (s *session) init(record interface{}) (err error) {
	if s.columns, s.binder, err = s.Mapper(record); err != nil {
		return err
	}
	for i, column := range s.columns {
		if io.IsIdentityColumn(column) {
			updater, ok := newRecordUpdater(s, column, i)
			if ok {
				s.recordUpdaters = append(s.recordUpdaters, updater)
			}
			if s.Identity == "" {
				s.Identity = column.Name()
			}
		}
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

func (s *session) prepare(ctx context.Context, record interface{}, batchSize int) error {
	if len(s.OnDuplicateKeySql) > 0 && s.Dialect.Upsert != dialect.UpsertTypeInsertOrUpdate {
		return fmt.Errorf("upsert by insert with onduplicatekeysql option is supported for dialect with upsert feature: %v (current: %v)", dialect.UpsertTypeInsertOrUpdate, s.Dialect.Upsert)
	}
	SQL := s.Builder.Build(record, option.BatchSize(batchSize), option.OnDuplicateKeySql(s.OnDuplicateKeySql))
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
	if showSQL {
		fmt.Println(SQL)
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

func (s *session) insert(ctx context.Context, recValues []interface{}, valueAt io.ValueAccessor, size int, identitiesBatched []interface{}) (int64, int64, error) {
	inBatchCount := 0
	var err error
	var rowsAffected, totalRowsAffected, lastInsertedID int64
	var record interface{}

	for i := 0; i < size; i++ {
		record = valueAt(i)
		offset := inBatchCount * len(s.columns)
		if insertable, ok := record.(Insertable); ok {
			if err := insertable.OnInsert(ctx); err != nil {
				return 0, 0, err
			}
		}

		s.binder(record, recValues[offset:], 0, len(s.columns))
		for _, updater := range s.recordUpdaters {
			idIndex := offset + updater.columnPosition()
			identitiesBatched[inBatchCount] = recValues[idIndex]
			if err = updater.updateRecord(ctx, s, record, &recValues[idIndex], size, recValues[offset:idIndex+1], nil); err != nil {
				return 0, 0, err
			}
		}

		inBatchCount++
		if inBatchCount >= s.batchSize {
			rowsAffected, lastInsertedID, err = s.flush(ctx, recValues, identitiesBatched)
			if err != nil {
				return 0, 0, err
			}
			totalRowsAffected += rowsAffected
			inBatchCount = 0
		}
	}

	if inBatchCount > 0 {
		err = s.prepare(ctx, record, inBatchCount)
		if err != nil {
			return 0, 0, err
		}
		rowsAffected, lastInsertedID, err = s.flush(ctx, recValues[0:inBatchCount*len(s.columns)], identitiesBatched)
		if err != nil {
			return 0, 0, err
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

	var id int64
	if s.Dialect.CanLastInsertID {
		id, err = result.LastInsertId()
		if err != nil {
			return 0, 0, err
		}
	}
	if id > 0 {
		for _, updater := range s.recordUpdaters {
			lastInsertedID, err := updater.afterFlush(ctx, values, identities, rowsAffected, id)
			if err != nil {
				return 0, 0, err
			}

			if lastInsertedID > 0 {
				id = lastInsertedID
			}
		}
	}
	return rowsAffected, id, nil
}

func (s *session) flushQuery(ctx context.Context, values []interface{}, identities []interface{}) (int64, int64, error) {
	var rowsAffected, newLastInsertedID int64
	rows, err := s.stmt.QueryContext(ctx, values...)
	if err != nil {
		return 0, 0, err
	}
	defer io.RunWithError(rows.Close, &err)
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
