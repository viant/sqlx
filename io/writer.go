package io

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/opt"
	"reflect"
	"strings"
)

//Writer represents generic db writer
type Writer struct {
	db            *sql.DB
	dialect       *info.Dialect
	tableName     string
	tagName       string
	columnMapper  ColumnMapper
	columns       Columns
	insertBinder  PlaceholderBinder
	insertBuilder Builder
	insertBatch   *opt.BatchOption
	autoIncrement *int
}

func NewWriter(ctx context.Context, db *sql.DB, tableName string, options ...opt.Option) (*Writer, error) {
	var columnMapper ColumnMapper
	if !opt.Assign(options, &columnMapper) {
		columnMapper = genericColumnMapper
	}
	writer := &Writer{
		db:           db,
		dialect:      opt.Options(options).Dialect(),
		tableName:    tableName,
		insertBatch:  opt.Options(options).Batch(),
		tagName:      opt.Options(options).Tag(),
		columnMapper: columnMapper,
	}

	err := writer.init(ctx, db, options)
	if err != nil {
		return nil, err
	}
	return writer, nil
}

func (w *Writer) init(ctx context.Context, db *sql.DB, options opt.Options) error {
	if w.dialect == nil {
		product := options.Product()
		if product == nil {
			return fmt.Errorf("missing product option: %T", db)
		}
		w.dialect = registry.LookupDialect(product)
		if w.dialect == nil {
			return fmt.Errorf("failed to detect dialect for product: %v", product.Name)
		}
	}

	if w.insertBatch == nil {
		w.insertBatch = &opt.BatchOption{
			Size: 1,
		}
	}
	if w.dialect.Insert == dialect.InsertWithSingleValues {
		w.insertBatch.Size = 1
	}
	return nil
}

func (w *Writer) Insert(any interface{}, options ...opt.Option) (int64, int64, error) {
	recordsFn, err := anyProvider(any)
	if err != nil {
		return 0, 0, err
	}
	record := recordsFn()
	batch := opt.Options(options).Batch()
	if batch == nil {
		batch = w.insertBatch
	}
	if len(w.columns) == 0 {

		if w.columns, w.insertBinder, err = w.columnMapper(record, w.tagName); err != nil {
			return 0, 0, err
		}
		if autoIncrement := w.columns.Autoincrement(); autoIncrement != -1 {
			w.autoIncrement = &autoIncrement
			w.columns = w.columns[:autoIncrement]
		}

		var values = make([]string, len(w.columns))
		for i := range values {
			values[i] = w.dialect.Placeholder
		}
		if w.insertBuilder, err = NewInsert(w.tableName, batch.Size, w.columns.Names(), values); err != nil {
			return 0, 0, err
		}
	}

	stmt, err := w.prepareInsertStatement(batch.Size)
	if err != nil {
		return 0, 0, err
	}
	defer stmt.Close()
	var tx *sql.Tx
	if w.dialect.Transactional {
		tx, err = w.db.Begin()
		if err != nil {
			return 0, 0, err
		}
	}
	rowsAffected, lastInsertedId, err := w.insert(batch, record, recordsFn, stmt)
	if err != nil {
		if rErr := tx.Rollback(); rErr != nil {
			return 0, 0, fmt.Errorf("failed to rollback: %w, %v", err, rErr)
		}
		return 0, 0, err
	}

	if w.dialect.Transactional {
		err = tx.Commit()
	}
	return rowsAffected, lastInsertedId, err
}

func (w *Writer) insert(batch *opt.BatchOption, record interface{}, recordsFn func() interface{}, stmt *sql.Stmt) (int64, int64, error) {
	var recValues = make([]interface{}, batch.Size*len(w.columns))
	var identities = make([]interface{}, batch.Size)
	inBatchCount := 0
	identityIndex := 0
	var err error
	var rowsAffected, totalRowsAffected, lastInsertedId int64
	//ToDo: get real lastInsertedId

	for ; record != nil; record = recordsFn() {
		offset := inBatchCount * len(w.columns)
		w.insertBinder(record, recValues[offset:], 0, len(w.columns))
		if w.autoIncrement != nil {
			if autoIncrement := w.autoIncrement; autoIncrement != nil {
				w.insertBinder(record, identities[identityIndex:], *w.autoIncrement, 1)
				identityIndex++
			}
		}
		inBatchCount++
		if inBatchCount == batch.Size {
			rowsAffected, lastInsertedId, err = flush(stmt, recValues, lastInsertedId, identities[:identityIndex])
			if err != nil {
				return 0, 0, err
			}
			totalRowsAffected += rowsAffected
			inBatchCount = 0
			identityIndex = 0
		}
	}

	if inBatchCount > 0 { //overflow
		stmt, err = w.prepareInsertStatement(inBatchCount)
		if err != nil {
			return 0, 0, nil
		}
		defer stmt.Close()
		rowsAffected, lastInsertedId, err = flush(stmt, recValues[0:inBatchCount*len(w.columns)], lastInsertedId, identities[:identityIndex])
		if err != nil {
			return 0, 0, nil
		}
		totalRowsAffected += rowsAffected
	}
	return totalRowsAffected, lastInsertedId, err
}

func anyProvider(any interface{}) (func() interface{}, error) {
	switch actual := any.(type) {
	case []interface{}:
		i := 0
		return func() interface{} {
			if i >= len(actual) {
				return nil
			}
			result := actual[i]
			i++
			return result
		}, nil
	case func() interface{}:
		return actual, nil
	default:
		anyValue := reflect.ValueOf(any)
		switch anyValue.Kind() {
		case reflect.Ptr, reflect.Struct:
			val := actual
			return func() interface{} {
				result := val
				val = nil
				return result
			}, nil

		case reflect.Slice:
			anyLength := anyValue.Len()
			i := 0
			return func() interface{} {
				if i >= anyLength {
					return nil
				}
				resultValue := anyValue.Index(i)
				if resultValue.Kind() != reflect.Ptr {
					resultValue = resultValue.Addr()
				}
				result := resultValue.Interface()
				i++
				return result
			}, nil
		}

	}
	return nil, fmt.Errorf("usnupported :%T", any)
}

func flush(stmt *sql.Stmt, values []interface{}, prevInsertedID int64, identities []interface{}) (int64, int64, error) {
	result, err := stmt.Exec(values...)
	if err != nil {
		return 0, 0, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	newLastInsertedID, err := result.LastInsertId()
	if err != nil {
		return 0, 0, err
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

func (w *Writer) prepareInsertStatement(batchSize int) (*sql.Stmt, error) {
	SQL := w.insertBuilder.Build(batchSize)
	return w.db.Prepare(SQL)
}

func buildPlaceholders(batchSize, nCols int, placeholder string) string {
	placeholders := make([]string, nCols)
	for i := range placeholders {
		placeholders[i] = placeholder
	}
	strPlaceholders := "(" + strings.Join(placeholders, ",") + ")"

	batchPlaceholders := make([]string, batchSize)
	for i := range batchPlaceholders {
		batchPlaceholders[i] = strPlaceholders
	}

	return strings.Join(batchPlaceholders, ",")

}
