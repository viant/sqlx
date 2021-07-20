package io

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/opt"
	"github.com/viant/sqlx/xunsafe"
	"reflect"
	"strings"
)

//Writer represents generic db writer
type Writer struct {
	db            *sql.DB
	dialect       *info.Dialect
	tableName     string
	columnMapper  ColumnMapper
	columns       Columns
	binder        PlaceholderBinder
	autoIncrement *int
	tagName       string
	batch         *opt.BatchOption
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
		batch:        opt.Options(options).Batch(),
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

	if w.batch == nil {
		w.batch = &opt.BatchOption{
			Size: 1,
		}
	}
	if w.dialect.Insert == dialect.InsertWithSingleValues {
		w.batch.Size = 1
	}
	return nil
}

func (w *Writer) Insert(any interface{}, options ...opt.Option) (int64, int64, error) {
	recordsFn, err := anyProvider(any)
	if err != nil {
		return 0, 0, err
	}
	record := recordsFn()
	if len(w.columns) == 0 {
		if w.columns, w.binder, err = w.columnMapper(record, w.tagName); err != nil {
			return 0, 0, err
		}
		if autoIncrement := w.columns.Autoincrement();autoIncrement !=-1 {
			w.autoIncrement = &autoIncrement
		}
	}
	batch := opt.Options(options).Batch()
	if batch == nil {
		batch = w.batch
	}
	stmt, err := w.buildInsertStmt(batch.Size)
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
	recValues := make([]interface{}, batch.Size*len(w.columns))

	var identities []interface{}
	batchLen := 0
	var err error
	var rowsAffected, totalRowsAffected, lastInsertedId int64
	//ToDo: get real lastInsertedId

	for ; record != nil; record = recordsFn() {
		offset := batchLen * len(w.columns)
		w.binder(record, recValues, offset)

		if w.autoIncrement != nil  {
			identities = append(identities, recValues[offset+*w.autoIncrement])
		}
		batchLen++
		if batchLen == batch.Size {
			rowsAffected, lastInsertedId, err = flush(stmt, recValues, lastInsertedId, identities)
			if err != nil {
				return 0, 0, err
			}
			totalRowsAffected += rowsAffected
			batchLen = 0
			identities = nil
		}
	}

	if batchLen > 0 { //overflow
		stmt, err = w.buildInsertStmt(batchLen)
		if err != nil {
			return 0, 0, nil
		}
		rowsAffected, lastInsertedId, err = flush(stmt, recValues[0:batchLen*len(w.columns)], lastInsertedId, identities)
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
		lastInsertedID = newLastInsertedID - (int64(len(identities)) + 1)
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

func recordValues(holderPtr uintptr, target *[]interface{}, offset int, pointers []xunsafe.Pointer) error {
	for i, ptr := range pointers {
		(*target)[offset+i] = ptr(holderPtr)
	}
	return nil
}

func holderPointer(record interface{}) uintptr {
	value := reflect.ValueOf(record)
	if value.Kind() != reflect.Ptr { //convert to a pointer
		vp := reflect.New(value.Type())
		vp.Elem().Set(value)
		value = vp
	}
	holderPtr := value.Elem().UnsafeAddr()
	return holderPtr
}

func autoincValue(record interface{}, ptr xunsafe.Pointer) (interface{}, error) {
	value := reflect.ValueOf(record)
	if value.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("record with autoinc should be a pointer not a %v", value.Kind())
	}
	holderPtr := value.Elem().UnsafeAddr()
	return ptr(holderPtr), nil
}

func (w *Writer) buildInsertStmt(batchSize int) (*sql.Stmt, error) {
	//TODO optimize it
	var colNames []string
	for _, column := range w.columns {
		colNames = append(colNames, column.Name())
	}
	sqlTemplate := "INSERT INTO %s(%s) VALUES%s"
	insertSQL := fmt.Sprintf(sqlTemplate, w.tableName, strings.Join(colNames, ","), buildPlaceholders(batchSize, len(w.columns), w.dialect.Placeholder))
	return w.db.Prepare(insertSQL)
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
