package writer

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/opt"
	"github.com/viant/sqlx/utils"
	"github.com/viant/sqlx/xunsafe"
	"reflect"
	"strings"
)

//Writer represents generic db writer
type Writer struct {
	db             *sql.DB
	dialect        *info.Dialect
	tableName      string
	recordType     reflect.Type
	columnNames    []string
	fieldPositions []int
	tagName        string
	batch          *opt.BatchOption
}

func NewWriter(ctx context.Context, db *sql.DB, tableName string, options ...opt.Option) (*Writer, error) {
	writer := &Writer{
		db:        db,
		dialect:   opt.Options(options).Dialect(),
		tableName: tableName,
		batch:     opt.Options(options).Batch(),
		tagName:   opt.Options(options).Tag(),
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

func (w *Writer) Write(any interface{}, options ...opt.Option) (int64, int64, error) {
	recordsFn := anyProvider(any)
	record := recordsFn()
	if w.recordType == nil {
		err := w.getRecTypeAndCols(record)
		if err != nil {
			return 0, 0, err
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
	rowsAffected, lastInsertedId, err := w.write(batch, record, recordsFn, stmt)
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

func (w *Writer) write(batch *opt.BatchOption, record interface{}, recordsFn func() interface{}, stmt *sql.Stmt) (int64, int64, error) {
	recValues := make([]interface{}, batch.Size*len(w.columnNames))
	batchLen := 0
	var err error
	var rowsAffected, totalRowsAffected, lastInsertedId int64
	for ; record != nil; record = recordsFn() {
		values, err := w.recordValues(record)
		if err != nil {
			return 0, 0, err
		}
		for j := range values {
			recValues[batchLen*len(w.columnNames)+j] = values[j]
		}
		batchLen++
		if batchLen == batch.Size {
			rowsAffected, lastInsertedId, err = flush(stmt, recValues)
			if err != nil {
				return 0, 0, nil
			}
			totalRowsAffected += rowsAffected
			batchLen = 0
		}
	}

	if batchLen > 0 { //overflow
		stmt, err = w.buildInsertStmt(batchLen)
		if err != nil {
			return 0, 0, nil
		}
		rowsAffected, lastInsertedId, err = flush(stmt, recValues[0:batchLen*len(w.columnNames)])
		if err != nil {
			return 0, 0, nil
		}
		totalRowsAffected += rowsAffected
	}

	return totalRowsAffected, lastInsertedId, err
}

func (w *Writer) getRecTypeAndCols(record interface{}) error {
	w.recordType = reflect.TypeOf(record)
	if w.recordType.Kind() == reflect.Ptr {
		w.recordType = w.recordType.Elem()
	}
	if w.recordType.Kind() != reflect.Struct {
		return fmt.Errorf("invalid record type: %v", w.recordType.Kind())
	}
	w.columnNames = []string{}
	w.fieldPositions = []int{}
	for i := 0; i < w.recordType.NumField(); i++ {
		if isExported := w.recordType.Field(i).PkgPath == ""; !isExported {
			continue
		}
		fieldName := w.recordType.Field(i).Name
		tagName := w.tagName
		tag := utils.ParseTag(w.recordType.Field(i).Tag.Get(tagName))
		isTransient := tag.FieldName == "-"
		if isTransient {
			continue
		}
		if tag.Autoincrement {
			w.batch.Size = 1
			continue
		}
		if tag.FieldName != "" {
			fieldName = tag.FieldName
		}
		w.columnNames = append(w.columnNames, fieldName)
		w.fieldPositions = append(w.fieldPositions, i)
	}
	return nil
}

func anyProvider(any interface{}) func() interface{} {
	switch actual := any.(type) {
	case []interface{}:
		i := 0
		return func() interface{} {
			if i >= len(actual) {
				return nil
			}
			i++
			return actual[i-1]
		}
	case interface{}:
		done := false
		return func() interface{} {
			if done {
				return nil
			}
			done = true
			return actual
		}
	}
	return func() interface{} { return nil }
}

func flush(stmt *sql.Stmt, values []interface{}) (int64, int64, error) {
	result, err := stmt.Exec(values...)
	if err != nil {
		return 0, 0, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	lastInsertedId, err := result.LastInsertId()
	if err != nil {
		return 0, 0, err
	}
	return rowsAffected, lastInsertedId, err
}

func (w *Writer) recordValues(record interface{}) ([]interface{}, error) {
	values := make([]interface{}, len(w.columnNames))
	var pointers = make([]xunsafe.Pointer, len(w.columnNames))
	var err error

	for i, position := range w.fieldPositions {
		pointers[i], err = xunsafe.FieldPointer(w.recordType, position)
		if err != nil {
			return nil, err
		}
	}
	value := reflect.ValueOf(record)
	if value.Kind() != reflect.Ptr { //convert to a pointer
		vp := reflect.New(value.Type())
		vp.Elem().Set(value)
		value = vp
	}
	holderPtr := value.Elem().UnsafeAddr()
	for i, ptr := range pointers {
		values[i] = ptr(holderPtr)
	}
	return values, nil
}

func (w *Writer) buildInsertStmt(batchSize int) (*sql.Stmt, error) {
	colNames := strings.Join(w.columnNames, ",")
	sqlTemplate := "INSERT INTO %s(%s) VALUES%s"
	insertSQL := fmt.Sprintf(sqlTemplate, w.tableName, colNames, buildPlaceholders(batchSize, len(w.columnNames), w.dialect.Placeholder))
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
