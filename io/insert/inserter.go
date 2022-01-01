package insert

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/insert/generators"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/option"
)

//Inserter represents generic db writer
type Inserter struct {
	db                  *sql.DB
	dialect             *info.Dialect
	tableName           string
	tagName             string
	mapper              io.ColumnMapper
	columns             io.Columns
	binder              io.PlaceholderBinder
	builder             Builder
	batch               *option.Batch
	autoIncrementColumn io.Column
	autoIncrement       *int
}

//New creates an inserter
func New(ctx context.Context, db *sql.DB, tableName string, options ...option.Option) (*Inserter, error) {
	var columnMapper io.ColumnMapper
	if !option.Assign(options, &columnMapper) {
		columnMapper = io.GenericColumnMapper
	}
	writer := &Inserter{
		db:        db,
		dialect:   option.Options(options).Dialect(),
		tableName: tableName,
		batch:     option.Options(options).Batch(),
		tagName:   option.Options(options).Tag(),
		mapper:    columnMapper,
	}

	err := writer.init(ctx, db, options)
	if err != nil {
		return nil, err
	}
	return writer, nil
}

func (w *Inserter) init(ctx context.Context, db *sql.DB, options option.Options) error {
	err := w.initDialect(ctx, db, options)
	if err != nil {
		return err
	}

	if w.batch == nil {
		w.batch = &option.Batch{
			Size: 1,
		}
	}
	if w.dialect.Insert == dialect.InsertWithSingleValues {
		w.batch.Size = 1
	}
	return nil
}

func (w *Inserter) initDialect(ctx context.Context, db *sql.DB, options option.Options) error {
	if w.dialect == nil {
		product := options.Product()
		if product == nil {
			var err error
			meta := metadata.New()
			product, err = meta.DetectProduct(ctx, db)
			if err != nil {
				return fmt.Errorf("missing product option: %T", db)
			}
		}
		w.dialect = registry.LookupDialect(product)
		if w.dialect == nil {
			return fmt.Errorf("failed to detect dialect for product: %v", product.Name)
		}
	}
	return nil
}

//Insert runs INSERT statement for supplied data
func (w *Inserter) Insert(ctx context.Context, any interface{}, options ...option.Option) (int64, int64, error) {
	recordsFn, err := io.AnyProvider(any)
	if err != nil {
		return 0, 0, err
	}
	record := recordsFn()
	batch := option.Options(options).Batch()
	if batch == nil {
		batch = w.batch
	}
	if len(w.columns) == 0 {
		if w.columns, w.binder, err = w.mapper(record, w.tagName); err != nil {
			return 0, 0, err
		}
		if autoIncrement := w.columns.Autoincrement(); autoIncrement != -1 {
			w.autoIncrement = &autoIncrement
			w.autoIncrementColumn = w.columns[autoIncrement]
			w.columns = w.columns[:autoIncrement]
		}
		var values = make([]string, len(w.columns))
		placeholderGetter := w.dialect.PlaceholderGetter()
		for i := range values {
			values[i] = placeholderGetter()
		}
		if w.builder, err = NewInsert(w.tableName, batch.Size, w.columns.Names(), values); err != nil {
			return 0, 0, err
		}
	}
	var tx *sql.Tx
	transactional := w.dialect.Transactional
	if option.Assign(options, &tx) { //transaction supply as option, do not manage locally transaction
		transactional = false
	}
	if transactional {
		tx, err = w.db.BeginTx(ctx, nil)
		if err != nil {
			return 0, 0, err
		}
	}

	err = generators.NewDefault(w.dialect, w.db, nil).Apply(ctx, any, w.tableName)
	if err != nil {
		if transactional {
			tx.Rollback()
		}
		return 0, 0, err
	}

	stmt, err := w.prepareInsertStatement(ctx, batch.Size, tx)
	if err != nil {
		return 0, 0, err
	}
	defer stmt.Close()
	rowsAffected, lastInsertedId, err := w.insert(ctx, batch, record, recordsFn, stmt, tx)
	if err != nil {
		if transactional {
			if rErr := tx.Rollback(); rErr != nil {
				return 0, 0, fmt.Errorf("failed to rollback: %w, %v", err, rErr)
			}
		}
		return 0, 0, err
	}
	if transactional {
		err = tx.Commit()
	}
	return rowsAffected, lastInsertedId, err
}

func (w *Inserter) insert(ctx context.Context, batch *option.Batch, record interface{}, recordsFn func() interface{}, stmt *sql.Stmt, tx *sql.Tx) (int64, int64, error) {
	var recValues = make([]interface{}, batch.Size*len(w.columns))
	var identities = make([]interface{}, batch.Size)
	inBatchCount := 0
	identityIndex := 0
	var err error
	var rowsAffected, totalRowsAffected, lastInsertedId int64
	//ToDo: get real lastInsertedId
	hasAutoIncrement := w.autoIncrement != nil
	for ; record != nil; record = recordsFn() {
		offset := inBatchCount * len(w.columns)
		w.binder(record, recValues[offset:], 0, len(w.columns))
		if w.autoIncrement != nil {
			if autoIncrement := w.autoIncrement; autoIncrement != nil {
				w.binder(record, identities[identityIndex:], *w.autoIncrement, 1)
				identityIndex++
			}
		}
		inBatchCount++
		if inBatchCount == batch.Size {
			rowsAffected, lastInsertedId, err = flush(ctx, stmt, recValues, lastInsertedId, identities[:identityIndex], hasAutoIncrement, w.dialect.CanLastInsertId)
			if err != nil {
				return 0, 0, err
			}
			totalRowsAffected += rowsAffected
			inBatchCount = 0
			identityIndex = 0
		}
	}

	if inBatchCount > 0 { //overflow
		stmt, err = w.prepareInsertStatement(ctx, inBatchCount, tx)
		if err != nil {
			return 0, 0, nil
		}
		defer stmt.Close()
		rowsAffected, lastInsertedId, err = flush(ctx, stmt, recValues[0:inBatchCount*len(w.columns)], lastInsertedId, identities[:identityIndex], hasAutoIncrement, w.dialect.CanLastInsertId)
		if err != nil {
			return 0, 0, nil
		}
		totalRowsAffected += rowsAffected
	}
	return totalRowsAffected, lastInsertedId, err
}

func (w *Inserter) prepareInsertStatement(ctx context.Context, batchSize int, tx *sql.Tx) (*sql.Stmt, error) {
	var options = []interface{}{
		batchSize, w.dialect.Insert,
	}

	if w.dialect != nil {
		options = append(options, w.dialect)
	}
	if w.autoIncrementColumn != nil {
		options = append(options, option.Identity(w.autoIncrementColumn.Name()))
	}
	SQL := w.builder.Build(options...)
	if tx != nil {
		return tx.Prepare(SQL)
	}
	return w.db.PrepareContext(ctx, SQL)
}

func flush(ctx context.Context, stmt *sql.Stmt, values []interface{}, prevInsertedID int64, identities []interface{}, hasAutoIncrement, canUseLastInsertedID bool) (int64, int64, error) {
	var rowsAffected, newLastInsertedID int64
	if hasAutoIncrement && !canUseLastInsertedID {
		rows, err := stmt.QueryContext(ctx, values...)
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

	result, err := stmt.ExecContext(ctx, values...)
	if err != nil {
		return 0, 0, err
	}
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	if hasAutoIncrement && canUseLastInsertedID {
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
