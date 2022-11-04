package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

type Udf struct{}

// all this handler requires more testing (especially with transactions)
func (n *Udf) Handle(ctx context.Context, db *sql.DB, target interface{}, iopts ...interface{}) (doNext bool, err error) {
	options := option.AsOptions(iopts)
	recordCount := options.RecordCount()
	if recordCount == 0 {
		return false, fmt.Errorf("invalid recordCount option, expected > 0, but had: %d", recordCount)
	}
	argsOps := options.Args()
	if argsOps == nil {
		return false, fmt.Errorf("argsOps was empty")
	}
	arguments, err := argsOps.StringN(3)
	if err != nil {
		return false, err
	}

	catalog, schema, sequenceName := arguments[0], arguments[1], arguments[2]

	tx := options.Tx()

	SQL := &sqlx.SQL{
		Query: `CALL SET_AUTO_INCREMENT_WITH_INNER_TX(?, ?, ?);`,
		Args:  []interface{}{schema, sequenceName, recordCount},
	}

	seq, ok := target.(*sink.Sequence)
	if !ok {
		return false, fmt.Errorf("expected %T, but had: %T", seq, target)
	}
	seq.Catalog = catalog
	seq.Schema = schema
	seq.Name = sequenceName
	//seq.DataType = "int" // TODO HARDCODED TYPE

	seq.Catalog, seq.Schema, seq.Name = arguments[0], arguments[1], arguments[2]

	// TODO ADD NEW TX
	// all this handler requires more testing (especially with transactions)
	var rows *sql.Rows
	if tx != nil {
		rows, err = tx.QueryContext(ctx, SQL.Query, SQL.Args...)
	} else {
		rows, err = db.QueryContext(ctx, SQL.Query, SQL.Args...)
	}

	defer func() {
		err2 := rows.Close()
		if err2 != nil && err == nil {
			err = err2
			doNext = false
		}
	}()

	if err != nil {
		return false, err
	}

	if rows.Next() {
		err = rows.Scan(&seq.Value, &seq.StartValue, &seq.IncrementBy)
	} else {
		err = fmt.Errorf("not records for %v", SQL.Query)
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func (n *Udf) CanUse(iopts ...interface{}) bool {
	options := option.AsOptions(iopts)
	return options.AutoincrementStrategy() == option.PresetIdWithUDFSequence
}

func runQuery(ctx context.Context, db *sql.DB, SQL string, trg []interface{}, tx *sql.Tx) (err error) {
	var rows *sql.Rows

	if tx != nil {
		rows, err = tx.QueryContext(ctx, SQL)
	} else {
		rows, err = db.QueryContext(ctx, SQL)
	}

	if err != nil {
		return err
	}

	defer func() {
		err2 := rows.Close()
		if err2 != nil && err == nil {
			err = err2
		}
	}()

	if rows.Next() {
		err = rows.Scan(trg...)
	} else {
		err = fmt.Errorf("not records for %v", SQL)
	}
	if err != nil {
		return err
	}

	err = rows.Err()

	return err
}
