package merge

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/product/mysql/merge/metric"
	"github.com/viant/sqlx/moption"
	"time"
)

func (e *Executor) delete(ctx context.Context, db *sql.DB, data []interface{}, stmt string) (int, error) {
	cnt := 0
	var err error
	start := time.Now()
	defer func() {
		e.metric.Delete.Main.Time = time.Now().Sub(start)
		e.metric.Delete.Main.Affected = cnt
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("### DELETING TIME %s FOR %d OF %d RECORDS \n", e.metric.Delete.Main.Time, cnt, len(data)))
	}()

	if len(data) == 0 {
		return 0, nil
	}
	cnt, err = e.execSQL(ctx, db, stmt)

	return cnt, err
}

func (e *Executor) update(ctx context.Context, db *sql.DB, data []interface{}, stmt string) (int, error) {
	cnt := 0
	var err error
	start := time.Now()
	defer func() {
		e.metric.Update.Main.Time = time.Now().Sub(start)
		e.metric.Update.Main.Affected = cnt
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("### UPDATING TIME %s FOR %d OF %d RECORDS \n", e.metric.Update.Main.Time, cnt, len(data)))
	}()

	if len(data) == 0 {
		return 0, nil
	}
	cnt, err = e.execSQL(ctx, db, stmt)
	return cnt, err
}

func (e *Executor) insert(ctx context.Context, db *sql.DB, data []interface{}, stmt string) (int, error) {
	cnt := 0
	var err error
	start := time.Now()
	defer func() {
		e.metric.Insert.Main.Time = time.Now().Sub(start)
		e.metric.Insert.Main.Affected = cnt
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("### INSERTING TIME %s FOR %d OF %d RECORDS \n", e.metric.Insert.Main.Time, cnt, len(data)))
	}()

	if len(data) == 0 {
		return 0, nil
	}
	cnt, err = e.execSQL(ctx, db, stmt)
	return cnt, err
}

func (e *Executor) insertByLoad(ctx context.Context, db *sql.DB, data []interface{}, table string, options ...moption.Option) (int, error) {
	cnt := 0
	var err error
	start := time.Now()
	defer func() {
		switch e.config.Strategy {
		case info.PresetMergeStrategyBaseUpsDel:
			e.metric.Upsert.Main.Time = time.Now().Sub(start)
			e.metric.Upsert.Main.Affected = cnt
			e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("### UPSERTING (LOAD) TIME %s FOR %d OF %d RECORDS \n", e.metric.Upsert.Main.Time, cnt, len(data)))
		default:
			e.metric.Insert.Main.Time = time.Now().Sub(start)
			e.metric.Insert.Main.Affected = cnt
			e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("### INSERTING (LOAD) TIME %s FOR %d OF %d RECORDS \n", e.metric.Insert.Main.Time, cnt, len(data)))
		}
	}()

	cnt, err = e.load(ctx, db, table, data, true, options...)
	return cnt, err
}

func (e *Executor) execSQL(ctx context.Context, db *sql.DB, stmt string) (int, error) {
	var err error
	var result sql.Result

	if e.Transaction != nil && e.Transaction.Tx != nil {
		result, err = e.Transaction.ExecContext(ctx, stmt)
	} else {
		result, err = db.ExecContext(ctx, stmt)
	}

	if err != nil {
		return 0, err
	}

	cnt, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(cnt), nil
}

func (e *Executor) loadTransientTable(ctx context.Context, db *sql.DB, data []interface{}, table string, initSQL []string, operation *metric.Operation, options ...moption.Option) (int, error) {
	fnName := "loadTransientTable"

	cnt := 0
	var err error
	start := time.Now()
	defer func() {
		duration := time.Now().Sub(start)
		operation.Time = duration
		operation.Affected = cnt
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("### LOADING TRANSIENT TABLE %s TIME %s FOR %d OF %d RECORDS \n", table, duration, cnt, len(data)))
	}()

	for _, SQL := range initSQL {
		_, err = db.Exec(SQL)
		if err != nil {
			err = fmt.Errorf("%s.%s: unable to init sql %s due to: %w", packageName, fnName, SQL, err)
			return 0, err
		}
	}

	cnt, err = e.load(ctx, db, table, data, true, options...)
	return cnt, err
}

func (e *Executor) load(ctx context.Context, db *sql.DB, table string, data []interface{}, checkCount bool, options ...moption.Option) (int, error) {
	fnName := "load"

	if len(data) == 0 { // loader panics with empty data
		return 0, nil
	}

	loader, err := e.ensureLoader(ctx, db, table, options...)
	if err != nil {
		return 0, err
	}

	loadOptions := moption.NewOptions(options...).GetLoadOptions()
	if e.Transaction != nil && e.Transaction.Tx != nil {
		loadOptions = append(loadOptions, loption.WithTransaction(e.Transaction.Tx))
	}
	cnt, err := loader.Exec(ctx, data, loadOptions...)
	if err != nil {
		return 0, fmt.Errorf("%s: unable to load data into table %s due to: %w", fnName, table, err)
	}

	loadOpts := loption.NewOptions(loadOptions...)
	if checkCount {
		switch withUpsert := loadOpts.GetWithUpsert(); withUpsert {
		case true:
			// Warning cnt can be also 2 times greater than len(data) if all records where replaced
			// withUpsert causes REPLACE option for LOAD INTO command, which doesn't omit db errors
			if cnt < len(data) {
				return 0, fmt.Errorf("%s (withupsert = %t): loaded only %d of %d records into table %s", fnName, withUpsert, cnt, len(data), table)
			}
		case false:
			if cnt != len(data) {
				return 0, fmt.Errorf("%s (withupsert = %t): loaded only %d of %d records into table %s", fnName, withUpsert, cnt, len(data), table)
			}
		}
	}

	return cnt, nil
}
