package merge

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/load"
	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/product/mysql/merge/config"
	"github.com/viant/sqlx/metadata/product/mysql/merge/metric"
	"github.com/viant/sqlx/moption"
	"github.com/viant/sqlx/option"
	"sync"
	"time"
)

var packageName = "merge"

type (
	// Executor represents MySQL merger session
	Executor struct {
		*io.Transaction
		dialect *info.Dialect
		columns io.Columns
		config  *config.Config
		loaders map[string]*load.Service
		mux     sync.Mutex
		metric  *metric.Metric
	}

	fnPreIns func(ctx context.Context, db *sql.DB, data []interface{}, table string, initSQL []string, operation *metric.Operation, options ...moption.Option) (int, error)
	fnPreUpd func(ctx context.Context, db *sql.DB, data []interface{}, table string, initSQL []string, operation *metric.Operation, options ...moption.Option) (int, error)
	fnPreDel func(ctx context.Context, db *sql.DB, data []interface{}, table string, initSQL []string, operation *metric.Operation, options ...moption.Option) (int, error)
	fnIns    func(ctx context.Context, db *sql.DB, data []interface{}, stmt string) (int, error)
	fnUpd    func(ctx context.Context, db *sql.DB, data []interface{}, stmt string) (int, error)
	fnDel    func(ctx context.Context, db *sql.DB, data []interface{}, stmt string) (int, error)

	fnInsByLoad func(ctx context.Context, db *sql.DB, data []interface{}, table string, options ...moption.Option) (int, error)
)

// NewMergeExecutor returns new MySQL session
func NewMergeExecutor(dialect *info.Dialect, cfg info.MergeConfig) (io.MergeExecutor, error) {
	mConfig, ok := cfg.(*config.Config)
	if !ok {
		return nil, fmt.Errorf("newmergeexecutor: unexpected config type, expected %T got %T", mConfig, cfg)
	}

	return &Executor{
		dialect: dialect,
		loaders: make(map[string]*load.Service),
		config:  mConfig,
		metric:  &metric.Metric{},
	}, nil
}

// Exec merges data to database table
func (e *Executor) Exec(ctx context.Context, srcData interface{}, db *sql.DB, tableName string, options ...moption.Option) (info.MergeResult, error) {
	start := time.Now()
	var err error
	defer func() { e.metric.Err = err }()

	err = e.ensureConfig()
	if err != nil {
		return e.metric, err
	}

	// TODO what if srcData is empty?
	valueAt, allInSrcCnt, err := io.Values(srcData)
	var dataToInsert, dataToUpdate, dataToDelete []interface{}

	switch e.config.Strategy {
	case info.PresetMergeStrategyBaseInsDel:
		dataToInsert, dataToUpdate, dataToDelete, err = e.prepareDMLDataSetsInsDel(ctx, db, valueAt, allInSrcCnt)
	case info.PresetMergeStrategyBaseInsUpdDel:
		dataToInsert, dataToUpdate, dataToDelete, err = e.prepareDMLDataSetsInsUpdDel(ctx, db, valueAt, allInSrcCnt)
	case info.PresetMergeStrategyBaseUpsDel:
		dataToInsert, dataToUpdate, dataToDelete, err = e.prepareDMLDataSetsUpsDel(ctx, db, valueAt, allInSrcCnt)
	default:
		return e.metric, fmt.Errorf("merge executor: unsupported strategy %v", e.config.Strategy)
	}
	if err != nil {
		return e.metric, err
	}

	var preInsert fnPreIns
	var preUpdate fnPreUpd
	var preDelete fnPreDel

	var insert fnIns
	var update fnUpd
	var del fnDel
	var insertByLoad fnInsByLoad

	// DELETE
	var preDelOptions []moption.Option
	if e.config.Delete != nil && e.config.Delete.Transient != nil {
		preDelete = e.loadTransientTable
		opt := moption.WithLoadOptions(e.config.Delete.Transient.LoadOptions)
		preDelOptions = []moption.Option{opt}
	}

	if e.config.Delete != nil {
		del = e.delete
	}

	// UPDATE
	var preUpdOptions []moption.Option
	if e.config.Update != nil && e.config.Update.Transient != nil {
		preUpdate = e.loadTransientTable
		opt := moption.WithLoadOptions(e.config.Update.Transient.LoadOptions)
		preUpdOptions = []moption.Option{opt}
	}

	if e.config.Update != nil {
		update = e.update
	}

	// INSERT
	var preInsOptions []moption.Option
	if e.config.Insert != nil && e.config.Insert.Transient != nil {
		preInsert = e.loadTransientTable
		opt := moption.WithLoadOptions(e.config.Insert.Transient.LoadOptions)
		preInsOptions = append(preInsOptions, opt)
	}

	var insOptions []moption.Option
	if e.config.Insert != nil {
		switch e.config.Insert.InsertStrategy {
		case "", info.PresetMergeInsStrategyBase:
			insert = e.insert
		case info.PresetMergeInsStrategyInsByLoad:
			preInsert = nil
			insert = nil
			insertByLoad = e.insertByLoad
		}
		opts := moption.WithLoadOptions(e.config.Insert.LoadOptions)
		if e.config.Insert.LoadOptions != nil {
			insOptions = append(insOptions, opts)
		}
	}

	if insert != nil && insertByLoad != nil {
		return e.metric, fmt.Errorf("merge executor: unable to handle both opertions at the same time: insert and insertByLoad")
	}

	if preDelete != nil {
		_, err = preDelete(ctx, db, dataToDelete, e.config.Delete.Transient.Table(), e.config.Delete.Transient.InitSQLs(), &e.metric.Delete.Transient, preDelOptions...)
		if err != nil {
			return e.metric, err
		}
	}

	if preUpdate != nil {
		_, err = preUpdate(ctx, db, dataToUpdate, e.config.Update.Transient.Table(), e.config.Update.Transient.InitSQLs(), &e.metric.Update.Transient, preUpdOptions...)
		if err != nil {
			return e.metric, err
		}
	}

	if preInsert != nil {
		_, err = preInsert(ctx, db, dataToInsert, e.config.Insert.Transient.Table(), e.config.Insert.Transient.InitSQLs(), &e.metric.Insert.Transient, preInsOptions...)
		if err != nil {
			return e.metric, err
		}
	}

	if err = e.begin(ctx, db, options...); err != nil {
		return e.metric, err
	}

	if del != nil {
		_, err = del(ctx, db, dataToDelete, e.config.Delete.DeleteSQL)
		if err != nil {
			return e.metric, e.end(err)
		}
	}

	if update != nil {
		_, err = update(ctx, db, dataToUpdate, e.config.Update.UpdateSQL)
		if err != nil {
			return e.metric, e.end(err)
		}
	}

	if insert != nil {
		_, err = insert(ctx, db, dataToInsert, e.config.Insert.InsertSQL)
		if err != nil {
			return e.metric, e.end(err)
		}
	}

	if insertByLoad != nil {
		_, err = insertByLoad(ctx, db, dataToInsert, tableName, insOptions...)
		if err != nil {
			return e.metric, e.end(err)
		}
	}

	err = e.end(err)

	e.metric.TotalTime = time.Now().Sub(start)
	e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("# TOTAL TIME: %s\n", e.metric.TotalTime))
	e.metric.Summary()

	//toolbox.Dump(e.metric)
	//
	//for _, s := range e.metric.Total.Report {
	//	fmt.Print(s)
	//}

	return e.metric, err
}

func (e *Executor) begin(ctx context.Context, db *sql.DB, options ...moption.Option) error {
	var err error
	mOpts := moption.NewOptions(options...)
	opts := []option.Option{mOpts.GetTransaction()}

	e.Transaction, err = io.TransactionFor(ctx, e.dialect, db, opts)
	if err != nil {
		return err
	}
	return nil
}

func (e *Executor) end(err error) error {
	if e.Transaction == nil || e.Transaction.Tx == nil {
		return err
	}

	if err != nil {
		return e.Transaction.RollbackWithErr(err)
	}

	return e.Transaction.Commit()
}

func (e *Executor) adjustConfig() error {
	if e.config == nil {
		return fmt.Errorf("merge sesssion: empty config")
	}

	if e.config.Strategy == "" {
		e.config.Strategy = info.PresetMergeStrategyBaseInsDel
	}
	return nil
}

func (e *Executor) validateConfig() error {

	switch e.config.Strategy {
	case info.PresetMergeStrategyBaseInsDel:
		if e.config.Insert == nil {
			return fmt.Errorf("merge session validate config: undefined insert config for strategy: %v", e.config.Strategy)
		}
		if e.config.Update != nil {
			return fmt.Errorf("merge session validate config: unable to use defined update config for strategy: %v", e.config.Strategy)
		}
		if e.config.Delete == nil {
			return fmt.Errorf("merge session validate config: undefined delete config for strategy: %v", e.config.Strategy)
		}
	case info.PresetMergeStrategyBaseInsUpdDel:
		if e.config.Insert == nil {
			return fmt.Errorf("merge session validate config: undefined insert config for strategy: %v", e.config.Strategy)
		}
		if e.config.Update == nil {
			return fmt.Errorf("merge session validate config: undefined update config for strategy: %v", e.config.Strategy)
		}
		if e.config.Delete == nil {
			return fmt.Errorf("merge session validate config: undefined delete config for strategy: %v", e.config.Strategy)
		}
	case info.PresetMergeStrategyBaseUpsDel:
		if e.config.Insert == nil {
			return fmt.Errorf("merge session validate config: undefined insert config for strategy: %v", e.config.Strategy)
		}
		if e.config.Update != nil {
			return fmt.Errorf("merge session validate config: unable to use defined update config for strategy: %v", e.config.Strategy)
		}
		if e.config.Delete == nil {
			return fmt.Errorf("merge session validate config: undefined delete config for strategy: %v", e.config.Strategy)
		}

		switch e.config.Insert.InsertStrategy {
		case info.PresetMergeInsStrategyInsByLoad:
			opts := loption.NewOptions(e.config.Insert.LoadOptions...)
			if !opts.GetWithUpsert() {
				return fmt.Errorf("merge session validate config: merge strategy %v combined with insert strategy %v require upsert option for insert loader", e.config.Strategy, e.config.Insert.InsertStrategy)
			}

		default:
			return fmt.Errorf("merge session validate config: unsupported insert stategy %v for merge strategy: %v", e.config.Insert.InsertStrategy, e.config.Strategy)
		}

	default:
		return fmt.Errorf("merge session: unsupported merge startegy: %v", e.config.Strategy)
	}

	return nil
}

func (e *Executor) ensureConfig() error {
	err := e.adjustConfig()
	if err != nil {
		return err
	}
	return e.validateConfig()
}
