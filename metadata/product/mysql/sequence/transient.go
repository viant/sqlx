package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// Transient represents struct used to setting new autoincrement value
// using insert inside new transaction finished by rollback
type Transient struct{}

// Handle sets new autoincrement value by inserting row using new transaction finished by rollback, uses locking
func (n *Transient) Handle(ctx context.Context, db *sql.DB, target interface{}, iopts ...interface{}) (doNext bool, err error) {

	meta := metadata.New()
	options := option.AsOptions(iopts)

	argsOps := options.Args()
	if argsOps == nil {
		return false, fmt.Errorf("argsOps was empty")
	}

	targetSequence, ok := target.(*sink.Sequence)
	if !ok {
		return false, fmt.Errorf("invalid target, expected :%T, but had: %T", targetSequence, target)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer io.RunWithError(tx.Rollback, &err)

	lockingOptions := option.Options{argsOps, tx}
	product := option.Options.Product(options)
	if product != nil {
		lockingOptions = append(lockingOptions, product)
	}

	if err = n.lock(ctx, meta, db, lockingOptions); err != nil {
		return false, err
	}
	fn := func() error { return n.unlock(ctx, meta, db, lockingOptions) }
	defer io.RunWithError(fn, &err)

	sequence := sink.Sequence{}
	arguments := argsOps.Unwrap()
	if len(arguments) < 3 {
		return false, fmt.Errorf("unable to get sequence's metadata due to: len(arguments) < 3")
	}

	sequence.Catalog = arguments[0].(string)
	sequence.Schema = arguments[1].(string)
	sequence.Name = arguments[2].(string)

	sequenceSQLBuilder := options.SequenceSQLBuilder()
	if sequenceSQLBuilder == nil {
		return false, fmt.Errorf("SequenceSQLBuilder was empty")
	}

	retryMaxCnt := 5

	for i := 1; i <= retryMaxCnt; i++ {
		err = updateSequence(ctx, db, &sequence, tx)
		if err != nil {
			return false, err
		}

		transientDML, err := sequenceSQLBuilder(&sequence)
		if err != nil {
			return false, err
		}
		if transientDML == nil {
			return false, fmt.Errorf("transientDML was empty")
		}

		_ = n.turnFkKeyCheck(tx, 0)
		_, err = tx.ExecContext(ctx, transientDML.Query, transientDML.Args...)
		_ = n.turnFkKeyCheck(tx, 1)

		if err != nil && i < retryMaxCnt {
			continue
		}

		if err != nil { //temp workaround of cascading sequencer
			err = fmt.Errorf("unable to get sequence values (attempt %d) using transient dml %v due to: %w", i, transientDML, err)
		}

		break
	}

	if err != nil {
		return false, err
	}

	*targetSequence = sequence
	return false, nil
}

func (n *Transient) turnFkKeyCheck(tx *sql.Tx, flag int) error {
	_, err := tx.Exec("SET foreign_key_checks = " + strconv.Itoa(flag))
	return err
}

func (n *Transient) lock(ctx context.Context, meta *metadata.Service, db *sql.DB, options option.Options) error {
	result := sink.Lock{}
	argsOps := options.Args()
	if argsOps == nil {
		return fmt.Errorf("argsOps was empty")
	}
	err := meta.Info(ctx, db, info.KindLockGet, &result, options...)
	if err != nil {
		return err
	}
	if result.Success == 0 {
		return fmt.Errorf("failed to acquire lock '%v'", result.Name)
	}
	return nil
}

func (n *Transient) unlock(ctx context.Context, meta *metadata.Service, db *sql.DB, options option.Options) error {
	result := sink.Lock{}
	argsOps := options.Args()
	if argsOps == nil {
		return fmt.Errorf("argsOps was empty")
	}
	err := meta.Info(ctx, db, info.KindLockRelease, &result, options...)
	if err != nil {
		return err
	}
	if result.Success == 0 {
		return fmt.Errorf("failed to release lock '%s'", result.Name)
	}
	return nil
}

// CanUse returns true if Handle function can be executed
func (n *Transient) CanUse(iopts ...interface{}) bool {
	options := option.AsOptions(iopts)
	return options.PresetIDStrategy() == dialect.PresetIDWithTransientTransaction
}
