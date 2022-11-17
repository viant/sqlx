package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
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

	if err = n.lock(ctx, meta, db, options); err != nil {
		return false, err
	}
	fn := func() error { return n.unlock(ctx, meta, db, options) }
	defer io.MergeErrorIfNeeded(fn, &err)

	sequence := sink.Sequence{}

	err = meta.Info(ctx, db, info.KindSequences, &sequence, options...)
	if err != nil {
		return false, err
	}

	sequenceSQLBuilder := options.SequenceSQLBuilder()
	if sequenceSQLBuilder == nil {
		return false, fmt.Errorf("SequenceSQLBuilder was empty")
	}

	transientDML, err := sequenceSQLBuilder(&sequence)
	if err != nil {
		return false, err
	}
	if transientDML == nil {
		return false, fmt.Errorf("transientDML was empty")
	}

	tx, err := db.BeginTx(ctx, nil)
	defer io.MergeErrorIfNeeded(tx.Rollback, &err)

	if err != nil {
		return false, err
	}

	_, err = tx.ExecContext(ctx, transientDML.Query, transientDML.Args...)
	if err != nil {
		return false, err
	}

	*targetSequence = sequence

	return false, nil
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
	return options.PresetIDStrategy() == option.PresetIDWithTransientTransaction
}
