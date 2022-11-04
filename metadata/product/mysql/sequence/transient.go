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

type Transient struct{}

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
	defer io.MergeErrorIfNeeded(n.unlock(ctx, meta, db, options), &err)

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
	defer io.MergeErrorIfNeeded(tx.Rollback(), &err)

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
	arguments := argsOps.Unwrap()

	err := meta.Info(ctx, db, info.KindLockGet, &result, options...)
	if err != nil {
		return err
	}
	if result.Success == 0 {
		return fmt.Errorf("unable to create lock for (catalog, schema, table) = (%s, %s, %s), already exists the same lock in use", arguments[0], arguments[1], arguments[2])
	}
	return nil
}

func (n *Transient) unlock(ctx context.Context, meta *metadata.Service, db *sql.DB, options option.Options) error {
	result := sink.Lock{}
	argsOps := options.Args()
	if argsOps == nil {
		return fmt.Errorf("argsOps was empty")
	}
	arguments := argsOps.Unwrap()

	err := meta.Info(ctx, db, info.KindLockRelease, &result, options...)
	if err != nil {
		return err
	}
	if result.Success == 0 {
		return fmt.Errorf("unable to release lock for (catalog, schema, table) = (%s, %s, %s), already exists the same lock in use", arguments[0], arguments[1], arguments[2])
	}
	return nil
}

func (n *Transient) CanUse(iopts ...interface{}) bool {
	options := option.AsOptions(iopts)
	return options.AutoincrementStrategy() == option.PresetIdWithTransientTransaction
}
