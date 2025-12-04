package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

const retryMaxCount = 3
const minSequenceArgs = 3

// Transient represents struct used to setting new autoincrement value
// using insert inside new transaction finished by rollback
type Transient struct{}

// Handle sets new autoincrement value by inserting row using new transaction finished by rollback, uses locking
func (n *Transient) HandleLegacy(ctx context.Context, db *sql.DB, target interface{}, iopts ...interface{}) (doNext bool, err error) {
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

		transientDML, _, err := sequenceSQLBuilder(&sequence)
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

func ensureTransientDMLAndRecCnt(sequence sink.Sequence, options option.Options) (*sqlx.SQL, int64, error) {
	sequenceSQLBuilder := options.SequenceSQLBuilder()
	if sequenceSQLBuilder == nil {
		return nil, 0, fmt.Errorf("error: SequenceSQLBuilder is nil")
	}

	transientDML, recordCnt, err := sequenceSQLBuilder(&sequence)
	if err != nil {
		return nil, 0, err
	}

	if transientDML == nil {
		return nil, 0, fmt.Errorf("builder returned nil transientDML")
	}

	return transientDML, recordCnt, nil
}

func (n *Transient) prepareSequence(ctx context.Context, db *sql.DB, tx *sql.Tx, argsOps *option.Args) (sink.Sequence, error) {
	catalog, schema, name, err := n.parseSequenceArgs(argsOps.Unwrap())
	if err != nil {
		return sink.Sequence{}, err
	}
	sequence := sink.Sequence{Catalog: catalog, Name: name, Schema: schema}

	err = ensureIncrementsAndValues(ctx, db, &sequence, tx)
	if err != nil {
		return sink.Sequence{}, err
	}
	return sequence, nil
}

// parseSequenceArgs validates and extracts catalog, schema and name from arguments
func (n *Transient) parseSequenceArgs(arguments []interface{}) (catalog, schema, name string, err error) {
	errPrefix := "parseSequenceArgs failed:"
	if len(arguments) < minSequenceArgs {
		return "", "", "", fmt.Errorf("unable to get sequence's metadata due to: len(arguments) < %d", minSequenceArgs)
	}
	var ok bool
	catalog, ok = arguments[0].(string)
	if !ok {
		return "", "", "", fmt.Errorf("%s sequence catalog must be string, got %T", errPrefix, arguments[0])
	}
	schema, ok = arguments[1].(string)
	if !ok {
		return "", "", "", fmt.Errorf("%s sequence schema must be string, got %T", errPrefix, arguments[1])
	}
	name, ok = arguments[2].(string)
	if !ok {
		return "", "", "", fmt.Errorf("%s sequence name must be string, got %T", errPrefix, arguments[2])
	}
	return catalog, schema, name, nil
}

// presetSequence performs the transient insert(s) to set the sequence value using a savepoint and retries
func (n *Transient) presetSequence(ctx context.Context, tx *sql.Tx, sequence *sink.Sequence, query string, args []interface{}, recCnt int64) error {
	var err error

	// Create savepoint to safely remove the first row before the second insert
	if _, err = tx.ExecContext(ctx, "SAVEPOINT seq_reserve"); err != nil {
		return err
	}

	// Disable FK checks during transient inserts
	_ = n.turnFkKeyCheck(tx, 0)                    // intentionally ignore error
	defer func() { _ = n.turnFkKeyCheck(tx, 1) }() // re-enable; intentionally ignore error

	for i := 1; i <= retryMaxCount; i++ {

		// PERF: Modify only the last arg in-place (ID placeholder) instead of copying the slice.
		// Safe because args is only used in this function, and we change only the last element which is our tmp ID.
		// Other args can't be modified
		argsWithEmptyID := args
		argsWithEmptyID[len(argsWithEmptyID)-1] = nil

		res, execErr := tx.ExecContext(ctx, query, argsWithEmptyID...)
		if execErr != nil {
			err = execErr
			continue
		}

		baseID, idErr := res.LastInsertId()
		if idErr != nil {
			err = idErr
			_, _ = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT seq_reserve")
			continue
		}
		sequence.Value = baseID

		if recCnt <= 1 {
			sequence.Value = baseID + sequence.IncrementBy
			break // Success for single record insert

		}

		// Remove the first row; AUTO_INCREMENT bump remains
		if _, derr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT seq_reserve"); derr != nil {
			err = derr
			continue
		}

		maxIdValue, err := sequence.ComputeNextForTransient(recCnt)
		if err != nil {
			return err
		}

		// PERF: Modify only the last arg in-place (ID placeholder) instead of copying the slice.
		// Safe because args is only used in this function, and we change only the last element which is our tmp ID.
		// Other args can't be modified
		argsWithMaxID := args
		argsWithMaxID[len(argsWithMaxID)-1] = &maxIdValue

		if _, execErr := tx.ExecContext(ctx, query, argsWithMaxID...); execErr != nil {
			err = execErr
			continue
		}
		// Success
		break
	}

	return err
}

func (n *Transient) Handle(ctx context.Context, db *sql.DB, target interface{}, iopts ...interface{}) (doNext bool, err error) {
	doNext = false // always false for backward compatibility
	errPrefix := "sequence.Handle failed:"
	meta := metadata.New()
	var options option.Options = option.AsOptions(iopts)
	argsOps := options.Args()
	if argsOps == nil {
		return doNext, fmt.Errorf("%s options.Args() returned nil", errPrefix)
	}

	targetSequence, ok := target.(*sink.Sequence)
	if !ok {
		return doNext, fmt.Errorf("%s invalid target, expected :%T, but had: %T", errPrefix, targetSequence, target)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return doNext, fmt.Errorf("%s %w", errPrefix, err)
	}
	defer io.RunWithError(tx.Rollback, &err)

	lockingOptions := option.Options{argsOps, tx}
	product := option.Options.Product(options)
	if product != nil {
		lockingOptions = append(lockingOptions, product)
	}

	// Extract basic sequence metadata
	sequence, err := n.prepareSequence(ctx, db, tx, argsOps)
	if err != nil {
		return doNext, fmt.Errorf("%s unable to prepare sequence: %w", errPrefix, err)
	}

	// Build DML for transient inserts (auto-increment id)
	transientDML, recordCnt, err := ensureTransientDMLAndRecCnt(sequence, options)
	if err != nil {
		return doNext, fmt.Errorf("%s unable to build transient DML: %w", errPrefix, err)
	}

	if err = n.lock(ctx, meta, db, lockingOptions); err != nil {
		return doNext, fmt.Errorf("%s %w", errPrefix, err)
	}
	fn := func() error { return n.unlock(ctx, meta, db, lockingOptions) }
	defer io.RunWithError(fn, &err)

	// First insert with NULL id to get the actual base value,
	// then optional second insert to reserve up to target if RecCnt > 1
	if err = n.presetSequence(ctx, tx, &sequence, transientDML.Query, transientDML.Args, recordCnt); err != nil {
		return doNext, fmt.Errorf("%s unable to get sequence values after %d attempts: %w", errPrefix, retryMaxCount, err)
	}

	*targetSequence = sequence

	//debugSequencer := os.Getenv("DEBUG_SEQUENCER") == "true"
	//if debugSequencer {
	//	fmt.Printf("t sequencer_ptr: %p sequencer: %v, start: %v, value: %v, max: %v, increment: %v\n", &sequence, sequence.Name, sequence.StartValue, sequence.Value, sequence.MaxValue, sequence.IncrementBy)
	//}

	return doNext, nil
}
