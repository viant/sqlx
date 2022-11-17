package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// Next represents struct used to setting new autoincrement value
// using insert inside new transaction finished by rollback
type Next struct{}

// Handle sets new autoincrement value by inserting row using new transaction finished by rollback, uses locking
func (n *Next) Handle(ctx context.Context, db *sql.DB, target interface{}, iopts ...interface{}) (doNext bool, err error) {

	meta := metadata.New()
	options := option.AsOptions(iopts)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Commit()

	argsOps := options.Args()
	if argsOps == nil {
		return false, fmt.Errorf("argsOps was empty")
	}

	targetSequence, ok := target.(*sink.Sequence)
	if !ok {
		return false, fmt.Errorf("invalid target, expected :%T, but had: %T", targetSequence, target)
	}
	sequence := sink.Sequence{}
	err = meta.Info(ctx, db, info.KindSequences, &sequence, options...)
	if err != nil {
		return false, err
	}

	if err != nil {
		return false, err
	}

	count := options.RecordCount()
	var args = []interface{}{
		sequence.Value + count,
	}
	DML := fmt.Sprintf("SELECT setval('%s_id_seq', ?, true)", sequence.Name)
	row := tx.QueryRowContext(ctx, DML, args...)
	val := 0
	if err = row.Scan(&val); err != nil {
		return false, err
	}
	if err = row.Err(); err != nil {
		return false, err
	}
	sequence.Value += count + 1
	sequence.IncrementBy = 1
	*targetSequence = sequence
	return false, nil
}

// CanUse returns true if Handle function can be executed
func (n *Next) CanUse(iopts ...interface{}) bool {
	return true
}
