package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"math"
)

// Max represents struct used to setting new autoincrement value
// using simple "SELECT MAX(ID)" approach
type Max struct{}

// Handle sets new autoincrement value using simple "SELECT MAX(ID)" approach
func (n *Max) Handle(ctx context.Context, db *sql.DB, target interface{}, iopts ...interface{}) (doNext bool, err error) {
	options := option.AsOptions(iopts)

	recordCount := options.RecordCount()
	if recordCount <= 0 {
		return false, fmt.Errorf("invalid recordCount option, expected > 0, but had: %d", recordCount)
	}

	targetSequence, ok := target.(*sink.Sequence)
	if !ok || targetSequence == nil {
		return false, fmt.Errorf("invalid target, expected :%T, but had: %T", targetSequence, target)
	}

	maxIDSQLBuilder := options.MaxIDSQLBuilder()
	if maxIDSQLBuilder == nil {
		return false, fmt.Errorf("maxIDSQLBuilder was empty")
	}
	maxIDSQL := maxIDSQLBuilder()

	if maxIDSQL == nil {
		return false, fmt.Errorf("maxIDSQL was empty")
	}

	var maxID int64 = 0
	var queryer sequenceQueryer = db
	if tx := options.Tx(); tx != nil {
		queryer = tx
	} else {
		connection, err := db.Conn(ctx)
		if err != nil {
			return false, err
		}
		defer connection.Close()
		queryer = connection
	}
	row := queryer.QueryRowContext(ctx, maxIDSQL.Query, maxIDSQL.Args...)
	err = row.Scan(&maxID)
	if err != nil {
		return false, err
	}
	err = row.Err()
	if err != nil {
		return false, err
	}

	sequence, err := (&Metadata{}).identity(ctx, queryer, options)
	if err != nil {
		return false, err
	}
	if maxID < 0 {
		maxID = 0
	}
	if recordCount >= math.MaxInt64 || maxID > math.MaxInt64-recordCount-1 {
		return false, fmt.Errorf("sequence range overflows int64")
	}
	sequence.StartValue = 1
	sequence.IncrementBy = 1
	sequence.Value = maxID + 1

	sequence.Value = sequence.NextValue(recordCount)
	*targetSequence = sequence

	return false, nil
}

// CanUse returns true if Handle function can be executed
func (n *Max) CanUse(iopts ...interface{}) bool {
	options := option.AsOptions(iopts)
	return options.PresetIDStrategy() == dialect.PresetIDWithMax
}
