package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

type Max struct{}

func (n *Max) Handle(ctx context.Context, db *sql.DB, target interface{}, iopts ...interface{}) (doNext bool, err error) {
	options := option.AsOptions(iopts)

	recordCount := options.RecordCount()
	if recordCount == 0 {
		return false, fmt.Errorf("invalid recordCount option, expected > 0, but had: %d", recordCount)
	}

	targetSequence, ok := target.(*sink.Sequence)
	if !ok {
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
	row := db.QueryRowContext(ctx, maxIDSQL.Query, maxIDSQL.Args...)
	err = row.Scan(&maxID)
	if err != nil {
		return false, err
	}
	err = row.Err()
	if err != nil {
		return false, err
	}

	sequence := sink.Sequence{}
	sequence.StartValue = 1
	sequence.IncrementBy = 1
	sequence.Value = maxID + 1

	sequence.Value = sequence.NextValue(recordCount)
	*targetSequence = sequence

	return false, nil
}

func (n *Max) CanUse(iopts ...interface{}) bool {
	options := option.AsOptions(iopts)
	return options.AutoincrementStrategy() == option.PresetIdWithMax
}
