package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"strconv"
	"strings"
)

const autoincrementAssignment = "AUTO_INCREMENT="

func UpdateSequence(ctx context.Context, db *sql.DB, target interface{}, args interface{}) error {
	sequence, ok := target.(*sink.Sequence)
	if !ok {
		return fmt.Errorf("invalid auxiliarySink, expected :%T, but had: %T", sequence, target)
	}
	argsOps, ok := args.(*option.Args)
	if !ok {
		return fmt.Errorf("invalid auxiliarySink, expected :%T, but had: %T", argsOps, args)
	}
	arguments := argsOps.Unwrap()
	var name, SQL string
	if err := runQuery(ctx, db, buildShowCreate(arguments), &name, &SQL); err != nil {
		return err
	}

	if index := strings.Index(SQL, autoincrementAssignment); index != -1 {
		seqValueFragment := SQL[index+len(autoincrementAssignment):]
		if index := strings.Index(seqValueFragment, " "); index != -1 {
			seqValueFragment = seqValueFragment[:index]
			value, err := strconv.Atoi(seqValueFragment)
			if err != nil {
				return fmt.Errorf("invalue sequence value: %w, %v", err, seqValueFragment)
			}
			sequence.Value = int64(value)
		}
	}

	offset := int64(0)
	if err := runQuery(ctx, db, "SELECT @@SESSION.auto_increment_increment, @@SESSION.auto_increment_offset", &sequence.IncrementBy, &offset); err != nil {
		return err
	}
	if sequence.Value == 0 {
		sequence.Value = offset
	}
	return nil
}

func runQuery(ctx context.Context, db *sql.DB, SQL string, args ...interface{}) error {
	query, err := db.QueryContext(ctx, SQL)
	if err != nil {
		return err
	}
	defer query.Close()
	if query.Next() {
		return query.Scan(args...)
	}
	return fmt.Errorf("not records for %v", SQL)
}

func buildShowCreate(arguments []interface{}) string {
	return fmt.Sprintf("SHOW CREATE TABLE %v.%v", arguments[1], arguments[2])
}
