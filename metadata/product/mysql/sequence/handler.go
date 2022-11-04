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

// Warning!
// Until we don't use autoincrement (by insert with 0 value id), "show create table" and "information_schema.tables"
// show wrong autoincrement value if auto_increment_increment > 1
func UpdateMySQLSequence(ctx context.Context, db *sql.DB, target interface{}, iopts ...interface{}) (doNext bool, err error) {

	options := option.AsOptions(iopts)
	tx := options.Tx()

	switch actual := target.(type) {
	case *sink.Sequence:
		return false, updateSequence(ctx, db, actual, tx)
	case *[]sink.Sequence:
		for i := range *actual {
			if err := updateSequence(ctx, db, &(*actual)[i], tx); err != nil {
				return false, err
			}
		}
	case *[]*sink.Sequence:
		for i := range *actual {
			if err := updateSequence(ctx, db, (*actual)[i], tx); err != nil {
				return false, err
			}
		}
	}
	return false, nil
}

func updateSequence(ctx context.Context, db *sql.DB, sequence *sink.Sequence, tx *sql.Tx) error {
	var name, DDL string
	SQL := buildShowCreate([]interface{}{sequence.Catalog, sequence.Schema, sequence.Name})

	if err := runQuery(ctx, db, SQL, []interface{}{&name, &DDL}, tx); err != nil {
		return err
	}

	if index := strings.Index(DDL, autoincrementAssignment); index != -1 {
		seqValueFragment := DDL[index+len(autoincrementAssignment):]
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
	if err := runQuery(ctx, db, "SELECT @@SESSION.auto_increment_increment, @@SESSION.auto_increment_offset", []interface{}{&sequence.IncrementBy, &offset}, tx); err != nil {
		return err
	}

	if sequence.Value == 0 {
		sequence.Value = offset
	}
	if sequence.StartValue == 0 {
		sequence.StartValue = offset
	}
	return nil
}

func buildShowCreate(arguments []interface{}) string {
	return fmt.Sprintf("SHOW CREATE TABLE %v.%v", arguments[1], arguments[2])
}
