package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

const autoincrementAssignment = "AUTO_INCREMENT="
const autoincrementColumnDef = " NOT NULL AUTO_INCREMENT,"

// MaxSeqValue represents default maximum sequence value
const MaxSeqValue = 9223372036854775807

// UpdateMySQLSequence updates for all passed sink.Sequences theirs Value or StartValue
// getting autoincrement metadata by: SHOW CREATE TABLE ...,  @@SESSION.auto_increment_increment, @@SESSION.auto_increment_offset
//
// Warning!
// Until we don't use autoincrement in the table (by insert at least one row with 0-value id), "show create table" and "information_schema.tables"
// show wrong autoincrement value if @@SESSION.auto_increment_increment > 1
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
		debugSequencer := os.Getenv("DEBUG_SEQUENCER") == "true"
		if debugSequencer {
			fmt.Printf("u sequencer: %v, value: %v\n", sequence.Name, seqValueFragment)
		}
		if index := strings.Index(seqValueFragment, " "); index != -1 {
			seqValueFragment = seqValueFragment[:index]
			value, err := strconv.Atoi(seqValueFragment)
			if err != nil {
				return fmt.Errorf("invalue sequence value: %w, %v", err, seqValueFragment)
			}
			sequence.Value = int64(value)
		}
	}

	if indexEnd := strings.Index(DDL, autoincrementColumnDef); indexEnd != -1 {
		colTypeEndedFragment := DDL[:indexEnd]
		if indexStart := strings.LastIndex(colTypeEndedFragment, " "); indexStart != -1 {
			colType := colTypeEndedFragment[indexStart+1:]
			sequence.DataType = colType
		}
	}
	err := ensureIncrementsAndValues(ctx, db, sequence, tx)
	if err != nil {
		return err
	}
	return nil
}

func ensureIncrementsAndValues(ctx context.Context, db *sql.DB, sequence *sink.Sequence, tx *sql.Tx) error {
	if sequence.IncrementBy == 0 || sequence.StartValue == 0 {
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
	}

	if sequence.Value == 0 {
		sequence.Value = sequence.StartValue
	}

	if sequence.MaxValue == 0 {
		sequence.MaxValue = MaxSeqValue
	}

	if sequence.DataType == "" {
		sequence.DataType = "int"
	}

	//debugSequencer := os.Getenv("DEBUG_SEQUENCER") == "true"
	//if debugSequencer {
	//	fmt.Printf("e sequencer_ptr: %p sequencer: %v, start: %v, value: %v, max: %v, increment: %v\n", &sequence, sequence.Name, sequence.StartValue, sequence.Value, sequence.MaxValue, sequence.IncrementBy)
	//}
	return nil
}

func buildShowCreate(arguments []interface{}) string {
	return fmt.Sprintf("SHOW CREATE TABLE %v.%v", arguments[1], arguments[2])
}
