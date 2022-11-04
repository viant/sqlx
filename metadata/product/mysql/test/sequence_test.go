package mysql_test

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/metadata/sink"
	"os"
	"strconv"
	"testing"
)

// Don't set (@SESSION|@GLOBAL).auto_increment_offset > (@SESSION|@GLOBAL).auto_increment_increment
//
// MySQl has problem with proper returning/showing AUTOINCREMENT values when
// (@SESSION|@GLOBAL).auto_increment_offset > (@SESSION|@GLOBAL).auto_increment_increment
// e.g.: for auto_increment_increment = 2 and auto_increment_offset = 3
// AUTOINCREMENT values are shown:
// from real inserts (selected from table): 1 -> 3 -> 5 -> 7
// from SHOW CREATE TABLE: nil -> 2 -> 4 -> 6
// from INFORMATION_SCHEMA.TABLES (with ANALYZE TABLE before): 1 -> 2 -> 4 -> 6
func TestSequence_NextValue_Gen(t *testing.T) {
	driver := "mysql"
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN before running test")
	}
	dsnSchema := os.Getenv("TEST_MYSQL_DSN_SCHEMA")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN_SCHEMA before running test")
	}

	var testCases = []struct {
		description       string
		table             string
		initSQL           []string
		insertSQL         string
		minSeqStartValue  int
		maxSeqStartValue  int
		minSeqIncrementBy int
		maxSeqIncrementBy int
		minSeqValue       int
		maxSeqValue       int
		minRecordCnt      int64
		maxRecordCnt      int64
		seq               *sink.Sequence
		expected          int64
		hasError          bool
	}{
		{
			description: "1. Single NextValue test",
			table:       "t1",
			initSQL: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1 (foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, foo_name TEXT)",
			},
			insertSQL:         "INSERT INTO t1 (foo_name) VALUES ('foo')",
			minSeqStartValue:  3,
			maxSeqStartValue:  3,
			minSeqIncrementBy: 2,
			maxSeqIncrementBy: 2,
			minSeqValue:       1,
			maxSeqValue:       1,
			minRecordCnt:      1,
			maxRecordCnt:      1,
			seq:               nil,   // created inside test
			expected:          0,     // created inside test
			hasError:          false, // created inside test

		},
		{
			description: "1. Multi NextValue test (cases with StartValue > IncrementBy omitted)",
			table:       "t1",
			initSQL: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1 (foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, foo_name TEXT)",
			},
			insertSQL:         "INSERT INTO t1 (foo_name) VALUES ('foo')",
			minSeqStartValue:  1,
			maxSeqStartValue:  5,
			minSeqIncrementBy: 1,
			maxSeqIncrementBy: 5,
			minSeqValue:       1,
			maxSeqValue:       5,
			minRecordCnt:      1,
			maxRecordCnt:      5,
			seq:               nil,   // created inside test
			expected:          0,     // created inside test
			hasError:          false, // created inside test

		},
	}

	for _, testCase := range testCases {

		db, err := sql.Open(driver, dsn)
		if !assert.Nil(t, err, testCase.description) {
			return
		}

	outer:
		for startValue := testCase.minSeqStartValue; startValue <= testCase.maxSeqStartValue; startValue++ {
			for incrementBy := testCase.minSeqIncrementBy; incrementBy <= testCase.maxSeqIncrementBy; incrementBy++ {
				for value := testCase.minSeqValue; value <= testCase.minSeqValue; value++ {
					for recordCnt := testCase.minRecordCnt; recordCnt <= testCase.maxRecordCnt; recordCnt++ {
						//testCase.description = fmt.Sprintf("%d nodes active cluster, with local node %d offset, seqValue: %d, recordCnt: %d", incrementBy, startValue, value, recordCnt)
						ctx := context.Background()

						for _, SQL := range testCase.initSQL {
							_, err := db.Exec(SQL)
							if !assert.Nil(t, err, testCase.description) {
								continue outer
							}
						}

						tx, err := db.BeginTx(ctx, nil)
						var onDone = func(err error) {
							if err != nil {
								_ = tx.Rollback()
							}
							_ = tx.Commit()
						}
						if !assert.Nil(t, err, testCase.description) {
							onDone(err)
							continue outer
						}

						seq := &sink.Sequence{
							Catalog:     "",
							Schema:      "",
							Name:        "",
							Value:       int64(value),
							IncrementBy: int64(incrementBy),
							DataType:    "",
							StartValue:  int64(startValue),
							MaxValue:    0,
						}

						_, err = tx.ExecContext(ctx, "SET SESSION auto_increment_offset="+strconv.FormatInt(seq.StartValue, 10))
						if !assert.Nil(t, err, testCase.description) {
							onDone(err)
							continue outer
						}

						_, err = tx.ExecContext(ctx, "SET SESSION auto_increment_increment="+strconv.FormatInt(seq.IncrementBy, 10))
						if !assert.Nil(t, err, testCase.description) {
							onDone(err)
							continue outer
						}

						_, err = tx.ExecContext(ctx, "ALTER TABLE "+testCase.table+" AUTO_INCREMENT = "+strconv.Itoa(int(seq.Value)))
						if !assert.Nil(t, err, testCase.description) {
							onDone(err)
							continue outer
						}

						for i := int64(1); i <= recordCnt; i++ {
							_, err = tx.ExecContext(ctx, testCase.insertSQL)
							if !assert.Nil(t, err, testCase.description) {
								onDone(err)
								continue outer
							}
						}

						_, err = tx.ExecContext(ctx, "ANALYZE TABLE "+testCase.table)
						assert.Nil(t, err, testCase.description)

						SQL := "SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + dsnSchema + "' AND TABLE_NAME = '" + testCase.table + "'"
						rows, err := tx.QueryContext(ctx, SQL)
						assert.Nil(t, err, testCase.description)
						var onDone2 = func(error) { rows.Close() } //TODO what if defer throws err?
						if rows.Next() {
							err = rows.Scan(&testCase.expected)
							if !assert.Nil(t, err, testCase.description) {
								onDone2(err)
								onDone(err)
								continue outer
							}
						}

						err = rows.Err()
						if !assert.Nil(t, err, testCase.description) {
							onDone2(err)
							onDone(err)
							continue outer
						}

						// TEST
						actual := seq.NextValue(recordCnt)

						testCase.hasError = false

						// Specific MySQL problems
						if seq.StartValue > seq.IncrementBy {
							testCase.hasError = true
						}

						if testCase.hasError {
							assert.NotEqualValues(t, testCase.expected, actual, testCase.description)
						} else {
							assert.EqualValues(t, testCase.expected, actual, testCase.description)
						}

						onDone2(err)
						onDone(err)

					}
				}
			}
		}
	}
}
