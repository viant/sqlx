package mysql_test

import (
	"context"
	"database/sql"
	_ "embed"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"os"
	"testing"
)

// Warning!
// 1. Until we don't use autoincrement (by insert with 0 value id), "show create table" and "information_schema.tables"
// show wrong autoincrement value if (GLOBAL|SESSION) auto_increment_increment > 1
//
// 2. Problems with offset > increment_by - see test sqlx/metadata/product/mysql/test/sequence_test.go
func TestService_NextSequenceValue(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN before running test")
	}

	dsnSchema := os.Getenv("TEST_MYSQL_DSN_SCHEMA")
	if dsnSchema == "" {
		t.Skip("set TEST_MYSQL_DSN_SCHEMA before running test")
	}

	var useCases = []struct {
		description string
		initSQL     []string
		options     []option.Option
		expect      *sink.Sequence
	}{
		{
			description: "01. info.KindSequenceNextValue with PresetIDWithTransientTransaction strategy",
			initSQL: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1(foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, foo_name TEXT, bar INTEGER)",
				"SET SESSION auto_increment_offset=5",
				"SET  SESSION auto_increment_increment=10",
			},
			options: option.Options{
				option.NewArgs("", dsnSchema, "t1"),
				dialect.PresetIDWithTransientTransaction,
				dmlBuilder(1, &sqlx.SQL{
					Query: `INSERT INTO t1 (foo_name, bar, foo_id) VALUES (?,?,?)`,
					Args:  []interface{}{"John", 20, 0},
				}),
				option.RecordCount(1),
			},
			expect: &sink.Sequence{
				Catalog:     "",
				Schema:      dsnSchema,
				Name:        "t1",
				Value:       15,
				IncrementBy: 10,
				DataType:    "int",
				StartValue:  5,
				MaxValue:    9223372036854775807,
			},
		},
		{
			description: "02. info.KindSequenceNextValue with PresetIDWithTransientTransaction strategy",
			initSQL: []string{
				createSequenceTable,
				"DROP PROCEDURE IF EXISTS SET_AUTO_INCREMENT_WITH_INNER_TX",
				createProcedure,
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1 (foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, foo_name TEXT, bar INTEGER)",
				"SET SESSION auto_increment_offset=5",
				"SET  SESSION auto_increment_increment=10",
				"DELETE FROM FOO_SEQUENCES WHERE FULL_TABLE_NAME = '" + dsnSchema + ".t1'",
				"INSERT INTO FOO_SEQUENCES (FULL_TABLE_NAME, HOST, SCHEMA_NAME, TABLE_NAME, OFFSET, INCREMENT_BY, AUTO_INCREMENT) " +
					"VALUES ('" + dsnSchema + ".t1', 'TODO HOST TODO', '" + dsnSchema + "', 't1', @@SESSION.AUTO_INCREMENT_OFFSET, @@SESSION.AUTO_INCREMENT_INCREMENT, @@SESSION.AUTO_INCREMENT_OFFSET)",
				"INSERT INTO t1 (foo_name) VALUES ('x')", // insert into table t1 required for incrementing next value
				// first insert into t1 requires 0-value id to initialize properly offset = 5 and increment = 10 (MySQL bug?)
			},
			options: option.Options{
				option.NewArgs("", dsnSchema, "t1"),
				dialect.PresetIDWithUDFSequence,
				dmlBuilder(1, &sqlx.SQL{
					Query: `INSERT INTO t1 (foo_name, bar, foo_id) VALUES (?,?,?)`,
					Args:  []interface{}{"John", 20, 0},
				}),
				option.RecordCount(1),
			},
			expect: &sink.Sequence{
				Catalog:     "",
				Schema:      dsnSchema,
				Name:        "t1",
				Value:       15,
				IncrementBy: 10,
				DataType:    "int",
				StartValue:  5,
				MaxValue:    9223372036854775807,
			},
		},
	}

	meta := metadata.New()
	db, err := sql.Open("mysql", dsn)
	ctx := context.Background()

	for _, testCase := range useCases {

		//fmt.Printf("=====> TEST %d: %s\n", i, testCase.description)

		func() {
			if !assert.Nil(t, err, testCase.description) {
				return
			}

			for _, SQL := range testCase.initSQL {
				_, err := db.Exec(SQL)
				if !assert.Nil(t, err, testCase.description) {
					return
				}
			}
			options := testCase.options

			nextSequence := &sink.Sequence{}
			err = meta.Info(ctx, db, info.KindSequenceNextValue, nextSequence, options...)
			if !assert.Nil(t, err, testCase.description) {
				return
			}

			nextSequence.DataType = testCase.expect.DataType // we don't care
			assert.EqualValues(t, testCase.expect, nextSequence, testCase.description)

			aSequence := sink.Sequence{}
			err = meta.Info(ctx, db, info.KindSequences, &aSequence, options...)
			if !assert.Nil(t, err, testCase.description) {
				return
			}

			aSequence.DataType = nextSequence.DataType // we don't care
			assert.EqualValues(t, &aSequence, nextSequence, testCase.description)
		}()
	}
}

func dmlBuilder(recordCount int64, sql *sqlx.SQL) func(sequence *sink.Sequence) (*sqlx.SQL, error) {
	return func(sequence *sink.Sequence) (*sqlx.SQL, error) {
		sequence.Value = sequence.NextValue(recordCount)
		sql.Args[len(sql.Args)-1] = sequence.Value - 1 // "-1" or(?) sequence.IncrementBy because transient insert
		return sql, nil
	}
}

//go:embed schema/foo.sql
var createSequenceTable string

//go:embed schema/udf.sql
var createProcedure string
