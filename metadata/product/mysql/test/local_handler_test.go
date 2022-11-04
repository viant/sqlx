package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/metadata/info"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/product/mysql/sequence"
	sink2 "github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"os"
	"strings"
	"testing"
)

// TODO TEST FOR UPDATE SEQUENCE
// TODO CREATING PROCEDURE
// TODO Udf TABLE NAME FOR SEQUENCES

// Don't set (@SESSION|@GLOBAL).auto_increment_offset > (@SESSION|@GLOBAL).auto_increment_increment
//
// MySQl has problem with proper returning/showing AUTOINCREMENT values when
// (@SESSION|@GLOBAL).auto_increment_offset > (@SESSION|@GLOBAL).auto_increment_increment
// e.g.: for auto_increment_increment = 2 and auto_increment_offset = 3
// AUTOINCREMENT values are shown:
// from real inserts (selected from table): 1 -> 3 -> 5 -> 7
// from SHOW CREATE TABLE: nil -> 2 -> 4 -> 6
// from INFORMATION_SCHEMA.TABLES (with ANALYZE TABLE before): 1 -> 2 -> 4 -> 6
func TestHandler_NextWithUDFTransaction(t *testing.T) {

	driver := "mysql"
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN before running test")
	}
	dsnSchema := os.Getenv("TEST_MYSQL_DSN_SCHEMA")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN_SCHEMA before running test")
	}

	var useCases = []struct {
		description    string
		table          string
		initSQL        []string
		insertSQLQuery string
		postSQL        string
		handler01      info.Handler
		handler02      info.Handler
		sink           *sink2.Sequence
		args           option.Args
		tx             *sql.Tx
		recordCnt      int64
		sequencesTable string
		expected01     *sink2.Sequence
		expected02     *sink2.Sequence
		expectedDoNext bool
		insertStrategy option.PresetIdStrategy
	}{
		{
			description:    "Udf",
			table:          "t1",
			recordCnt:      2,
			args:           option.Args{}, // populated inside test
			handler01:      &sequence.Udf{},
			handler02:      info.NewHandler(sequence.UpdateMySQLSequence),
			sink:           new(sink2.Sequence),
			tx:             nil,
			sequencesTable: "FOO_SEQUENCES",
			initSQL: []string{
				createSequenceTable,
				createProcedure,
				"DROP TABLE IF EXISTS [table]",
				"CREATE TABLE [table] (foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, foo_name TEXT)",
				`DELETE FROM FOO_SEQUENCES WHERE FULL_TABLE_NAME = '[dsnSchema].[table]'`,
				`INSERT INTO FOO_SEQUENCES (FULL_TABLE_NAME, HOST, SCHEMA_NAME, TABLE_NAME, OFFSET, INCREMENT_BY, AUTO_INCREMENT)
				VALUES ('[dsnSchema].[table]', 'test_host', '[dsnSchema]', '[table]', @@GLOBAL.AUTO_INCREMENT_OFFSET, @@GLOBAL.AUTO_INCREMENT_INCREMENT, @@GLOBAL.AUTO_INCREMENT_OFFSET)`,
				// TODO 'test_host'-----------------/|\
			},
			insertSQLQuery: "INSERT INTO [table] (foo_id, foo_name) VALUES (?,?)",
			postSQL:        `INSERT INTO [table] (foo_name) values ("foo")`,
			expected01:     &sink2.Sequence{}, // populated inside test
			expected02:     &sink2.Sequence{}, // populated inside test
			expectedDoNext: false,
			insertStrategy: option.PresetIdWithUDFSequence,
		},
	}
outer:

	for _, testCase := range useCases {
		ctx := context.Background()
		db, err := sql.Open(driver, dsn)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		// Init SQL
		for _, SQL := range testCase.initSQL {
			SQL = strings.ReplaceAll(SQL, `[dsnSchema]`, dsnSchema)
			SQL = strings.ReplaceAll(SQL, `[table]`, testCase.table)
			_, err := db.Exec(SQL)
			if !assert.Nil(t, err, testCase.description) {
				continue outer
			}
		}

		// Populate args
		testCase.args = *option.NewArgs("", dsnSchema, testCase.table)

		// Populate expected01
		row := db.QueryRow("SELECT @@GLOBAL.auto_increment_increment, @@GLOBAL.auto_increment_offset")
		var incrementBy, offset int64
		err = row.Scan(&incrementBy, &offset)
		if err != nil {
			assert.Nil(t, err, testCase.description)
			continue
		}
		if offset > incrementBy {
			err = fmt.Errorf("unable to handle case when @@GLOBAL.auto_increment_offset > @@GLOBAL.auto_increment_increment (%d > %d)", offset, incrementBy)
			assert.Nil(t, err, testCase.description)
			continue
		}

		testCase.expected01.Schema = dsnSchema
		testCase.expected01.Name = testCase.table
		testCase.expected01.Value = offset + incrementBy*testCase.recordCnt
		testCase.expected01.IncrementBy = incrementBy
		testCase.expected01.StartValue = offset
		testCase.expected01.DataType = "int"

		// Test A
		actualDoNext, err := testCase.handler01.Handle(ctx, db, testCase.sink, &testCase.args, testCase.tx, testCase.insertStrategy, option.RecordCount(testCase.recordCnt))
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		actualSink := testCase.sink
		assert.EqualValues(t, testCase.expected01, actualSink, testCase.description)
		assert.EqualValues(t, testCase.expectedDoNext, actualDoNext, testCase.description)

		// Test B - check sequence in db
		// postSQL
		testCase.postSQL = strings.ReplaceAll(testCase.postSQL, `[table]`, testCase.table)
		for j := int64(0); j < testCase.recordCnt; j++ {
			_, err := db.Exec(testCase.postSQL)
			if !assert.Nil(t, err, testCase.description) {
				continue outer
			}
		}

		sinks := &[]sink2.Sequence{*new(sink2.Sequence)}
		testCase.expected02.Schema = dsnSchema
		testCase.expected02.Name = testCase.table
		testCase.expected02.DataType = "int"
		(*sinks)[0] = *testCase.expected02
		actualDoNext, err = testCase.handler02.Handle(ctx, db, sinks, &testCase.args, testCase.tx, testCase.insertStrategy)
		assert.Nil(t, err, testCase.description)
		assert.EqualValues(t, 1, len(*sinks), testCase.description)
		actualSink2 := &(*sinks)[0]
		assert.EqualValues(t, testCase.expected01, actualSink2, testCase.description)
	}

}

func TestHandler_NextWithTransientTransaction(t *testing.T) {
	driver := "mysql"
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN before running test")
	}
	dsnSchema := os.Getenv("TEST_MYSQL_DSN_SCHEMA")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN_SCHEMA before running test")
	}

	var useCases = []struct {
		description        string
		table              string
		initSQL            []string
		handler01          info.Handler
		handler02          info.Handler
		sink               *sink2.Sequence
		args               option.Args
		tx                 *sql.Tx
		sqlxSQL            *sqlx.SQL
		expectedSeqNextVal int64
		expectedDoNext     bool
		insertStrategy     option.PresetIdStrategy
	}{
		{
			description: "Max",
			table:       "t1",
			args:        option.Args{}, // populated inside test
			handler01:   &sequence.Transient{},
			handler02:   info.NewHandler(sequence.UpdateMySQLSequence),
			sink:        &sink2.Sequence{},
			tx:          nil,
			sqlxSQL: &sqlx.SQL{
				Query: `INSERT INTO [table] (foo_name, foo_id) VALUES (?,?)`,
				Args:  []interface{}{"foo"}, // populated inside test
			},
			initSQL: []string{
				"DROP TABLE IF EXISTS [table]",
				"CREATE TABLE [table] (foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, foo_name TEXT)"},
			expectedSeqNextVal: 77,
			expectedDoNext:     false,
			insertStrategy:     option.PresetIdWithTransientTransaction,
		},
	}
outer:

	for _, testCase := range useCases {
		ctx := context.Background()
		db, err := sql.Open(driver, dsn)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		// Check if @@GLOBAL.auto_increment_offset > @@GLOBAL.auto_increment_increment
		row := db.QueryRow("SELECT @@GLOBAL.auto_increment_increment, @@GLOBAL.auto_increment_offset")
		var incrementBy, offset int64
		err = row.Scan(&incrementBy, &offset)
		if !assert.Nil(t, err, testCase.description) {
			continue outer
		}
		err = row.Err()
		if !assert.Nil(t, err, testCase.description) {
			continue outer
		}

		if offset > incrementBy {
			err = fmt.Errorf("unable to handle case when @@GLOBAL.auto_increment_offset > @@GLOBAL.auto_increment_increment (%d > %d)", offset, incrementBy)
			assert.Nil(t, err, testCase.description)
			continue
		}

		// Init SQL
		for _, SQL := range testCase.initSQL {
			SQL = strings.ReplaceAll(SQL, `[dsnSchema]`, dsnSchema)
			SQL = strings.ReplaceAll(SQL, `[table]`, testCase.table)
			_, err := db.Exec(SQL)
			if !assert.Nil(t, err, testCase.description) {
				continue outer
			}
		}

		// Populate sqlxSQL.Query
		testCase.sqlxSQL.Query = strings.ReplaceAll(testCase.sqlxSQL.Query, `[table]`, testCase.table)
		testCase.sqlxSQL.Args = append(testCase.sqlxSQL.Args, ptrInt64(testCase.expectedSeqNextVal-1)) // -1 because of internal insert

		// Populate args
		testCase.args = *option.NewArgs("", dsnSchema, testCase.table)

		fn := func(sequence *sink2.Sequence) (*sqlx.SQL, error) {
			return testCase.sqlxSQL, nil
		}

		// Test A
		actualDoNext, err := testCase.handler01.Handle(ctx, db, testCase.sink, &testCase.args, testCase.tx, fn, testCase.insertStrategy)
		assert.Nil(t, err, testCase.description)
		assert.EqualValues(t, testCase.expectedDoNext, actualDoNext, testCase.description)

		// Test B
		sink := []sink2.Sequence{*new(sink2.Sequence)}

		actualDoNext, err = testCase.handler02.Handle(ctx, db, &sink, &testCase.args, testCase.tx, testCase.sqlxSQL, testCase.insertStrategy)
		assert.Nil(t, err, testCase.description)
		assert.EqualValues(t, 1, len(sink), testCase.description)

		actualSeqNextVal := sink[0].Value
		assert.EqualValues(t, testCase.expectedSeqNextVal, actualSeqNextVal, testCase.description)
	}
}

func BenchmarkHandler_NextWithUDFTransaction(b *testing.B) {
	driver := "mysql"
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		b.Skip("set TEST_MYSQL_DSN before running test")
	}
	dsnSchema := os.Getenv("TEST_MYSQL_DSN_SCHEMA")
	if dsn == "" {
		b.Skip("set TEST_MYSQL_DSN_SCHEMA before running test")
	}

	var useCases = []struct {
		description    string
		table          string
		initSQL        []string
		handler        info.Handler
		sink           *sink2.Sequence
		args           option.Args
		tx             *sql.Tx
		sqlxSQL        *sqlx.SQL
		recordCnt      int64
		sequencesTable string
		expected       sink2.Sequence
		expectedDoNext bool
		insertStrategy option.PresetIdStrategy
	}{
		{
			description:    "Udf",
			table:          "t1",
			recordCnt:      2,
			args:           option.Args{}, // populated inside test
			handler:        &sequence.Udf{},
			sink:           new(sink2.Sequence),
			tx:             nil,
			sqlxSQL:        nil,
			sequencesTable: "FOO_SEQUENCES",
			initSQL: []string{
				createSequenceTable,
				//createProcedure,
				`DELETE FROM FOO_SEQUENCES WHERE FULL_TABLE_NAME = '[dsnSchema].[table]'`,
				`INSERT INTO FOO_SEQUENCES (FULL_TABLE_NAME, HOST, SCHEMA_NAME, TABLE_NAME, OFFSET, INCREMENT_BY, AUTO_INCREMENT)
				VALUES ('[dsnSchema].[table]', 'test_host', '[dns_schema]', '[table]', @@GLOBAL.AUTO_INCREMENT_OFFSET, @@GLOBAL.AUTO_INCREMENT_INCREMENT, @@GLOBAL.AUTO_INCREMENT_OFFSET)`,
			},
			expected: sink2.Sequence{ // populated inside test
				Catalog:     "",
				Schema:      "",
				Name:        "",
				Value:       0,
				IncrementBy: 0,
				DataType:    "",
				StartValue:  0,
				MaxValue:    0,
			},
			expectedDoNext: false,
			insertStrategy: option.PresetIdWithUDFSequence,
		},
	}
outer:
	for _, testCase := range useCases {
		ctx := context.Background()
		db, err := sql.Open(driver, dsn)
		if !assert.Nil(b, err, testCase.description) {
			continue
		}

		// Init SQL
		for _, SQL := range testCase.initSQL {
			SQL = strings.ReplaceAll(SQL, `[dsnSchema]`, dsnSchema)
			SQL = strings.ReplaceAll(SQL, `[table]`, testCase.table)
			_, err := db.Exec(SQL)
			if !assert.Nil(b, err, testCase.description) {
				continue outer
			}
		}

		// Populate args
		testCase.args = *option.NewArgs("", dsnSchema, testCase.table)

		// Benchmark
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := testCase.handler.Handle(ctx, db, testCase.sink, &testCase.args, testCase.tx, testCase.sqlxSQL, testCase.insertStrategy, option.RecordCount(testCase.recordCnt))
			assert.Nil(b, err, testCase.description)
		}

	}
}

func BenchmarkHandler_NextWithTransientTransaction(b *testing.B) {
	driver := "mysql"
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		b.Skip("set TEST_MYSQL_DSN before running test")
	}
	dsnSchema := os.Getenv("TEST_MYSQL_DSN_SCHEMA")
	if dsn == "" {
		b.Skip("set TEST_MYSQL_DSN_SCHEMA before running test")
	}

	var useCases = []struct {
		description        string
		table              string
		options            []option.Option
		initSQL            []string
		handler            info.Handler
		sink               *sink2.Sequence
		args               option.Args
		tx                 *sql.Tx
		sqlxSQL            *sqlx.SQL
		expectedSeqNextVal int64
		expectedDoNext     bool
		insertStrategy     option.PresetIdStrategy
	}{
		{
			description: "Max",
			table:       "t1",
			args:        option.Args{}, // populated inside test
			handler:     &sequence.Transient{},
			sink:        &sink2.Sequence{},
			tx:          nil,
			sqlxSQL: &sqlx.SQL{
				Query: `INSERT INTO [table] (foo_name, foo_id) VALUES (?,?)`,
				Args:  []interface{}{"name test"}, // populated inside test
			},
			initSQL: []string{
				"DROP TABLE IF EXISTS [table]",
				"CREATE TABLE [table] (foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, foo_name TEXT)"},
			expectedSeqNextVal: 77,
			expectedDoNext:     false,
			insertStrategy:     option.PresetIdWithTransientTransaction,
		},
	}
outer:

	for _, testCase := range useCases {
		ctx := context.Background()
		db, err := sql.Open(driver, dsn)
		if !assert.Nil(b, err, testCase.description) {
			continue
		}

		// Init SQL
		for _, SQL := range testCase.initSQL {
			SQL = strings.ReplaceAll(SQL, `[dsnSchema]`, dsnSchema)
			SQL = strings.ReplaceAll(SQL, `[table]`, testCase.table)
			_, err := db.Exec(SQL)
			if !assert.Nil(b, err, testCase.description) {
				continue outer
			}
		}

		// Populate sqlxSQL.Query
		testCase.sqlxSQL.Query = strings.ReplaceAll(testCase.sqlxSQL.Query, `[table]`, testCase.table)
		testCase.sqlxSQL.Args = append(testCase.sqlxSQL.Args, ptrInt64(testCase.expectedSeqNextVal-1)) // -1 because of internal insert

		// Populate args
		testCase.args = *option.NewArgs("", dsnSchema, testCase.table)

		fn := func(sequence *sink2.Sequence) (*sqlx.SQL, error) {
			return testCase.sqlxSQL, nil
		}

		// Benchmark
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := testCase.handler.Handle(ctx, db, testCase.sink, &testCase.args, testCase.tx, fn, testCase.insertStrategy)
			assert.Nil(b, err, testCase.description)
		}
	}
}

func ptrInt64(i int64) *int64 {
	return &i
}
