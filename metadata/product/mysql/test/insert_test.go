package mysql_test

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/option"
	"os"
	"testing"
)

/* Warning!
   MySQL - 5.7.41 has a bug that causes wrong autoincrement value set
   Added buggedVersions to testcase struct to avoid failing some tests.
   Script below allows to reproduce this bug.

drop table if exists t12;
create table t12 ( foo_id integer auto_increment primary key, bar integer);

insert into t12(foo_id,bar)
select x.* from(
select 7 a,7 b union
select 0 a,8 b union
select 0 a,9 b) x;

show create table t12;

insert into t12(foo_id,bar)
select 0 a,10 b;

select * from t12;
-- foo_id == 11 <> 10
*/

func TestService_Exec_Mysql(t *testing.T) {
	driver := "mysql"
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN before running test")
	}

	type entityWithAutoIncrement struct {
		Id   int    `sqlx:"name=foo_id,generator=autoincrement"`
		Desc string `sqlx:"-"`
		Bar  float64
	}

	var useCases = []struct {
		description    string
		table          string
		options        []option.Option
		records        interface{}
		params         []interface{}
		expect         interface{}
		initSQL        []string
		affected       int64
		lastID         int64
		buggedVersions []string
	}{
		{
			description: "1. Insert 1 row into empty table with: batch size 1, resetIDWithTransientTransaction strategy",
			table:       "t12",
			initSQL: []string{
				"DROP TABLE IF EXISTS t12",
				"CREATE TABLE t12 ( foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, bar INTEGER)",
			},
			records: []*entityWithAutoIncrement{
				{Id: 0, Bar: 1},
			},
			affected: 1,
			lastID:   1,
			options: []option.Option{
				option.BatchSize(1),
				dialect.PresetIDWithTransientTransaction,
			},
		},
		{
			description: "2. Insert 1 row into empty table with: batch size 1, PresetIDWithTransientTransaction strategy and non zero-value ID",
			table:       "t12",
			initSQL: []string{
				"DROP TABLE IF EXISTS t12",
				"CREATE TABLE t12 ( foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, bar INTEGER)",
			},
			records: []*entityWithAutoIncrement{
				{Id: 1, Bar: 1},
			},
			affected: 1,
			lastID:   1,
			options: []option.Option{
				option.BatchSize(1),
				dialect.PresetIDWithTransientTransaction,
			},
		},
		{
			description: "3. Insert 2 rows into empty table with: batch size 2, PresetIDWithTransientTransaction strategy",
			table:       "t12",
			initSQL: []string{
				"DROP TABLE IF EXISTS t12",
				"CREATE TABLE t12 ( foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, bar INTEGER)",
			},
			records: []*entityWithAutoIncrement{
				{Id: 0, Desc: "description1", Bar: 1},
				{Id: 0, Desc: "description2", Bar: 2},
			},
			affected: 2,
			lastID:   2,
			options: []option.Option{
				option.BatchSize(2),
				dialect.PresetIDWithTransientTransaction,
			},
		},
		{
			description: "4. Insert 3 rows into empty table with: batch size 2, PresetIDWithTransientTransaction strategy",
			table:       "t12",
			initSQL: []string{
				"DROP TABLE IF EXISTS t12",
				"CREATE TABLE t12 ( foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, bar INTEGER)",
			},
			records: []*entityWithAutoIncrement{
				{Id: 0, Bar: 17},
				{Id: 0, Bar: 18},
				{Id: 0, Bar: 19},
			},
			affected: 3,
			lastID:   3,
			options: []option.Option{
				option.BatchSize(2),
				dialect.PresetIDWithTransientTransaction,
			},
		},
		{
			description: "5. Insert 8 rows into empty table with: batch size 3, PresetIDWithTransientTransaction strategy",
			table:       "t12",
			initSQL: []string{
				"DROP TABLE IF EXISTS t12",
				"CREATE TABLE t12 ( foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, bar INTEGER)",
			},
			records: []*entityWithAutoIncrement{
				{Id: 0, Bar: 1},
				{Id: 0, Bar: 2},
				{Id: 0, Bar: 3},
				{Id: 0, Bar: 4},
				{Id: 0, Bar: 5},
				{Id: 0, Bar: 6},
				{Id: 0, Bar: 7},
				{Id: 0, Bar: 8},
			},
			affected: 8,
			lastID:   8,
			options: []option.Option{
				option.BatchSize(3),
				dialect.PresetIDWithTransientTransaction,
			},
		},
		{
			description: "6. Insert 3 rows into non-empty table with: batch size 2, PresetIDWithTransientTransaction strategy",
			table:       "t12",
			initSQL: []string{
				"DROP TABLE IF EXISTS t12",
				"CREATE TABLE t12 ( foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, bar INTEGER)",
				`INSERT INTO t12 (bar, foo_id) VALUES (1,1)`,
				`INSERT INTO t12 (bar, foo_id) VALUES (2,2)`,
				`INSERT INTO t12 (bar, foo_id) VALUES (3,3)`,
			},
			records: []*entityWithAutoIncrement{
				{Id: 0, Bar: 4},
				{Id: 0, Bar: 5},
				{Id: 0, Bar: 6},
			},
			affected: 3,
			lastID:   6,
			options: []option.Option{
				option.BatchSize(2),
				dialect.PresetIDWithTransientTransaction,
			},
		},
		{
			description: "7. Insert 4 rows, (all rows with non-zero id value) into empty table with: batch size 3, PresetIDWithTransientTransaction strategy",
			table:       "t12",
			initSQL: []string{
				"DROP TABLE IF EXISTS t12",
				"CREATE TABLE t12 ( foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, bar INTEGER)",
			},
			records: []*entityWithAutoIncrement{
				{Id: 7, Bar: 7},
				{Id: 8, Bar: 8},
				{Id: 9, Bar: 9},
				{Id: 20, Bar: 20},
			},
			affected: 4,
			lastID:   20,
			options: []option.Option{
				option.BatchSize(3),
				dialect.PresetIDWithTransientTransaction,
			},
		},
		{
			description: "8. Insert 4 rows, (first row with zero value id) into empty table with: batch size 3, PresetIDWithTransientTransaction strategy",
			table:       "t12",
			initSQL: []string{
				"DROP TABLE IF EXISTS t12",
				"CREATE TABLE t12 ( foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, bar INTEGER)",
			},
			records: []*entityWithAutoIncrement{
				{Id: 0, Bar: 1},
				{Id: 91, Bar: 2},
				{Id: 92, Bar: 3},
				{Id: 93, Bar: 4},
			},
			affected: 4,
			lastID:   4,
			options: []option.Option{
				option.BatchSize(3),
				dialect.PresetIDWithTransientTransaction,
			},
		},
		{ // doesn't work properly with MySQL - 5.7.41
			description: "9. Insert 4 rows, (first row with non-zero value) into empty table with: batch size 3, PresetIDWithTransientTransaction strategy",
			table:       "t12",
			initSQL: []string{
				"DROP TABLE IF EXISTS t12",
				"CREATE TABLE t12 ( foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, bar INTEGER)",
			},
			records: []*entityWithAutoIncrement{
				{Id: 7, Bar: 7},
				{Id: 0, Bar: 8},
				{Id: 0, Bar: 9},
				{Id: 0, Bar: 10},
			},
			affected: 4,
			lastID:   10,
			options: []option.Option{
				option.BatchSize(3),
				dialect.PresetIDWithTransientTransaction,
			},
			buggedVersions: []string{"MySQL - 5.7.41"},
		},
	}

outer:
	for _, testCase := range useCases {
		var db *sql.DB

		db, err := sql.Open(driver, dsn)
		tx, err := db.Begin()
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		for _, SQL := range testCase.initSQL {
			_, err := tx.Exec(SQL)
			if !assert.Nil(t, err, testCase.description) {
				continue outer
			}
		}
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		inserter, err := insert.New(context.TODO(), db, testCase.table, testCase.options...)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		testCase.options = append(testCase.options, tx)
		affected, lastID, err := inserter.Exec(context.TODO(), testCase.records, testCase.options...)
		assert.Nil(t, err, testCase.description)
		assert.EqualValues(t, testCase.affected, affected, testCase.description)

		result := []string{}
		meta := metadata.New()
		err = meta.Info(context.TODO(), db, info.KindVersion, &result)
		if !assert.Nil(t, err, testCase.description) {
			return
		}

		version := result[0]
		isBugged := false
		for _, ver := range testCase.buggedVersions {
			if ver == version {
				isBugged = true
			}
		}

		if !isBugged {
			assert.EqualValues(t, testCase.lastID, lastID, testCase.description)
		}
	}
}

func TestService_Exec_Mysql_Global_Offset(t *testing.T) {
	driver := "mysql"
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN before running test")
	}

	type entityWithAutoIncrement struct {
		Id   int    `sqlx:"name=foo_id,generator=autoincrement"`
		Desc string `sqlx:"-"`
		Bar  float64
	}

	var useCases = []struct {
		description    string
		table          string
		options        []option.Option
		records        interface{}
		params         []interface{}
		expect         interface{}
		initSQL        []string
		affected       int64
		lastID         int64
		buggedVersions []string
	}{
		{
			description: "1. Insert 3 rows into empty table with: batch size 2, PresetIDWithTransientTransaction strategy, offset = 5, incrementBy = 10 ",
			table:       "t12",
			initSQL: []string{
				"DROP TABLE IF EXISTS t12",
				"CREATE TABLE t12 (foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, bar INTEGER)",
				"SET GLOBAL auto_increment_offset=5",     // In the past it was possible to use SET SESSION and pass tx (see next_sequence_test)
				"SET GLOBAL auto_increment_increment=10", // In the past it was possible to use SET SESSION and pass tx (see next_sequence_test)
			},
			records: []*entityWithAutoIncrement{
				{Id: 0, Bar: 17},
				{Id: 0, Bar: 18},
				{Id: 0, Bar: 19},
			},
			affected: 3,
			lastID:   25,
			options: []option.Option{
				option.BatchSize(2),
				dialect.PresetIDWithTransientTransaction,
			},
		},
		{
			description: "2. Insert rows into empty table with: ession-dialect DefaultPresetIDStrategy strategy, offset = 5, incrementBy = 10 ",
			table:       "t12",
			initSQL: []string{
				"DROP TABLE IF EXISTS t12",
				"CREATE TABLE t12 (foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, bar INTEGER)",
				"SET GLOBAL auto_increment_offset=5",     // In the past it was possible to use SET SESSION and pass tx (see next_sequence_test)
				"SET GLOBAL auto_increment_increment=10", // In the past it was possible to use SET SESSION and pass tx (see next_sequence_test)
			},
			records: []*entityWithAutoIncrement{
				{Id: 0, Bar: 17},
				{Id: 0, Bar: 18},
				{Id: 0, Bar: 19},
			},
			affected: 3,
			lastID:   25,
			options: []option.Option{
				option.BatchSize(2),
				//dialect.PresetIDWithTransientTransaction, // don't pass strategy by Options, use Dialect.DefaultPresetIDStrategy
			},
		},
	}

	defer setDefault(t, driver, dsn)

outer:
	for _, testCase := range useCases {
		//ctx := context.Background()
		var db *sql.DB

		db, err := sql.Open(driver, dsn)
		tx, err := db.Begin()
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		for _, SQL := range testCase.initSQL {
			_, err := tx.Exec(SQL)
			if !assert.Nil(t, err, testCase.description) {
				continue outer
			}
		}
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		insert, err := insert.New(context.TODO(), db, testCase.table, testCase.options...)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		testCase.options = append(testCase.options, tx)
		affected, lastID, err := insert.Exec(context.TODO(), testCase.records, testCase.options...)
		assert.Nil(t, err, testCase.description)
		assert.EqualValues(t, testCase.affected, affected, testCase.description)
		assert.EqualValues(t, testCase.lastID, lastID, testCase.description)
	}
}

func setDefault(t *testing.T, driver string, dsn string) {
	db, err := sql.Open(driver, dsn)
	assert.Nil(t, err, setDefault)

	initSQL := []string{
		"SET GLOBAL auto_increment_offset=1",
		"SET GLOBAL auto_increment_increment=1",
	}

	for _, SQL := range initSQL {
		_, err := db.Exec(SQL)
		assert.Nil(t, err, setDefault)
	}
}
