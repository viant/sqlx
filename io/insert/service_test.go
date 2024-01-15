package insert_test

import (
	"context"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/option"
	"testing"
)

func TestService_Exec(t *testing.T) {

	type entity struct {
		ID   int    `sqlx:"name=foo_id,primaryKey=true,generator=autoincrement"`
		Name string `sqlx:"foo_name"`
		Desc string `sqlx:"-"`
		Bar  float64
	}
	type entityWithAutoIncrement struct {
		ID   int    `sqlx:"name=foo_id,generator=autoincrement"`
		Name string `sqlx:"foo_name"`
		Desc string `sqlx:"-"`
		Bar  float64
	}

	type entityWithStringID struct {
		ID   string `sqlx:"name=foo_id"`
		Name string `sqlx:"foo_name"`
		Desc string `sqlx:"-"`
		Bar  float64
	}

	type Audit struct {
		CreatedAt string `sqlx:"name=created_at"`
		UpdatedAt string `sqlx:"name=updated_at"`
	}

	type EntityValue struct {
		Bar  float64
		Name string `sqlx:"foo_name"`
	}

	type entityWithCompositeColumns struct {
		ID string `sqlx:"name=foo_id"`
		Audit
		EntityValue
		Desc string `sqlx:"-"`
	}

	var useCases = []struct {
		description string
		table       string
		driver      string
		dsn         string
		options     []option.Option
		records     interface{}
		params      []interface{}
		expect      interface{}
		initSQL     []string
		affected    int64
		lastID      int64
	}{
		{
			description: "Service.Builder ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t1",
			initSQL: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, bar INTEGER)",
			},
			records: []interface{}{
				&entity{ID: 1, Name: "John1", Desc: "description", Bar: 17},
				&entity{ID: 2, Name: "John2", Desc: "description", Bar: 18},
				&entity{ID: 3, Name: "John3", Desc: "description", Bar: 19},
			},
			affected: 3,
			lastID:   3,
		},
		{
			description: "Service.Builder: batchSize size: 2 ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t2",
			initSQL: []string{
				"DROP TABLE IF EXISTS t2",
				"CREATE TABLE t2 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, Bar INTEGER)",
			},
			records: []entity{
				{ID: 10, Name: "John1", Desc: "description", Bar: 17},
				{ID: 11, Name: "John2", Desc: "description", Bar: 18},
				{ID: 12, Name: "John3", Desc: "description", Bar: 19},
			},
			affected: 3,
			lastID:   12,
			options: []option.Option{
				option.BatchSize(2),
			},
		},
		{
			description: "Service.Builder - autoincrement batchSize - empty table ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t3",
			initSQL: []string{
				"DROP TABLE IF EXISTS t3",
				"CREATE TABLE t3 (foo_id INTEGER PRIMARY KEY AUTOINCREMENT, foo_name TEXT, Bar INTEGER)",
			},
			records: []*entityWithAutoIncrement{
				{ID: 0, Name: "John1", Desc: "description", Bar: 17},
				{ID: 0, Name: "John2", Desc: "description", Bar: 18},
				{ID: 0, Name: "John3", Desc: "description", Bar: 19},
			},
			affected: 3,
			lastID:   3,
			options: []option.Option{
				option.BatchSize(2),
			},
		},
		{
			description: "Service.Builder - autoincrement batchSize - existing data",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t4",
			initSQL: []string{
				"DROP TABLE IF EXISTS t4",
				"CREATE TABLE t4 (foo_id INTEGER PRIMARY KEY AUTOINCREMENT, foo_name TEXT, Bar INTEGER)",
				"INSERT INTO t4(foo_name) VALUES('test')",
			},
			records: []*entityWithAutoIncrement{
				{ID: 0, Name: "John1", Desc: "description", Bar: 17},
				{ID: 0, Name: "John2", Desc: "description", Bar: 18},
				{ID: 0, Name: "John3", Desc: "description", Bar: 19},
				{ID: 0, Name: "John4", Desc: "description", Bar: 19},
				{ID: 0, Name: "John5", Desc: "description", Bar: 19},
			},
			affected: 5,
			lastID:   6,
			options: []option.Option{
				option.BatchSize(3),
			},
		},
		{
			description: "Service.Builder - autoincrement batchSize - empty table ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t3",
			initSQL: []string{
				"DROP TABLE IF EXISTS t3",
				"CREATE TABLE t3 (foo_id INTEGER PRIMARY KEY AUTOINCREMENT, foo_name TEXT, Bar INTEGER)",
			},
			records: []*entityWithAutoIncrement{
				{ID: 0, Name: "John1", Desc: "description", Bar: 17},
				{ID: 0, Name: "John2", Desc: "description", Bar: 18},
				{ID: 0, Name: "John3", Desc: "description", Bar: 19},
			},
			affected: 3,
			lastID:   3,
			options: []option.Option{
				option.BatchSize(2),
				dialect.PresetIDWithMax,
			},
		},
		{
			description: "String ID ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t3",
			initSQL: []string{
				"DROP TABLE IF EXISTS t3",
				"CREATE TABLE t3 (foo_id VARCHAR(5) PRIMARY KEY, foo_name TEXT, Bar INTEGER)",
			},
			records: []*entityWithStringID{
				{ID: "12345", Name: "John1", Desc: "description", Bar: 17},
				{ID: "12346", Name: "John2", Desc: "description", Bar: 18},
				{ID: "12347", Name: "John3", Desc: "description", Bar: 19},
			},
			affected: 3,
			lastID:   3,
			options: []option.Option{
				option.BatchSize(2),
				dialect.PresetIDWithMax,
			},
		},
		{
			description: "composite objects",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t3",
			initSQL: []string{
				"DROP TABLE IF EXISTS t3",
				`CREATE TABLE t3 (
foo_id VARCHAR(5) PRIMARY KEY, 
foo_name TEXT, 
created_at TEXT,
updated_at TEXT,
Bar INTEGER
                )`,
			},
			records: []*entityWithCompositeColumns{
				{
					ID: "12345", EntityValue: EntityValue{
						Name: "John1",
						Bar:  17,
					},
					Desc: "description",
					Audit: Audit{
						CreatedAt: "time.Now()",
						UpdatedAt: "time.Now()",
					},
				},
				{
					ID: "12346",
					EntityValue: EntityValue{
						Name: "John2",
						Bar:  18,
					},
					Desc: "description",
				},
				{
					ID: "12347",
					EntityValue: EntityValue{
						Name: "John3",
						Bar:  19,
					}, Desc: "description",
				},
			},
			affected: 3,
			lastID:   3,
			options: []option.Option{
				option.BatchSize(2),
				dialect.PresetIDWithMax,
			},
		},
	}

outer:

	for i, testCase := range useCases {
		i = i
		//fmt.Printf("TESTCASE %d: %s\n", i, testCase.description)

		var db *sql.DB

		db, err := sql.Open(testCase.driver, testCase.dsn)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		for _, SQL := range testCase.initSQL {
			_, err := db.Exec(SQL)
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
		affected, lastID, err := inserter.Exec(context.TODO(), testCase.records, testCase.options...)
		assert.Nil(t, err, testCase.description)
		assert.EqualValues(t, testCase.affected, affected, testCase.description)
		assert.EqualValues(t, testCase.lastID, lastID, testCase.description)

	}

}
