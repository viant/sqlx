package update_test

import (
	"context"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/io/update"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/option"
	"testing"
)

func TestService_Exec(t *testing.T) {

	type recordPresence struct {
		Id   bool
		Name bool
		Desc bool
	}

	type record struct {
		Id   int             `sqlx:"name=foo_id,primaryKey=true,generator=autoincrement"`
		Name string          `sqlx:"foo_name"`
		Desc string          `sqlx:"desc"`
		Has  *recordPresence `sqlx:"presence=true"`
	}

	type entity struct {
		Id   int    `sqlx:"name=foo_id,primaryKey=true,generator=autoincrement"`
		Name string `sqlx:"foo_name"`
		Desc string `sqlx:"-"`
		Bar  float64
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
	}{
		{
			description: "Update all ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t1",
			initSQL: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, bar DECIMAL)",
				"INSERT INTO t1 (foo_id) VALUES(1)",
				"INSERT INTO t1 (foo_id) VALUES(2)",
				"INSERT INTO t1 (foo_id) VALUES(3)",
			},
			records: []interface{}{
				&entity{Id: 1, Name: "John1", Desc: "description", Bar: 17},
				&entity{Id: 2, Name: "John2", Desc: "description", Bar: 18},
				&entity{Id: 3, Name: "John3", Desc: "description", Bar: 19},
			},
			affected: 3,
		},

		{
			description: "Update selective fields",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t2",
			initSQL: []string{
				"DROP TABLE IF EXISTS t2",
				"CREATE TABLE t2 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT)",
				"INSERT INTO t2 (foo_id) VALUES(1)",
				"INSERT INTO t2 (foo_id, foo_name) VALUES(2, 'John2')",
				"INSERT INTO t2 (foo_id) VALUES(3)",
			},
			records: []interface{}{
				&record{Id: 1, Name: "John1", Desc: "test 1", Has: &recordPresence{
					Id:   true,
					Name: false,
					Desc: true,
				}},
				&record{Id: 2, Name: "John 2", Has: &recordPresence{
					Id:   true,
					Name: true,
					Desc: false,
				}},
				&record{Id: 3, Name: "John3", Has: &recordPresence{
					Id:   false,
					Name: false,
				}},
			},
			affected: 2,
		},
	}

outer:

	for _, testCase := range useCases {

		//ctx := context.Background()
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
		updater, err := update.New(context.TODO(), db, testCase.table, testCase.options...)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		affected, err := updater.Exec(context.TODO(), testCase.records)
		assert.Nil(t, err, testCase.description)
		assert.EqualValues(t, testCase.affected, affected, testCase.description)
	}

}
