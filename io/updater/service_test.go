package updater_test

import (
	"context"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/io/updater"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/option"
	"testing"
)

func TestService_Exec(t *testing.T) {

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
			description: "Service.Builder ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t1",
			initSQL: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, bar INTEGER)",
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
		writer, err := updater.New(context.TODO(), db, testCase.table, testCase.options...)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		affected, err := writer.Exec(context.TODO(), testCase.records)
		assert.Nil(t, err, testCase.description)
		assert.EqualValues(t, testCase.affected, affected, testCase.description)
	}

}
