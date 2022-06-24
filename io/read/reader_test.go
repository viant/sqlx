package read_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/option"
	"log"
	"os"
	"testing"
	"time"
)

func TestReader_ReadAll(t *testing.T) {

	type fooCase1 struct {
		Id   int
		Name string
	}

	type fooCase2 struct {
		Id   int    `sqlx:"foo_id"`
		Name string `sqlx:"foo_name"`
		Desc string `sqlx:"-"`
		Bar  float64
	}

	type case3FooID struct {
		Id   int `sqlx:"foo_id"`
		Desc string
	}

	type Case3FooName struct {
		Name string
	}

	type case3Wrapper struct {
		*case3FooID
		Case3FooName `sqlx:"ns=foo"`
	}

	var useCases = []struct {
		description    string
		query          string
		driver         string
		dsn            string
		newRow         func() interface{}
		params         []interface{}
		expect         interface{}
		initSQL        []string
		hasMapperError bool
		resolver       *io.Resolver
		expectResolved interface{}
		cache          *cache.Cache
		cachedData     [][]interface{}
		args           []interface{}
	}{
		{
			description: "Reading slice input   ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from t1",
				"insert into t1 values(1, \"John\")",
				"insert into t1 values(2, \"Bruce\")",
			},
			query: "select id , name  from t1 order by 1  ",
			newRow: func() interface{} {
				return make([]interface{}, 2)
			},
			expect: `[[1,"John"],[2,"Bruce"]]`,
		},
		{
			description: "Reading vanilla struct",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from t1",
				"insert into t1 values(1, \"John\")",
				"insert into t1 values(2, \"Bruce\")",
			},
			query: "select * from t1 order by id ",
			newRow: func() interface{} {
				return &fooCase1{}
			},
			expect: `[{"Id":1,"Name":"John"},{"Id":2,"Name":"Bruce"}]`,
		},
		{
			description: "Reading struct with tags  ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from t1",
				"insert into t1 values(1, \"John\")",
				"insert into t1 values(2, \"Bruce\")",
			},
			query: "select id as foo_id, name as foo_name from t1 order by 1 ",
			newRow: func() interface{} {
				return &fooCase2{}
			},
			expect: `[{"Id":1,"Name":"John","Desc":"","Bar":0},{"Id":2,"Name":"Bruce","Desc":"","Bar":0}]`,
		},
		{
			description: "Reading map input   ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from t1",
				"insert into t1 values(1, \"John\")",
				"insert into t1 values(2, \"Bruce\")",
			},
			query: "select id , name  from t1 order by 1  ",
			newRow: func() interface{} {
				return make(map[string]interface{})
			},
			expect: `[{"id":1,"name":"John"},{"id":2,"name":"Bruce"}]`,
		},

		{
			description: "Complex struct mapper",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t3 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT)",
				"delete from t3",
				"insert into t3 values(1, \"John\", \"desc1\")",
				"insert into t3 values(2, \"Bruce\", \"desc2\")",
			},
			query: "select foo_id , foo_name, desc  from t3 order by 1  ",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			expect: `[{"Id":1,"Desc":"desc1","Name":"John"},{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
		},
		{
			description: "Complex struct mapper with unresolved handelr",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t4 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT)",
				"delete from t4",
				"insert into t4 values(1, \"John\", \"desc1\")",
				"insert into t4 values(2, \"Bruce\", \"desc2\")",
			},
			query: "SELECT foo_id , foo_name, desc, '123' AS unk  FROM t4 ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			hasMapperError: true,
		},
		{
			description: "Complex struct mapper with unmappd handelr",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t4 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t4",
				"insert into t4 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t4 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t4 ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":1,"Desc":"desc1","Name":"John"},{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
			expectResolved: `["101","102"]`,
		},
		{
			description: "Cache",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t5 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t5",
				"insert into t5 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t5 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t5 ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":1,"Desc":"desc1","Name":"John"},{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
			expectResolved: `["101","102"]`,
			cache:          cache.NewCache("/tmp/t5/", time.Duration(10000)*time.Minute),
			cachedData: [][]interface{}{
				{float64(1), "John", "desc1", "101"},
				{float64(2), "Bruce", "desc2", "102"},
			},
		},
		{
			description: "Cache with args",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t5 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t5",
				"insert into t5 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t5 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t5 WHERE foo_id = ? ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
			expectResolved: `["102"]`,
			cache:          cache.NewCache("/tmp/t5/", time.Duration(10000)*time.Minute),
			cachedData: [][]interface{}{
				{float64(2), "Bruce", "desc2", "102"},
			},
			args: []interface{}{2},
		},
	}

outer:
	//for _, testCase := range useCases[len(useCases)-1:] {
	for _, testCase := range useCases {
		os.RemoveAll(testCase.dsn)
		ctx := context.Background()
		var db *sql.DB

		db, err := sql.Open(testCase.driver, testCase.dsn)
		if !assert.Nil(t, err, testCase.description) {
			log.Panic(err)
		}

		for _, SQL := range testCase.initSQL {
			_, err := db.Exec(SQL)
			if !assert.Nil(t, err, testCase.description) {
				continue outer
			}
		}

		var options = make([]option.Option, 0)
		if testCase.resolver != nil {
			options = append(options, testCase.resolver.Resolve)
		}

		if testCase.cache != nil {
			options = append(options, testCase.cache)
		}

		reader, err := read.New(ctx, db, testCase.query, testCase.newRow, options...)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		var actual = make([]interface{}, 0)
		err = reader.QueryAll(ctx, func(row interface{}) error {
			actual = append(actual, row)
			return nil
		}, testCase.args...)

		if testCase.hasMapperError {
			assert.NotNil(t, t, err, testCase.description)
			continue
		}

		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		actualJSON, _ := json.Marshal(actual)
		if !assert.EqualValues(t, testCase.expect, string(actualJSON), testCase.description) {
			fmt.Println(actualJSON)
			continue
		}

		if testCase.resolver != nil {
			actualJSON, _ := json.Marshal(testCase.resolver.Data(0))
			if !assert.EqualValues(t, testCase.expectResolved, string(actualJSON), testCase.description) {
				fmt.Println(actualJSON)
				continue
			}
		}

		if testCase.cache != nil {
			cacheEntry, err := testCase.cache.Get(context.TODO(), testCase.query, testCase.args)
			assert.Nil(t, err, testCase.description)
			assert.True(t, cacheEntry.Has(), testCase.description)
			assert.Equal(t, testCase.args, cacheEntry.Args, testCase.description)
			assert.Equal(t, testCase.query, cacheEntry.SQL, testCase.description)
			assert.Equal(t, testCase.cachedData, cacheEntry.Data, testCase.description)
		}
	}
}
