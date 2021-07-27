package io

import (
	"context"
	"database/sql"
	"encoding/json"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

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

func TestReader_ReadAll(t *testing.T) {
	var useCases = []struct {
		description string
		query       string
		driver      string
		dsn         string
		newRow      func() interface{}
		params      []interface{}
		expect      interface{}
		initSQL     []string
	}{
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
	}

outer:

	for _, useCase := range useCases {

		ctx := context.Background()
		var db *sql.DB

		db, err := sql.Open(useCase.driver, useCase.dsn)
		if !assert.Nil(t, err, useCase.description) {
			log.Panic(err)
		}

		for _, SQL := range useCase.initSQL {
			_, err := db.Exec(SQL)
			if !assert.Nil(t, err, useCase.description) {
				continue outer
			}
		}

		reader, err := NewReader(ctx, db, useCase.query, useCase.newRow)
		assert.Nil(t, err, useCases)
		var actual = make([]interface{}, 0)
		err = reader.QueryAll(ctx, func(row interface{}) error {
			actual = append(actual, row)
			return nil
		})
		assert.Nil(t, err, useCases)
		jActual, _ := json.Marshal(actual)
		assert.EqualValues(t, useCase.expect, string(jActual), useCase.description)

	}

}
