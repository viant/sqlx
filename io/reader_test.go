package io

import (
	"context"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

type Foo_Case1 struct {
	Id   int
	Name string
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
			description: "Test reader: sqllite",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{"CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name TEXT)", "delete from t1", "insert into t1 values(1, \"John\")"},
			query:       "select * from t1",
			newRow: func() interface{} {
				return &Foo_Case1{}
			},
		},
	}

outer:
	for _, useCase := range useCases {
		ctx := context.Background()
		var db *sql.DB

		db, err :=
			sql.Open(useCase.driver, useCase.dsn)
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
		err = reader.ReadAll(ctx, func(row interface{}) error {
			actual = append(actual, row)
			return nil
		})
		assert.Nil(t, err, useCases)

	}

}
