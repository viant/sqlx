package io_test

import (
	"context"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/opt"
	"log"
	"testing"
)


func TestWriter_Insert(t *testing.T) {
	type fooCase1 struct {
		Id   int
		Name string
	}

	type fooCase2 struct {
		Id   int    `sqlx:"-"`
		Name string `sqlx:"foo_name"`
		Desc string `sqlx:"-"`
		Bar  float64
	}

	var useCases = []struct {
		description string
		table       string
		driver      string
		dsn         string
		record      interface{}
		options     []opt.Option
		expect      interface{}
		initSQL     []string
	}{
		{
			description: "Writing vanilla struct",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t1",
			initSQL: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1 (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)",
			},
			record: fooCase1{Id: 1, Name: "John"},
		},
		{
			description: "Writing struct with tags  ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t2",
			initSQL: []string{
				"DROP TABLE IF EXISTS t2",
				"CREATE TABLE t2 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, Bar integer)",
			},
			record: &fooCase2{Id: 15, Name: "John", Desc: "description", Bar: 17},
		},
	}

outer:

	for _, useCase := range useCases {

		//ctx := context.Background()
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

		meta := metadata.New()
		product, err := meta.DetectProduct(context.TODO(), db)
		assert.Nil(t, err, useCases)
		if err != nil {
			continue
		}
		writer, err := io.NewWriter(context.TODO(), db, useCase.table, product)
		assert.Nil(t, err, useCases)
		if err != nil {
			continue
		}
		_, _, err = writer.Insert(useCase.record, 2)
		assert.Nil(t, err, useCases)
	}

}

func TestBulkWriter(t *testing.T) {
	type fooCase2 struct {
		Id   int    `sqlx:"name=foo_id,autoincrement=true"`
		Name string `sqlx:"foo_name"`
		Desc string `sqlx:"-"`
		Bar  float64
	}

	var useCases = []struct {
		description string
		table       string
		driver      string
		dsn         string
		options     []opt.Option
		records     interface{}
		params      []interface{}
		expect      interface{}
		initSQL     []string
	}{
		{
			description: "Bulk writing structs 1  ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t2",
			initSQL: []string{
				"DROP TABLE IF EXISTS t2",
				"CREATE TABLE t2 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, Bar integer)",
			},
			records: []interface{}{
				&fooCase2{Id: 1, Name: "John1", Desc: "description", Bar: 17},
				&fooCase2{Id: 2, Name: "John2", Desc: "description", Bar: 18},
				&fooCase2{Id: 3, Name: "John3", Desc: "description", Bar: 19},
			},
			options: []opt.Option{
				opt.TagOption{"sqlx"},
				&opt.BatchOption{2},
			},
		},
		{
			description: "Bulk writing structs 2 ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			table:       "t3",
			initSQL: []string{
				"DROP TABLE IF EXISTS t3",
				"CREATE TABLE t3 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, Bar integer)",
			},
			records: []fooCase2{
				{Id: 1, Name: "John1", Desc: "description", Bar: 17},
				{Id: 2, Name: "John2", Desc: "description", Bar: 18},
				{Id: 3, Name: "John3", Desc: "description", Bar: 19},
			},
			options: []opt.Option{
				opt.TagOption{"sqlx"},
				&opt.BatchOption{2},
			},
		},
	}

outer:

	for _, testCase := range useCases {

		//ctx := context.Background()
		var db *sql.DB

		db, err :=sql.Open(testCase.driver, testCase.dsn)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		for _, SQL := range testCase.initSQL {
			_, err := db.Exec(SQL)
			if !assert.Nil(t, err, testCase.description) {
				continue outer
			}
		}

		meta := metadata.New()
		product, err := meta.DetectProduct(context.TODO(), db)
		if ! assert.Nil(t, err, testCase.description) {
			continue
		}
		testCase.options = append(testCase.options, product)
		writer, err := io.NewWriter(context.TODO(), db, testCase.table, testCase.options...)
		if ! assert.Nil(t, err, testCase.description) {
			continue
		}
		_, _, err = writer.Insert(testCase.records)
		assert.Nil(t, err, testCase.description)
	}

}
