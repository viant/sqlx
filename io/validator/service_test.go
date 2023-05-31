package validator

import (
	"context"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"log"
	"strings"
	"testing"
)

type testCase struct {
	description         string
	driver              string
	dsn                 string
	initSQL             []string
	data                interface{}
	expectError         bool
	options             []Option
	expectErrorFragment string
}

type UniqueRecord struct {
	Id   int     `sqlx:"name=ID,autoincrement,primaryKey"`
	Name *string `sqlx:"name=name,unique,table=v01" json:",omitempty"`
}

type FkRecord struct {
	Id     int  `sqlx:"name=ID,autoincrement,primaryKey"`
	DeptId *int `sqlx:"name=name,refColumn=id,refTable=dept01,required" json:",omitempty"`
}

type NoNullRecord struct {
	Id     int  `sqlx:"name=ID,autoincrement,primaryKey"`
	Field1 *int `sqlx:"name=f1,required" json:",omitempty"`
}

type Record struct {
	Id     int        `sqlx:"name=ID,autoincrement,primaryKey"`
	Name   *string    `sqlx:"name=name,unique,table=v01" json:",omitempty"`
	DeptId *int       `sqlx:"name=name,refColumn=id,refTable=dept01" json:",omitempty"`
	Field1 *int       `sqlx:"name=f1,notNull" json:",omitempty"`
	Has    *RecordHas `sqlx:"presence=true"`
}

type RecordHas struct {
	Id     bool
	Name   bool
	DeptId bool
	Field1 bool
}

func TestNewValidation(t *testing.T) {
	return

	var testCases = []testCase{
		{
			description: "unique validation failure",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS v01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"delete from v01",
				`insert into v01 values(1, "John Wick", "desc1", "101")`,
			},
			data: &UniqueRecord{
				Id:   10,
				Name: stringPtr("John Wick"),
			},
			expectError:         true,
			expectErrorFragment: "is not unique",
		},
		{
			description: "unique validation valid",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS v01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"delete from v01",
				`insert into v01 values(1, "John Wick", "desc1", "101")`,
			},
			data: &UniqueRecord{
				Id:   10,
				Name: stringPtr("John Wick2"),
			},
			expectError: false,
		},
		{
			description: "fk validation failure",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS dept01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"CREATE TABLE IF NOT EXISTS v02 (id INTEGER PRIMARY KEY, dept_id INTEGER, desc TEXT, unk TEXT)",
				"delete from v02",
				"delete from dept01",
				`insert into dept01 values(1, "Admin", "admin dept", "101")`,
				`insert into v02 values(1, 2, "desc1", "101")`,
			},
			data: &FkRecord{
				Id:     10,
				DeptId: intPtr(2),
			},
			expectError:         true,
			expectErrorFragment: "does not exists",
		},
		{
			description: "fk validation passed",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS dept01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"CREATE TABLE IF NOT EXISTS v02 (id INTEGER PRIMARY KEY, dept_id INTEGER, desc TEXT, unk TEXT)",
				"delete from v02",
				"delete from dept01",
				`insert into dept01 values(1, "Admin", "admin dept", "101")`,
				`insert into v02 values(1, 2, "desc1", "101")`,
			},
			data: &FkRecord{
				Id:     10,
				DeptId: intPtr(1),
			},
			expectError: false,
		},
		{
			description: "not null validation failure",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{},
			data: &NoNullRecord{
				Id: 10,
			},
			expectError:         true,
			expectErrorFragment: "null",
		},
		{
			description: "not null validation passed",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{},
			data: &NoNullRecord{
				Id:     10,
				Field1: intPtr(1),
			},
			expectError: false,
		},
		{
			description: "not null validation passed",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{},
			data: &NoNullRecord{
				Id:     10,
				Field1: intPtr(1),
			},
			expectError: false,
		},

		{
			description: "fk validation failure with has",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS dept01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"CREATE TABLE IF NOT EXISTS v02 (id INTEGER PRIMARY KEY, dept_id INTEGER, desc TEXT, unk TEXT)",
				"delete from v02",
				"delete from dept01",
				`insert into dept01 values(1, "Admin", "admin dept", "101")`,
				`insert into v02 values(1, 2, "desc1", "101")`,
			},
			data: &Record{
				Id:     10,
				DeptId: intPtr(2), //DeptId ignore since has does not flag it as set
				Has: &RecordHas{
					DeptId: true,
				},
			},
			options:             []Option{WithPresence()},
			expectError:         true,
			expectErrorFragment: "does not exists",
		},
		{
			description: "fk validation failure with not has flag",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS dept01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"CREATE TABLE IF NOT EXISTS v02 (id INTEGER PRIMARY KEY, dept_id INTEGER, desc TEXT, unk TEXT)",
				"delete from v02",
				"delete from dept01",
				`insert into dept01 values(1, "Admin", "admin dept", "101")`,
				`insert into v02 values(1, 2, "desc1", "101")`,
			},
			data: &Record{
				Id:     10,
				DeptId: intPtr(2), //DeptId ignore since has does not flag it as set
				Has:    &RecordHas{},
			},
			options:     []Option{WithPresence()},
			expectError: false,
		},
		{
			description: "fk validation passed - dept is required",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS dept01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"CREATE TABLE IF NOT EXISTS v02 (id INTEGER PRIMARY KEY, dept_id INTEGER, desc TEXT, unk TEXT)",
				"delete from v02",
				"delete from dept01",
				`insert into dept01 values(1, "Admin", "admin dept", "101")`,
				`insert into v02 values(1, 2, "desc1", "101")`,
			},
			data: &FkRecord{
				Id:     10,
				DeptId: nil,
			},
			expectError: true,
		},
	}

	//TODO add option for HAS detection
	for _, testCase := range testCases {

		db, err := sql.Open(testCase.driver, testCase.dsn)
		if !assert.Nil(t, err, testCase.description) {
			log.Panic(err)
		}
		for _, SQL := range testCase.initSQL {
			_, err := db.Exec(SQL)
			if !assert.Nil(t, err, testCase.description) {
				continue
			}
		}
		validator := New()
		validation, _ := validator.Validate(context.Background(), db, testCase.data, testCase.options...)
		if testCase.expectError {
			if !assert.True(t, strings.Contains(validation.Error(), testCase.expectErrorFragment), testCase.description) {
				toolbox.Dump(validation)
				continue
			}
			if !assert.NotNilf(t, validation, testCase.description) {
				continue
			}
			continue
		}

		if assert.False(t, validation.Failed, testCase.description) {
			continue
		}
	}
}

func stringPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}
