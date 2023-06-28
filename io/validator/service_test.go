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
	expectViolations    bool
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
	Name   *string    `sqlx:"name=name,unique,table=v03" json:",omitempty"`
	DeptId *int       `sqlx:"name=dept_id,refColumn=id,refTable=dept01" json:",omitempty"`
	Desc   *int       `sqlx:"name=desc,required" json:",omitempty"`
	Has    *RecordHas `sqlx:"presence=true"`
}

type RecordHas struct {
	Id     bool
	Name   bool
	DeptId bool
	Desc   bool
}

func TestNewValidation(t *testing.T) {

	var testCases = []testCase{
		{
			description: "00 unique validation failure",
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
			expectViolations:    true,
			expectErrorFragment: "is not unique",
		},
		{
			description: "01 unique validation passed",
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
			expectViolations: false,
		},
		{
			description: "02 fk validation failure",
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
			expectViolations:    true,
			expectErrorFragment: "does not exists",
		},
		{
			description: "03 fk validation passed",
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
			expectViolations: false,
		},
		{
			description: "04 not null validation failure",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{},
			data: &NoNullRecord{
				Id: 10,
			},
			expectViolations:    true,
			expectErrorFragment: "null",
		},
		{
			description: "05 not null validation passed",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{},
			data: &NoNullRecord{
				Id:     10,
				Field1: intPtr(1),
			},
			expectViolations: false,
		},
		{
			description: "06 required failure - dept is required",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS dept01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"CREATE TABLE IF NOT EXISTS v02 (id INTEGER PRIMARY KEY, dept_id INTEGER, desc TEXT, unk TEXT)",
				"delete from v02",
				"delete from dept01",
				`insert into dept01 values(0, "Admin0", "admin dept0", "0101")`,
				`insert into dept01 values(1, "Admin", "admin dept", "101")`,
				`insert into v02 values(1, 2, "desc1", "101")`,
			},
			data: &FkRecord{
				Id:     10,
				DeptId: nil,
			},
			expectViolations: true,
		},
		{
			description: "07 required passed - dept is required",
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
			expectViolations: false,
		},
		{
			description: "08 fk validation failure with has",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS dept01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"delete from dept01",
			},
			data: &Record{
				Id:     10,
				DeptId: intPtr(2), //DeptId is ignored since has does not flag it as set
				Has: &RecordHas{
					DeptId: true,
				},
			},
			options:             []Option{WithSetMarker()},
			expectViolations:    true,
			expectErrorFragment: "does not exists",
		},
		{
			description: "09 fk validation passed with all has flags set to false",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS dept01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"delete from dept01",
			},
			data: &Record{
				Id:     10,
				DeptId: intPtr(2), //DeptId is ignored since has does not flag it as set
				Has:    &RecordHas{},
			},
			options:          []Option{WithSetMarker()},
			expectViolations: false,
		},
		{
			description: "10 not null validation failure with has",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{},
			data: &Record{
				Id:   10,
				Desc: nil, //Desc is ignored since has does not flag it as set
				Has: &RecordHas{
					Desc: true,
				},
			},
			options:             []Option{WithSetMarker()},
			expectViolations:    true,
			expectErrorFragment: "is null",
		},
		{
			description: "11 not null validation passed with all has flags set to false",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{},
			data: &Record{
				Id:   10,
				Desc: nil, //desc is ignored since has does not flag it as set
				Has:  &RecordHas{},
			},
			options:          []Option{WithSetMarker()},
			expectViolations: false,
		},
		{
			description: "12 unique validation failure with has",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS v03 (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, desc TEXT, unk TEXT)",
				"delete from v03",
				`insert into v03 values(1, "Name 101", 2, "desc1", "101")`,
			},
			data: &Record{
				Id:   11,
				Name: stringPtr("Name 101"), //Desc is ignored since has does not flag it as set
				Has: &RecordHas{
					Name: true,
				},
			},
			options:             []Option{WithSetMarker()},
			expectViolations:    true,
			expectErrorFragment: "is not unique",
		},
		{
			description: "13 unique validation passed with all has flags set to false",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS v03 (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, desc TEXT, unk TEXT)",
				"delete from v03",
				`insert into v03 values(1, "Name 101", 2, "desc1", "101")`,
			},
			data: &Record{
				Id:   11,
				Name: stringPtr("Name 101"), //Name is ignored since has does not flag it as set
				Has:  &RecordHas{},
			},
			options:          []Option{WithSetMarker()},
			expectViolations: false,
		},
	}

	for _, testCase := range testCases {
		//fmt.Printf("#CASE: %d/%d - %s\n", i+1, len(testCases), testCase.description)

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
		validation, err := validator.Validate(context.Background(), db, testCase.data, testCase.options...)
		assert.Nil(t, err, testCase.description)

		if testCase.expectViolations {
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

func TestNewValidationWithCache(t *testing.T) {

	var testCases = []testCase{
		{
			description: "00 unique validation failure",
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
			expectViolations:    true,
			expectErrorFragment: "is not unique",
		},
		{
			description: "01 unique validation passed",
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
			expectViolations: false,
		},
		{
			description: "02 fk validation failure",
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
			expectViolations:    true,
			expectErrorFragment: "does not exists",
		},
		{
			description: "03 fk validation passed",
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
			expectViolations: false,
		},
		{
			description: "04 not null validation failure",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{},
			data: &NoNullRecord{
				Id: 10,
			},
			expectViolations:    true,
			expectErrorFragment: "null",
		},
		{
			description: "05 not null validation passed",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{},
			data: &NoNullRecord{
				Id:     10,
				Field1: intPtr(1),
			},
			expectViolations: false,
		},
		{
			description: "06 required failure - dept is required",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS dept01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"CREATE TABLE IF NOT EXISTS v02 (id INTEGER PRIMARY KEY, dept_id INTEGER, desc TEXT, unk TEXT)",
				"delete from v02",
				"delete from dept01",
				`insert into dept01 values(0, "Admin0", "admin dept0", "0101")`,
				`insert into dept01 values(1, "Admin", "admin dept", "101")`,
				`insert into v02 values(1, 2, "desc1", "101")`,
			},
			data: &FkRecord{
				Id:     10,
				DeptId: nil,
			},
			expectViolations: true,
		},
		{
			description: "07 required passed - dept is required",
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
			expectViolations: false,
		},
		{
			description: "08 fk validation failure with has",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS dept01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"delete from dept01",
			},
			data: &Record{
				Id:     10,
				DeptId: intPtr(2), //DeptId is ignored since has does not flag it as set
				Has: &RecordHas{
					DeptId: true,
				},
			},
			options:             []Option{WithSetMarker()},
			expectViolations:    true,
			expectErrorFragment: "does not exists",
		},
		{
			description: "09 fk validation passed with all has flags set to false",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS dept01 (id INTEGER PRIMARY KEY, name TEXT, desc TEXT, unk TEXT)",
				"delete from dept01",
			},
			data: &Record{
				Id:     10,
				DeptId: intPtr(2), //DeptId is ignored since has does not flag it as set
				Has:    &RecordHas{},
			},
			options:          []Option{WithSetMarker()},
			expectViolations: false,
		},
		{
			description: "10 not null validation failure with has",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{},
			data: &Record{
				Id:   10,
				Desc: nil, //Desc is ignored since has does not flag it as set
				Has: &RecordHas{
					Desc: true,
				},
			},
			options:             []Option{WithSetMarker()},
			expectViolations:    true,
			expectErrorFragment: "is null",
		},
		{
			description: "11 not null validation passed with all has flags set to false",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL:     []string{},
			data: &Record{
				Id:   10,
				Desc: nil, //desc is ignored since has does not flag it as set
				Has:  &RecordHas{},
			},
			options:          []Option{WithSetMarker()},
			expectViolations: false,
		},
		{
			description: "12 unique validation failure with has",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS v03 (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, desc TEXT, unk TEXT)",
				"delete from v03",
				`insert into v03 values(1, "Name 101", 2, "desc1", "101")`,
			},
			data: &Record{
				Id:   11,
				Name: stringPtr("Name 101"), //Desc is ignored since has does not flag it as set
				Has: &RecordHas{
					Name: true,
				},
			},
			options:             []Option{WithSetMarker()},
			expectViolations:    true,
			expectErrorFragment: "is not unique",
		},
		{
			description: "13 unique validation passed with all has flags set to false",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS v03 (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, desc TEXT, unk TEXT)",
				"delete from v03",
				`insert into v03 values(1, "Name 101", 2, "desc1", "101")`,
			},
			data: &Record{
				Id:   11,
				Name: stringPtr("Name 101"), //Name is ignored since has does not flag it as set
				Has:  &RecordHas{},
			},
			options:          []Option{WithSetMarker()},
			expectViolations: false,
		},
	}
	for _, testCase := range testCases {
		//for i, testCase := range testCases {
		//	fmt.Printf("#CASE: %d/%d - %s\n", i+1, len(testCases), testCase.description)

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
		validation, err := validator.Validate(context.Background(), db, testCase.data, testCase.options...)
		assert.Nil(t, err, testCase.description)

		// repeat validation for using cached checks inside validator/service.go - checksFor func
		validation, err = validator.Validate(context.Background(), db, testCase.data, testCase.options...)
		assert.Nil(t, err, testCase.description)

		if testCase.expectViolations {
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
