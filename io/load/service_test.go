package load_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/sqlx/io/load"
	_ "github.com/viant/sqlx/metadata/product/mysql/load"
	"os"
	"testing"
)

func TestService_Exec(t *testing.T) {
	//os.Setenv("TEST_MYSQL_DSN", "root:dev@tcp(127.0.0.1:3307)/ci_ads?parseTime=true")
	dsn, shallSkip := getTestConfig(t)
	if shallSkip {
		return
	}

	type Config struct {
		Driver string
		DSN    string
	}

	c := &Config{
		Driver: "mysql",
		DSN:    dsn, //"root:dev@tcp(127.0.0.1:3307)/ci_ads?parseTime=true",
	}

	type Foo struct {
		//ID   int    `column:"ID" pk:"true" sqlx:"name=ID,primaryKey=true,generator=autoincrement"`
		Name string `column:"NAME" sqlx:"name=NAME,nullifyempty"`
		ID   int    `column:"ID" pk:"true" sqlx:"name=ID,primaryKey=true"`
	}

	var testCases = []struct {
		description string
		table       string
		records     interface{}
		expected    interface{}
	}{
		{
			description: "basic test",
			table:       "FOO",
			records: []*Foo{
				&Foo{ID: 1, Name: "A"},
			},
			expected: []*Foo{
				&Foo{ID: 1, Name: "A"},
			},
		},
	}

	for _, testCase := range testCases {
		/////
		initSQL := []string{
			"DROP TABLE IF EXISTS `" + testCase.table + "`",
			"CREATE TABLE IF NOT EXISTS `" + testCase.table + "` (\n  `ID` int(11) NOT NULL,\n  `NAME` varchar(255) DEFAULT NULL,\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1",
			//"TRUNCATE TABLE " + testCase.table,
		}

		db, err := sql.Open(c.Driver, c.DSN)
		assert.Nil(t, err, testCase.description)

		for _, SQL := range initSQL {
			//fmt.Println(SQL)
			_, err := db.Exec(SQL)
			assert.Nil(t, err, testCase.description)
		}

		loader, err := load.New(context.Background(), db, testCase.table)
		assert.Nil(t, err, testCase.description)

		count, err := loader.Exec(context.TODO(), testCase.records)
		assert.Nil(t, err, testCase.description)
		fmt.Printf("loaded %v\n", count)

		SQL := "SELECT * FROM " + testCase.table + " ORDER BY ID"
		rows, err := db.QueryContext(context.TODO(), SQL)
		assert.Nil(t, err, testCase.description)
		actual := []*Foo{}

		for rows.Next() {
			rule := Foo{}
			err = rows.Scan(&rule.ID, &rule.Name)
			assert.Nil(t, err, testCase.description)
			actual = append(actual, &rule)
		}

		if !assertly.AssertValues(t, testCase.expected, actual) {
			//fmt.Println("EXPECTED")
			//toolbox.DumpIndent(testCase.srcRecords, true)
			//fmt.Println("ACTUAL")
			//toolbox.DumpIndent(actual, true)
		}
		if assertly.AssertValues(t, actual, testCase.expected) {
			//fmt.Println("EXPECTED")
			//toolbox.DumpIndent(testCase.srcRecords, true)
			//fmt.Println("ACTUAL")
			//toolbox.DumpIndent(actual, true)
		}

		/////
	}
}

func getTestConfig(t *testing.T) (dsn string, shallSkip bool) {
	dsn = os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN before running test")
		return "", true
	}
	return dsn, false
}
