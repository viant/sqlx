package load

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	_ "github.com/viant/sqlx/metadata/product/mysql/load"
	"os"
	"testing"
)

func TestService_Exec(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN before running test")
	}

	type Config struct {
		Driver string
		DSN    string
	}

	c := &Config{
		Driver: "mysql",
		DSN:    dsn,
	}

	type Foo struct {
		ID   int    `sqlx:"name=ID,primaryKey=true"`
		Name string `sqlx:"name=NAME,nullifyempty"`
	}

	type FooReverse struct {
		Name string `sqlx:"name=NAME,nullifyemp"`
		ID   int    `sqlx:"name=ID,primaryKey=true"`
	}

	var testCases = []struct {
		description string
		table       string
		records     interface{}
		expected    interface{}
	}{
		{
			description: "ID as first field",
			table:       "FOO",
			records: []*Foo{
				{ID: 1, Name: "A"},
				{ID: 2, Name: "B"},
			},
			expected: []*Foo{
				{ID: 1, Name: "A"},
				{ID: 2, Name: "B"},
			},
		},
		{
			description: "ID as last field",
			table:       "FOO",
			records: []*FooReverse{
				{Name: "A", ID: 1},
				{Name: "B", ID: 2},
			},
			expected: []*Foo{
				{Name: "A", ID: 1},
				{Name: "B", ID: 2},
			},
		},
	}

	for _, testCase := range testCases {

		initSQL := []string{
			"DROP TABLE IF EXISTS `" + testCase.table + "`",
			"CREATE TABLE IF NOT EXISTS `" + testCase.table + "` (\n  `ID` int(11) NOT NULL,\n  `NAME` varchar(255) DEFAULT NULL,\n PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1",
		}

		db, err := sql.Open(c.Driver, c.DSN)
		assert.Nil(t, err, testCase.description)

		for _, SQL := range initSQL {
			_, err := db.Exec(SQL)
			assert.Nil(t, err, testCase.description)
		}

		loader, err := New(context.Background(), db, testCase.table)
		assert.Nil(t, err, testCase.description)

		_, err = loader.Exec(context.TODO(), testCase.records)
		assert.Nil(t, err, testCase.description)

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

		assertly.AssertValues(t, testCase.expected, actual)
		assertly.AssertValues(t, actual, testCase.expected)
	}
}
