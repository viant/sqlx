package load

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/load/reader/csv"
	"testing"
)

func TestBuildSQL(t *testing.T) {
	testCases := []struct {
		description     string
		readerID        string
		tableName       string
		fieldSeparator  string
		objectSeparator string
		escapeBy        string
		nullValue       string
		encloseBy       string
		expected        string
		columns         []string
	}{
		{
			description:     "test case #1",
			readerID:        "MysqlReader",
			tableName:       "Foos",
			fieldSeparator:  ",",
			objectSeparator: "#",
			escapeBy:        "^",
			encloseBy:       `"`,
			nullValue:       "null",
			columns:         []string{"Id", "Name", "Price"},
			expected:        `LOAD DATA LOCAL INFILE 'Reader::MysqlReader' INTO TABLE Foos FIELDS TERMINATED BY ',' ESCAPED BY '^' ENCLOSED BY '"' LINES TERMINATED BY '#' (Id,Name,Price)`,
		},
	}

	for _, testCase := range testCases {
		sql := BuildSQL(&csv.Config{
			FieldSeparator:  testCase.fieldSeparator,
			ObjectSeparator: testCase.objectSeparator,
			EncloseBy:       testCase.encloseBy,
			EscapeBy:        testCase.escapeBy,
			NullValue:       testCase.nullValue,
		}, testCase.readerID, testCase.tableName, io.NamesToColumns(testCase.columns))
		assert.Equal(t, testCase.expected, sql, testCase.description)
	}
}
