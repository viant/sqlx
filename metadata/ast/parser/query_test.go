package criteria

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"testing"
)

func TestParseSelect(t *testing.T) {

	{

		var testCases = []struct {
			description string
			SQL         string
			expect      string
		}{
			{
				description: "* select",
				SQL:         "SELECT * FROM x t",
				expect:      "SELECT * FROM x t",
			},

			{
				description: "basic select",
				SQL:         "SELECT col1, t.col2, col3 AS col FROM x t",
				expect:      "SELECT col1, t.col2, col3 AS col FROM x t",
			},

			{
				description: "execpt select",
				SQL:         "SELECT * EXCEPT c1,c2 FROM x t",
				expect:      "SELECT * EXCEPT c1, c2 FROM x t",
			},
			{
				description: "basic expr",
				SQL:         "SELECT col1 + col2 AS z, t.col2, col3 AS col FROM x t",
				expect:      "SELECT col1 + col2 AS z, t.col2, col3 AS col FROM x t",
			},

			{
				description: "JOIN select",
				SQL:         "SELECT t.* FROM x1 t join x2 z ON t.ID = z.ID",
				expect:      "SELECT t.* FROM x1 t join x2 z ON t.ID = z.ID",
			},
			{
				description: "JOIN select",
				SQL:         "SELECT t.* FROM x1 t join x2 z ON t.ID = z.ID LEFT JOIN x3 y ON t.ID = x3.ID",
				expect:      "SELECT t.* FROM x1 t join x2 z ON t.ID = z.ID LEFT JOIN x3 y ON t.ID = x3.ID",
			},

			{
				description: "select with WHERE",
				SQL:         "SELECT t.* FROM x t WHERE 1=1 AND (x=2)",
				expect:      "SELECT t.* FROM x t WHERE 1 = 1 AND (x=2)",
			},

			{
				description: "func call select",
				SQL:         "SELECT COALESCE(t.PARENT_ID,0) AS PARENT, t.col2, col3 AS col FROM x t",
				expect:      "SELECT COALESCE(t.PARENT_ID,0) AS PARENT, t.col2, col3 AS col FROM x t",
			},
			{
				description: "exists select",
				SQL:         "SELECT 1 FROM x t WHERE col IN (1,2,3)",
				expect:      "SELECT 1 FROM x t WHERE col IN (1,2,3)",
			},

			{
				description: "unary operand select",
				SQL:         "SELECT NOT t.col FROM x t",
				expect:      "SELECT  NOT t.col FROM x t",
			},
		}

		for _, testCase := range testCases {
			query, err := ParseQuery(testCase.SQL)
			if !assert.Nil(t, err) {
				fmt.Printf("%v\n", testCase.SQL)
				continue
			}
			actual := Stringify(query)
			if !assert.EqualValues(t, testCase.expect, actual) {
				toolbox.DumpIndent(query, true)
			}
		}
	}
}