package insert

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	"testing"
)

func TestInsert_Build(t *testing.T) {

	var testCases = []struct {
		description   string
		table         string
		batchSize     int
		identity      string
		callBatchSize int
		columns       []string
		dialect       *info.Dialect
		expect        string
	}{
		{
			description: "batch size 1 without table escape quote",
			table:       "foo",
			columns:     []string{"c1", "cN"},
			dialect: &info.Dialect{
				Placeholder: "?",
			},
			batchSize:     1,
			callBatchSize: 1,
			expect:        `INSERT INTO foo(c1,cN) VALUES (?,?)`,
		},
		{
			description: "batch size 5 without table escape quote",
			table:       "foo",
			columns:     []string{"c1", "cN"},
			dialect: &info.Dialect{
				Placeholder: "?",
			},
			batchSize:     5,
			callBatchSize: 5,
			expect:        `INSERT INTO foo(c1,cN) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)`,
		},
		{
			description: "smaller call batch size without table escape quote",
			table:       "foo",
			columns:     []string{"c1", "cN"},
			dialect: &info.Dialect{
				Placeholder: "?",
			},
			batchSize:     5,
			callBatchSize: 3,
			expect:        `INSERT INTO foo(c1,cN) VALUES (?,?),(?,?),(?,?)`,
		},
		{
			description: "table escaped with configured double quote",
			table:       "foo",
			columns:     []string{"c1", "cN"},
			dialect: &info.Dialect{
				Placeholder:               "?",
				SpecialKeywordEscapeQuote: '"',
			},
			batchSize:     1,
			callBatchSize: 1,
			expect:        `INSERT INTO "foo"(c1,cN) VALUES (?,?)`,
		},
		{
			description: "table escaped with configured backtick",
			table:       "foo",
			columns:     []string{"c1", "cN"},
			dialect: &info.Dialect{
				Placeholder:               "?",
				SpecialKeywordEscapeQuote: '`',
			},
			batchSize:     1,
			callBatchSize: 1,
			expect:        "INSERT INTO `foo`(c1,cN) VALUES (?,?)",
		},
	}

	for _, testCase := range testCases {
		builder, err := NewBuilder(testCase.table, testCase.columns, testCase.dialect, testCase.identity, testCase.batchSize)
		assert.Nil(t, err, testCase.description)
		actual := builder.Build(nil, option.BatchSize(testCase.callBatchSize))
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}
}
