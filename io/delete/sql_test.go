package delete

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	"testing"
)

func TestUpdate_Build(t *testing.T) {

	var testCases = []struct {
		description      string
		table            string
		columns          []string
		dialect          *info.Dialect
		batchSize        int
		builderBatchSize int
		expect           string
	}{
		{
			description:      "delete with batch size",
			table:            "foo",
			columns:          []string{"cId"},
			batchSize:        3,
			builderBatchSize: 3,
			dialect: &info.Dialect{
				Placeholder: "?",
			},
			expect: "DELETE FROM foo WHERE cId IN (?,?,?)",
		},
		{
			description:      "delete with batch size",
			table:            "foo",
			columns:          []string{"cId"},
			batchSize:        2,
			builderBatchSize: 3,
			dialect: &info.Dialect{
				Placeholder: "?",
			},
			expect: "DELETE FROM foo WHERE cId IN (?,?)",
		},
		{
			description:      "delete with two column",
			table:            "foo",
			columns:          []string{"c1", "c2"},
			batchSize:        3,
			builderBatchSize: 3,
			dialect: &info.Dialect{
				Placeholder: "?",
			},
			expect: "DELETE FROM foo WHERE (c1,c2) IN ((?,?),(?,?),(?,?))",
		},
		{
			description:      "delete with two column",
			table:            "foo",
			columns:          []string{"c1", "c2"},
			batchSize:        2,
			builderBatchSize: 3,
			dialect: &info.Dialect{
				Placeholder: "?",
			},
			expect: "DELETE FROM foo WHERE (c1,c2) IN ((?,?),(?,?))",
		},
	}

	for _, testCase := range testCases {
		builder, err := NewBuilder(testCase.table, testCase.columns, testCase.dialect, testCase.builderBatchSize)
		assert.Nil(t, err, testCase.description)
		actual := builder.Build(nil, option.BatchSize(testCase.batchSize))
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}

}
