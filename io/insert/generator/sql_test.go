package generator

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"testing"
)

func TestUpdate_Build(t *testing.T) {

	var testCases = []struct {
		description      string
		table            string
		columns          []sink.Column
		batchSize        int
		builderBatchSize int
		expect           string
	}{
		{
			description: "generate with batch size",
			table:       "foo",
			columns: []sink.Column{
				{
					Name:    "NAME",
					Default: stringPtr("uuid_v4()"),
				},
				{
					Name:    "ID",
					Default: stringPtr("nextval"),
				},
			},
			batchSize:        2,
			builderBatchSize: 2,
			expect:           `SELECT COALESCE(?,uuid_v4()) AS NAME,COALESCE(?,nextval) AS ID, ?+0 AS SQLX_POS UNION SELECT COALESCE(?,uuid_v4()) AS NAME,COALESCE(?,nextval) AS ID, ?+0 AS SQLX_POS`,
		},
		{
			description: "generate with batch size",
			table:       "foo",
			columns: []sink.Column{
				{
					Name:    "NAME",
					Default: stringPtr("uuid_v4()"),
				},
				{
					Name:    "ID",
					Default: stringPtr("nextval"),
				},
			},
			batchSize:        2,
			builderBatchSize: 3,
			expect:           `SELECT COALESCE(?,uuid_v4()) AS NAME,COALESCE(?,nextval) AS ID, ?+0 AS SQLX_POS UNION SELECT COALESCE(?,uuid_v4()) AS NAME,COALESCE(?,nextval) AS ID, ?+0 AS SQLX_POS`,
		},
	}

	for _, testCase := range testCases {
		builder := NewBuilder(testCase.columns, testCase.builderBatchSize)
		actual := builder.Build(option.BatchSize(testCase.batchSize))
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}

}

func stringPtr(value string) *string  {
	return &value
}
