package insert

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInsert_Build(t *testing.T) {

	var testCases = []struct {
		description   string
		table         string
		batchSize     int
		callBatchSize int
		columns       []string
		values        []string
		expect        string
	}{
		{
			description:   "batch size 1",
			table:         "foo",
			columns:       []string{"c1", "cN"},
			values:        []string{"?", "?"},
			batchSize: 1,
			callBatchSize: 1,
			expect: "INSERT INTO foo(c1,cN) VALUES (?,?)",
		},
		{
			description:   "batch size 5",
			table:         "foo",
			columns:       []string{"c1", "cN"},
			values:        []string{"?", "?"},
			batchSize: 5,
			callBatchSize: 5,
			expect: "INSERT INTO foo(c1,cN) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)",
		},
		{
			description:   "batch size 5",
			table:         "foo",
			columns:       []string{"c1", "cN"},
			values:        []string{"?", "?"},
			batchSize: 5,
			callBatchSize: 3,
			expect: "INSERT INTO foo(c1,cN) VALUES (?,?),(?,?),(?,?)",
		},
	}

	for _, testCase := range testCases {
		builder, err := NewInsert(testCase.table, testCase.batchSize, testCase.columns, testCase.values)
		assert.Nil(t, err, testCase.description)
		actual := builder.Build(testCase.callBatchSize)
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}

}
