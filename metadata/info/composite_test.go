package info

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/metadata/database"
)

func TestDialect_CompositeIn(t *testing.T) {
	testCases := []struct {
		description string
		dialect     *Dialect
		columns     []string
		rowCount    int
		expect      string
	}{
		{
			description: "mysql composite",
			dialect:     &Dialect{Product: database.Product{Name: "MySQL"}},
			columns:     []string{"t.advertiser_id", "t.val"},
			rowCount:    2,
			expect:      "(t.advertiser_id, t.val) IN ((?, ?), (?, ?))",
		},
		{
			description: "postgres composite",
			dialect:     &Dialect{Product: database.Product{Name: "PostgreSQL"}},
			columns:     []string{"t.advertiser_id", "t.val"},
			rowCount:    2,
			expect:      "(t.advertiser_id, t.val) IN ((?, ?), (?, ?))",
		},
		{
			description: "bigquery composite",
			dialect: &Dialect{Product: database.Product{Name: "BigQuery"}, CompositeInRenderer: func(columns []string, rowCount int) string {
				return "(t.advertiser_id, t.val) IN (SELECT AS STRUCT ?, ? UNION ALL SELECT AS STRUCT ?, ?)"
			}},
			columns:  []string{"t.advertiser_id", "t.val"},
			rowCount: 2,
			expect:   "(t.advertiser_id, t.val) IN (SELECT AS STRUCT ?, ? UNION ALL SELECT AS STRUCT ?, ?)",
		},
		{
			description: "empty rows",
			dialect:     &Dialect{Product: database.Product{Name: "MySQL"}},
			columns:     []string{"t.advertiser_id", "t.val"},
			rowCount:    0,
			expect:      "1 = 0",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			assert.Equal(t, testCase.expect, testCase.dialect.CompositeIn(testCase.columns, testCase.rowCount))
		})
	}
}
