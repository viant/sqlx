package info_test

import (
	"testing"

	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/product/bigquery"
	"github.com/viant/sqlx/metadata/product/pg"
	"github.com/viant/sqlx/metadata/registry"
)

func TestDialectCompositeIn(t *testing.T) {
	testCases := []struct {
		name     string
		dialect  *info.Dialect
		columns  []string
		rowCount int
		want     string
	}{
		{
			name:     "zero rows",
			dialect:  &info.Dialect{},
			columns:  []string{"t.id", "t.version"},
			rowCount: 0,
			want:     "1 = 0",
		},
		{
			name:     "zero columns",
			dialect:  &info.Dialect{},
			rowCount: 2,
			want:     "1 = 0",
		},
		{
			name:     "scalar values",
			dialect:  &info.Dialect{},
			columns:  []string{"t.id"},
			rowCount: 3,
			want:     "t.id IN (?, ?, ?)",
		},
		{
			name:     "standard tuples",
			dialect:  &info.Dialect{Product: database.Product{Name: "MySQL"}},
			columns:  []string{"t.id", "t.version"},
			rowCount: 2,
			want:     "(t.id, t.version) IN ((?, ?), (?, ?))",
		},
		{
			name:     "bigquery structs",
			dialect:  registry.LookupDialect(bigquery.BigQuery()),
			columns:  []string{"t.id", "t.version"},
			rowCount: 2,
			want:     "(t.id, t.version) IN (SELECT AS STRUCT ?, ? UNION ALL SELECT AS STRUCT ?, ?)",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.dialect == nil {
				t.Fatal("missing dialect")
			}
			if actual := testCase.dialect.CompositeIn(testCase.columns, testCase.rowCount); actual != testCase.want {
				t.Fatalf("expected %q, got %q", testCase.want, actual)
			}
		})
	}
}

func TestDialectCompositeInEnsurePlaceholders(t *testing.T) {
	dialect := &info.Dialect{PlaceholderResolver: &pg.PlaceholderGenerator{}}
	actual := dialect.EnsurePlaceholders(dialect.CompositeIn([]string{"t.id", "t.version"}, 2))
	want := "(t.id, t.version) IN (($1, $2), ($3, $4))"
	if actual != want {
		t.Fatalf("expected %q, got %q", want, actual)
	}
}
