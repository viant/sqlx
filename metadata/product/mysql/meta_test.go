package mysql

import (
	"testing"

	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
)

func TestMetadataCatalogCriteria(t *testing.T) {
	testCases := []struct {
		kind        info.Kind
		wantColumns []string
	}{
		{kind: info.KindSchemas, wantColumns: []string{""}},
		{kind: info.KindSchema, wantColumns: []string{"", "SCHEMA_NAME"}},
		{kind: info.KindTables, wantColumns: []string{"", "TABLE_SCHEMA"}},
		{kind: info.KindTable, wantColumns: []string{"", "TABLE_SCHEMA", "TABLE_NAME"}},
		{kind: info.KindIndexes, wantColumns: []string{"", "TABLE_SCHEMA", "TABLE_NAME"}},
		{kind: info.KindIndex, wantColumns: []string{"", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.kind.String(), func(t *testing.T) {
			queries := registry.Lookup(MySQL5().Name, testCase.kind)
			query := queries.Match(MySQL5())
			if query == nil {
				t.Fatalf("missing MySQL query for %v", testCase.kind)
			}
			if len(query.Criteria) != len(testCase.wantColumns) {
				t.Fatalf("expected %d criteria, got %d", len(testCase.wantColumns), len(query.Criteria))
			}
			for i, wantColumn := range testCase.wantColumns {
				if actual := query.Criteria[i].Column; actual != wantColumn {
					t.Errorf("criterion %q: expected column %q, got %q", query.Criteria[i].Name, wantColumn, actual)
				}
			}
		})
	}
}
