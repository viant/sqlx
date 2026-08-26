package metadata

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/product/oracle"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/option"
)

func TestOracleMetadataCriteriaExpansion(t *testing.T) {
	testCases := []struct {
		name       string
		kind       info.Kind
		args       []interface{}
		wantFilter string
		wantArgs   []interface{}
	}{
		{
			name:       "tables append schema filter",
			kind:       info.KindTables,
			args:       []interface{}{"", "APP"},
			wantFilter: "WHERE T.OWNER=?",
			wantArgs:   []interface{}{"APP"},
		},
		{
			name:       "indexes replace WHERE marker before grouping",
			kind:       info.KindIndexes,
			args:       []interface{}{"", "APP", "ORDERS"},
			wantFilter: "WHERE I.TABLE_OWNER=? AND I.TABLE_NAME=?",
			wantArgs:   []interface{}{"APP", "ORDERS"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			queries := registry.Lookup(oracle.Oracle().Name, testCase.kind)
			query := queries.Match(oracle.Oracle())
			if query == nil {
				t.Fatalf("missing Oracle query for %v", testCase.kind)
			}

			actualSQL, actualArgs, err := prepareSQL(query, func() string { return "?" }, option.NewArgs(testCase.args...))
			if err != nil {
				t.Fatalf("prepareSQL failed: %v", err)
			}
			if !strings.Contains(actualSQL, testCase.wantFilter) {
				t.Fatalf("expected SQL to contain %q, got:\n%s", testCase.wantFilter, actualSQL)
			}
			if strings.Contains(actualSQL, "$WHERE") {
				t.Fatalf("unexpanded WHERE marker in SQL:\n%s", actualSQL)
			}
			if !reflect.DeepEqual(testCase.wantArgs, actualArgs) {
				t.Fatalf("expected args %v, got %v", testCase.wantArgs, actualArgs)
			}
		})
	}
}
