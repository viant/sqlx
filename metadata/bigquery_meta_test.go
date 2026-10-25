package metadata

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/product/bigquery"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/option"
)

func TestBigQueryMetadataQualification(t *testing.T) {
	testCases := []struct {
		name       string
		kind       info.Kind
		args       []interface{}
		wantSource string
		wantFilter string
		wantArgs   []interface{}
	}{
		{
			name:       "schemas use catalog as project qualifier",
			kind:       info.KindSchemas,
			args:       []interface{}{"analytics-project"},
			wantSource: "FROM `analytics-project.INFORMATION_SCHEMA.SCHEMATA`",
			wantArgs:   []interface{}{},
		},
		{
			name:       "schema uses project qualifier and schema filter",
			kind:       info.KindSchema,
			args:       []interface{}{"analytics-project", "sales"},
			wantSource: "FROM `analytics-project.INFORMATION_SCHEMA.SCHEMATA`",
			wantFilter: "WHERE SCHEMA_NAME=?",
			wantArgs:   []interface{}{"sales"},
		},
		{
			name:       "tables use dataset qualifier",
			kind:       info.KindTables,
			args:       []interface{}{"analytics-project", "sales"},
			wantSource: "FROM `analytics-project.sales.INFORMATION_SCHEMA.TABLES`",
			wantArgs:   []interface{}{},
		},
		{
			name:       "table uses project and dataset qualifiers and table filter",
			kind:       info.KindTable,
			args:       []interface{}{"analytics-project", "sales", "orders"},
			wantSource: "FROM `analytics-project.sales.INFORMATION_SCHEMA.COLUMNS`",
			wantFilter: "WHERE TABLE_NAME=?",
			wantArgs:   []interface{}{"orders"},
		},
		{
			name:       "same dataset in another project keeps explicit catalog",
			kind:       info.KindTable,
			args:       []interface{}{"other-project", "sales", "orders"},
			wantSource: "FROM `other-project.sales.INFORMATION_SCHEMA.COLUMNS`",
			wantFilter: "WHERE TABLE_NAME=?",
			wantArgs:   []interface{}{"orders"},
		},
		{
			name:       "hyphen followed by digit in project",
			kind:       info.KindTable,
			args:       []interface{}{"viant-1p-connect", "api_data", "first_party_conversion_data"},
			wantSource: "FROM `viant-1p-connect.api_data.INFORMATION_SCHEMA.COLUMNS`",
			wantFilter: "WHERE TABLE_NAME=?",
			wantArgs:   []interface{}{"first_party_conversion_data"},
		},
		{
			name:       "dataset without explicit project keeps default project semantics",
			kind:       info.KindTable,
			args:       []interface{}{"", "sales", "orders"},
			wantSource: "FROM `sales.INFORMATION_SCHEMA.COLUMNS`",
			wantFilter: "WHERE TABLE_NAME=?",
			wantArgs:   []interface{}{"orders"},
		},
		{
			name:       "tables without catalog use default project",
			kind:       info.KindTables,
			args:       []interface{}{"", "sales"},
			wantSource: "FROM `sales.INFORMATION_SCHEMA.TABLES`",
			wantArgs:   []interface{}{},
		},
		{
			name:       "table filter remains a bound value",
			kind:       info.KindTable,
			args:       []interface{}{"viant-1p-connect", "api_data", "orders' OR 1=1 --"},
			wantSource: "FROM `viant-1p-connect.api_data.INFORMATION_SCHEMA.COLUMNS`",
			wantFilter: "WHERE TABLE_NAME=?",
			wantArgs:   []interface{}{"orders' OR 1=1 --"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			queries := registry.Lookup(bigquery.BigQuery().Name, testCase.kind)
			query := queries.Match(bigquery.BigQuery())
			if query == nil {
				t.Fatalf("missing BigQuery query for %v", testCase.kind)
			}

			actualSQL, actualArgs, err := prepareSQL(query, func() string { return "?" }, option.NewArgs(testCase.args...))
			if err != nil {
				t.Fatalf("prepareSQL failed: %v", err)
			}
			if !strings.Contains(actualSQL, testCase.wantSource) {
				t.Fatalf("expected SQL to contain %q, got:\n%s", testCase.wantSource, actualSQL)
			}
			if testCase.wantFilter != "" && !strings.Contains(actualSQL, testCase.wantFilter) {
				t.Fatalf("expected SQL to contain %q, got:\n%s", testCase.wantFilter, actualSQL)
			}
			if strings.Contains(actualSQL, "$Args[") {
				t.Fatalf("unexpanded source qualifier in SQL:\n%s", actualSQL)
			}
			if !reflect.DeepEqual(testCase.wantArgs, actualArgs) {
				t.Fatalf("expected args %v, got %v", testCase.wantArgs, actualArgs)
			}
		})
	}
}

func TestBigQueryMetadataRejectsIdentifierEscapes(t *testing.T) {
	query := registry.Lookup(bigquery.BigQuery().Name, info.KindTable).Match(bigquery.BigQuery())
	for _, bad := range []string{"bad`project", "bad\\project", "bad\nproject", "bad\x00project", string([]byte{0xff})} {
		for _, args := range [][]interface{}{{bad, "sales", "orders"}, {"project", bad, "orders"}} {
			_, _, err := prepareSQL(query, func() string { return "?" }, option.NewArgs(args...))
			if err == nil {
				t.Fatal("unsafe catalog/dataset must be rejected before metadata dispatch")
			}
		}
	}
}

func TestBigQueryMetadataRequiresStringNamespaceArguments(t *testing.T) {
	query := registry.Lookup(bigquery.BigQuery().Name, info.KindTable).Match(bigquery.BigQuery())
	for _, args := range [][]interface{}{{"project"}, {42, "sales", "orders"}, {"project", nil, "orders"}} {
		_, _, err := prepareSQL(query, func() string { return "?" }, option.NewArgs(args...))
		if err == nil {
			t.Fatal("missing or non-string metadata namespace must fail before dispatch")
		}
	}
}
