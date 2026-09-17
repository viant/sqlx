package metadata

import (
	"reflect"
	"testing"

	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

func TestQueryOrderByCompatibility(t *testing.T) {
	product := database.Product{Name: "test"}
	query := info.NewQuery(info.KindTables, "SELECT * FROM TABLES", product,
		info.NewCriterion(info.Catalog, ""),
		info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
	)
	if query.OrderBy != "" {
		t.Fatalf("NewQuery OrderBy: got %q, want empty", query.OrderBy)
	}

	query.OrderBy = "TABLE_NAME"
	if query.OrderBy != "TABLE_NAME" {
		t.Fatalf("OrderBy was not retained: got %q", query.OrderBy)
	}
	keyed := info.Query{Kind: info.KindVersion, SQL: "SELECT 1", OrderBy: "VERSION"}
	if keyed.OrderBy != "VERSION" {
		t.Fatalf("keyed literal OrderBy was not retained: got %q", keyed.OrderBy)
	}

	withoutOrder := *query
	withoutOrder.OrderBy = ""
	wantSQL, wantArgs, err := prepareSQL(&withoutOrder, func() string { return "?" }, option.NewArgs("", "APP"))
	if err != nil {
		t.Fatalf("prepareSQL without OrderBy failed: %v", err)
	}
	actualSQL, actualArgs, err := prepareSQL(query, func() string { return "?" }, option.NewArgs("", "APP"))
	if err != nil {
		t.Fatalf("prepareSQL with OrderBy failed: %v", err)
	}
	if actualSQL != wantSQL || !reflect.DeepEqual(actualArgs, wantArgs) {
		t.Fatalf("OrderBy changed prepared query: got (%q, %v), want (%q, %v)", actualSQL, actualArgs, wantSQL, wantArgs)
	}
}
