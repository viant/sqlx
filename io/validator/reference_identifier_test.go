package validator

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

func TestReferenceBigQueryIdentity(t *testing.T) {
	bq := &info.Dialect{Product: database.Product{Name: "BigQuery"}}
	for _, table := range []string{"`project.dataset.table`", "[project.dataset.table]", "[project:dataset.table]"} {
		native := Reference{Field: "Parent", Table: table, Column: "id"}
		for _, tc := range []struct {
			schema, table string
			want          bool
		}{
			{"", "project.dataset.table", true}, {"", "`project.dataset.table`", true}, {"", "[project:dataset.table]", true},
			{"project.dataset", "table", true}, {"`project.dataset`", "table", true}, {"project", "dataset.table", true},
			{"", "other.dataset.table", false}, {"", "project.other.table", false}, {"", "dataset.table", false}, {"", "table", false},
			{"other.dataset", "table", false}, {"project.other", "table", false},
		} {
			match, err := native.MatchesTarget(Reference{Field: "Parent", Schema: tc.schema, Table: tc.table, Column: "id"}, bq)
			if err != nil || match != tc.want {
				t.Fatalf("%s vs %s/%s: %v %v", table, tc.schema, tc.table, match, err)
			}
		}
	}
	native := Reference{Field: "Parent", Table: "[project.dataset.table]", Column: "id"}
	match, err := native.MatchesTarget(Reference{Field: "Parent", Table: "project.dataset.table", Column: "id"}, &info.Dialect{Product: database.Product{Name: "SQLServer"}})
	if err != nil || match {
		t.Fatalf("SQL Server literal dot split: %v %v", match, err)
	}
	native = Reference{Field: "Parent", Schema: `"main"`, Table: `"odd.table"`, Column: "id"}
	match, err = native.MatchesTarget(Reference{Field: "Parent", Table: `main."odd.table"`, Column: "id"}, nil)
	if err != nil || !match {
		t.Fatalf("ordinary quoted parts: %v %v", match, err)
	}
	match, err = native.MatchesTarget(Reference{Field: "Parent", Table: `main.odd.table`, Column: "id"}, nil)
	if err != nil || match {
		t.Fatalf("ordinary literal dot split: %v %v", match, err)
	}
}

func TestAuthoredQualifiedConstraintSQL(t *testing.T) {
	for _, table := range []string{"`project.dataset.table`", "[project.dataset.table]", "[project:dataset.table]", `"odd.table"`, "`my-project.dataset.table`"} {
		tag := "id,unique,table=" + table + ",refTable=" + table + ",refColumn=id"
		typ := reflect.StructOf([]reflect.StructField{{Name: "ID", Type: reflect.TypeOf(0), Tag: reflect.StructTag("sqlx:" + strconv.Quote(tag))}})
		checks, err := NewChecks(typ, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, check := range []*Check{checks.Unique[0], checks.RefKey[0]} {
			if !strings.Contains(check.SQL, "FROM "+table+" WHERE") {
				t.Fatalf("lost authored SQL: %s", check.SQL)
			}
		}
		if checks.RefKey[0].Reference.Table != table {
			t.Fatalf("lost stored table: %+v", checks.RefKey[0].Reference)
		}
	}
}

func TestAuthoredSeparatedSchemaSQL(t *testing.T) {
	for _, tc := range []struct{ schema, table, want string }{
		{"project.dataset", "parents", "project.dataset.parents"},
		{"project", "dataset.parents", "project.dataset.parents"},
		{`"main"`, `"odd.parents"`, `"main"."odd.parents"`},
	} {
		value := "id,unique,db=" + tc.schema + ",table=" + tc.table + ",refDb=" + tc.schema + ",refTable=" + tc.table + ",refColumn=id"
		typ := reflect.StructOf([]reflect.StructField{{Name: "ID", Type: reflect.TypeOf(0), Tag: reflect.StructTag("sqlx:" + strconv.Quote(value))}})
		checks, err := NewChecks(typ, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, check := range []*Check{checks.Unique[0], checks.RefKey[0]} {
			if !strings.Contains(check.SQL, "FROM "+tc.want+" WHERE") {
				t.Fatal(check.SQL)
			}
		}
	}
}
