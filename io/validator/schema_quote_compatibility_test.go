package validator_test

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
)

func schemaQuoteRow(schema string, parent int64, name string) any {
	typ := reflect.StructOf([]reflect.StructField{
		{Name: "ID", Type: reflect.TypeOf(int64(0)), Tag: `sqlx:"id,primaryKey"`},
		{Name: "Parent", Type: reflect.TypeOf(int64(0)), Tag: reflect.StructTag("sqlx:" + strconv.Quote("parent_id,refDb="+schema+",refTable=parents,refColumn=id"))},
		{Name: "Name", Type: reflect.TypeOf(""), Tag: reflect.StructTag("sqlx:" + strconv.Quote("name,db="+schema+",table=records,unique"))},
	})
	row := reflect.New(typ)
	row.Elem().Field(0).SetInt(2)
	row.Elem().Field(1).SetInt(parent)
	row.Elem().Field(2).SetString(name)
	return row.Interface()
}

func TestSchemaQuotesPreserveSimpleTargetSQLite(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "INSERT INTO parents VALUES(7)", "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT UNIQUE)", "INSERT INTO records VALUES(1,'taken')")
	for _, schema := range []string{"main", `"main"`, "`main`", "[main]"} {
		for _, tc := range []struct {
			parent      int64
			name, check string
		}{{7, "new", ""}, {8, "new", "refKey"}, {7, "taken", "unique"}} {
			row := schemaQuoteRow(schema, tc.parent, tc.name)
			checks, err := validator.NewChecks(reflect.TypeOf(row), nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("authored schema=%q stored schema=%q SQL=%s", schema, checks.RefKey[0].Reference.Schema, checks.RefKey[0].SQL)
			result, err := validator.New().Validate(context.Background(), h.DB, row, validator.WithShallow(true))
			if err != nil {
				t.Fatalf("%s: %v", schema, err)
			}
			if tc.check == "" {
				if len(result.Violations) != 0 {
					t.Fatal(result.Violations)
				}
				continue
			}
			if len(result.Violations) != 1 || result.Violations[0].Check != tc.check {
				t.Fatalf("%s: %+v", schema, result.Violations)
			}
		}
	}
}

func TestQuotedDottedSchemaRequiresDelimiterSQLite(t *testing.T) {
	h := sqlite.New(t, `ATTACH DATABASE ':memory:' AS "odd.schema"`, `CREATE TABLE "odd.schema".parents(id INTEGER PRIMARY KEY)`, `INSERT INTO "odd.schema".parents VALUES(7)`, `CREATE TABLE "odd.schema".records(id INTEGER PRIMARY KEY,name TEXT UNIQUE)`, `INSERT INTO "odd.schema".records VALUES(1,'taken')`)
	// ATTACH is connection-local; keep discovery and validation on that connection.
	h.DB.SetMaxOpenConns(1)
	for _, tc := range []struct {
		parent      int64
		name, check string
	}{{7, "new", ""}, {8, "new", "refKey"}, {7, "taken", "unique"}} {
		row := schemaQuoteRow(`"odd.schema"`, tc.parent, tc.name)
		checks, err := validator.NewChecks(reflect.TypeOf(row), nil)
		if err != nil {
			t.Fatal(err)
		}
		result, err := validator.New().Validate(context.Background(), h.DB, row, validator.WithShallow(true))
		if err != nil {
			t.Fatal(err)
		}
		for _, check := range append(checks.Unique, checks.RefKey...) {
			if !strings.Contains(check.SQL, `FROM "odd.schema".`) {
				t.Fatalf("lost schema delimiter: %s", check.SQL)
			}
		}
		if tc.check == "" {
			if len(result.Violations) != 0 {
				t.Fatal(result.Violations)
			}
			continue
		}
		if len(result.Violations) != 1 || result.Violations[0].Check != tc.check {
			t.Fatalf("%+v", result.Violations)
		}
	}
	// The old decoded spelling is not the same SQL identifier.
	if _, err := h.DB.Exec(`SELECT id FROM odd.schema.parents`); err == nil {
		t.Fatal("stripped dotted schema unexpectedly resolved")
	}
}
