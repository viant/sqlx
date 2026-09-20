package validator_test

import (
	"context"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
)

func TestQuotedSchemaConstraintsSQLite(t *testing.T) {
	h := sqlite.New(t, `CREATE TABLE "odd.parents"(id INTEGER PRIMARY KEY)`, `INSERT INTO "odd.parents" VALUES(7)`, `CREATE TABLE "odd.records"(id INTEGER PRIMARY KEY,name TEXT UNIQUE)`, `INSERT INTO "odd.records" VALUES(1,'taken')`)
	type row struct {
		ID     int    `sqlx:"id,primaryKey"`
		Parent int    `sqlx:"parent_id,refDb=\"main\",refTable=\"odd.parents\",refColumn=id"`
		Name   string `sqlx:"name,db=\"main\",table=\"odd.records\",unique"`
	}
	for _, tc := range []struct {
		value row
		want  string
	}{
		{row{2, 7, "new"}, ""}, {row{2, 8, "new"}, "refKey"}, {row{2, 7, "taken"}, "unique"},
	} {
		result, err := validator.New().Validate(context.Background(), h.DB, &tc.value, validator.WithShallow(true))
		if err != nil {
			t.Fatal(err)
		}
		if tc.want == "" {
			if len(result.Violations) != 0 {
				t.Fatal(result.Violations)
			}
			continue
		}
		if len(result.Violations) != 1 || result.Violations[0].Check != tc.want {
			t.Fatalf("violations=%+v want=%s", result.Violations, tc.want)
		}
	}
}
