package validator_test

import (
	"context"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
	"testing"
)

func TestSchemaQualifiedConstraintsSQLite(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "INSERT INTO parents VALUES(7)", "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT UNIQUE)", "INSERT INTO records VALUES(1,'taken')")
	type row struct {
		ID     int    `sqlx:"id,primaryKey"`
		Parent int    `sqlx:"parent_id,refDb=main,refTable=parents,refColumn=id"`
		Name   string `sqlx:"name,db=main,table=records,unique"`
	}
	for _, tc := range []struct {
		name  string
		value row
		want  int
	}{
		{"valid", row{ID: 2, Parent: 7, Name: "new"}, 0},
		{"missing parent", row{ID: 2, Parent: 8, Name: "new"}, 1},
		{"duplicate", row{ID: 2, Parent: 7, Name: "taken"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := validator.New().Validate(context.Background(), h.DB, &tc.value, validator.WithShallow(true))
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Violations) != tc.want {
				t.Fatalf("violations=%+v", result.Violations)
			}
		})
	}
}
