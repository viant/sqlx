package validator_test

import (
	"context"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
)

func TestExplicitValidationCoverageDoesNotChangePresence(t *testing.T) {
	for _, tc := range []struct {
		name            string
		marked, include bool
		failed          bool
	}{
		{"backfilled null checked despite omitted marker", false, true, true},
		{"explicit coverage excludes marked field", true, false, false},
		{"covered marked null fails", true, true, true},
		{"uncovered omission skipped", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := &requiredRow[*int]{Has: &requiredPresence{Value: tc.marked}}
			marker := row.Has
			result, err := validator.New().Validate(context.Background(), nil, row, validator.WithShallow(true), validator.WithSetMarker(), validator.WithFieldFilter(func(name string) bool {
				if name != "Value" {
					t.Fatalf("noncanonical field %q", name)
				}
				return tc.include
			}))
			if err != nil {
				t.Fatal(err)
			}
			if result.Failed != tc.failed {
				t.Fatalf("got %v want failed=%v", result, tc.failed)
			}
			if row.Has != marker || row.Has.Value != tc.marked {
				t.Fatal("validation changed persistence presence")
			}
		})
	}
}

func TestExplicitValidationCoverageSQLChecks(t *testing.T) {
	for _, kind := range []string{"reference", "unique", "previous unique"} {
		for _, include := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: " excluded", true: " included"}[include], func(t *testing.T) {
				h := sqlite.New(t, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE records(tenant_id INTEGER,id INTEGER,name TEXT UNIQUE,PRIMARY KEY(tenant_id,id))", "INSERT INTO records VALUES(1,0,'taken')")
				var row any = &compositeUniqueRow{Tenant: 2, ID: 2, Name: "taken"}
				opts := []validator.Option{validator.WithShallow(true), validator.WithFieldFilter(func(string) bool { return include })}
				if kind == "reference" {
					row = &transactionRef{Parent: 7}
				}
				if kind == "previous unique" {
					opts = append(opts, validator.WithPrevious(nil))
				}
				result, err := validator.New().Validate(context.Background(), h.DB, row, opts...)
				if err != nil {
					t.Fatal(err)
				}
				if result.Failed != include {
					t.Fatalf("got %v want failed=%v", result, include)
				}
			})
		}
	}
}
