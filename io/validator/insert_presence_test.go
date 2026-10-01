package validator_test

import (
	"context"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
	"testing"
)

type insertScopedPresence struct {
	ID    int     `sqlx:"id,primaryKey"`
	Owner int     `sqlx:"owner,required"`
	Name  *string `sqlx:"name,uniqueDep=owner,table=records"`
}

func TestInsertFilteredUniqueUsesCurrentZeroDependency(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY,owner INTEGER NOT NULL,name TEXT,UNIQUE(owner,name))", "INSERT INTO records VALUES(1,7,'existing'),(2,0,'zero-existing')")
	name := "existing"
	zeroExisting := "zero-existing"
	for _, tc := range []struct {
		name   string
		row    insertScopedPresence
		fields func(string) bool
		want   int
	}{{"omitted owner zero", insertScopedPresence{Name: &name}, includeFields("Name"), 0}, {"zero dependency collision", insertScopedPresence{Name: &zeroExisting}, includeFields("Name"), 1}, {"supplied owner collision", insertScopedPresence{Owner: 7, Name: &name}, includeFields("Name", "Owner"), 1}, {"omitted unique field", insertScopedPresence{Owner: 7}, includeFields("Owner"), 0}, {"explicit null unique field", insertScopedPresence{}, includeFields("Name"), 0}} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := validator.New().Validate(context.Background(), h.DB, &tc.row, validator.WithShallow(true), validator.WithCandidatePolicies([]validator.CandidatePolicy{{FieldFilter: tc.fields}}))
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Violations) != tc.want {
				t.Fatalf("got %+v", result)
			}
		})
	}
}
