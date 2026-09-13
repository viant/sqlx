package validator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
)

type compositeUniqueRow struct {
	Tenant int    `sqlx:"tenant_id,primaryKey"`
	ID     int    `sqlx:"id,primaryKey"`
	Name   string `sqlx:"name,unique,table=records"`
}

func TestUniquePreviousSQLite(t *testing.T) {
	tests := []struct {
		name              string
		current, previous []*compositeUniqueRow
		violations        int
		error             string
		limit             int
	}{
		{"unchanged explicit zero", []*compositeUniqueRow{{1, 0, "zero"}}, []*compositeUniqueRow{{1, 0, "zero"}}, 0, "", 0},
		{"working key changed uses previous", []*compositeUniqueRow{{8, 99, "zero"}}, []*compositeUniqueRow{{1, 0, "zero"}}, 0, "", 0},
		{"same first key different second collision", []*compositeUniqueRow{{1, 0, "one"}}, []*compositeUniqueRow{{1, 0, "zero"}}, 1, "", 0},
		{"same second key other tenant collision", []*compositeUniqueRow{{1, 0, "other"}}, []*compositeUniqueRow{{1, 0, "zero"}}, 1, "", 0},
		{"insert zero excludes nothing", []*compositeUniqueRow{{1, 0, "zero"}}, nil, 1, "", 0},
		{"insert unused", []*compositeUniqueRow{{1, 0, "new"}}, nil, 0, "", 0},
		{"cross candidate stored collisions", []*compositeUniqueRow{{1, 0, "one"}, {1, 1, "zero"}}, []*compositeUniqueRow{{1, 0, "zero"}, {1, 1, "one"}}, 2, "", 0},
		{"duplicate new names", []*compositeUniqueRow{{1, 2, "new"}, {2, 3, "new"}}, nil, 2, "", 0},
		{"mixed explicit aligned previous", []*compositeUniqueRow{{1, 0, "zero"}, {2, 3, "new"}}, []*compositeUniqueRow{{1, 0, "zero"}, nil}, 0, "", 0},
		{"alignment count", []*compositeUniqueRow{{1, 0, "zero"}, {2, 3, "new"}}, []*compositeUniqueRow{{1, 0, "zero"}}, 0, "count", 0},
		{"full tuple budget", []*compositeUniqueRow{{1, 0, "zero"}}, []*compositeUniqueRow{{1, 0, "zero"}}, 0, "requires 3 placeholders", 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(tenant_id INTEGER, id INTEGER, name TEXT UNIQUE, PRIMARY KEY(tenant_id,id))", "INSERT INTO records VALUES(1,0,'zero'),(1,1,'one'),(2,0,'other')")
			var previous any
			if tc.previous != nil {
				previous = tc.previous
			}
			result, err := validator.New().Validate(context.Background(), h.DB, tc.current, validator.WithPrevious(previous), validator.WithShallow(true), validator.WithLocation("Rows"), validator.WithMaxPlaceholders(tc.limit))
			if tc.error != "" {
				if err == nil || !strings.Contains(err.Error(), tc.error) {
					t.Fatalf("got %v want %s", err, tc.error)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Violations) != tc.violations {
				t.Fatalf("got %v, want %d violations", result, tc.violations)
			}
			sqlite.AssertRows(t, h, "SELECT tenant_id,id,name FROM records ORDER BY tenant_id,id", []compositeUniqueRow{{1, 0, "zero"}, {1, 1, "one"}, {2, 0, "other"}})
		})
	}
}

type nullablePreviousKey struct {
	Tenant *int   `sqlx:"tenant_id,primaryKey"`
	ID     *int   `sqlx:"id,primaryKey"`
	Name   string `sqlx:"name,unique,table=records"`
}

func TestUniquePreviousRejectsPartialKey(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(tenant_id INTEGER,id INTEGER,name TEXT UNIQUE,PRIMARY KEY(tenant_id,id))")
	id := 0
	_, err := validator.New().Validate(context.Background(), h.DB, &nullablePreviousKey{Name: "new"}, validator.WithPrevious(&nullablePreviousKey{Tenant: &id}), validator.WithShallow(true))
	if err == nil || !strings.Contains(err.Error(), "primary key id is null") {
		t.Fatalf("got %v", err)
	}
}
