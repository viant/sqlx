package validator_test

import (
	"context"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
)

type candidateCoverageRow struct {
	ID       int    `sqlx:"id,primaryKey"`
	Required *int   `sqlx:"required,required"`
	Parent   *int   `sqlx:"parent,refColumn=id,refTable=parents"`
	Name     string `sqlx:"name,unique,table=records"`
}

func TestCandidatePoliciesPerCandidateCoverageSQLite(t *testing.T) {
	parent := 404
	h := sqlite.New(t,
		"CREATE TABLE parents(id INTEGER PRIMARY KEY)",
		"CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT UNIQUE,required INTEGER,parent INTEGER)",
		"INSERT INTO records(id,name,required,parent) VALUES(1,'taken',1,1)",
	)
	rows := []*candidateCoverageRow{
		{ID: 2, Parent: &parent, Name: "taken"},
		{ID: 3, Parent: &parent, Name: "taken"},
	}
	result, err := validator.New().Validate(context.Background(), h.DB, rows,
		validator.WithShallow(true),
		validator.WithLocation("Rows"),
		validator.WithCandidatePolicies([]validator.CandidatePolicy{
			{FieldFilter: includeFields()},
			{FieldFilter: includeFields("Required", "Parent", "Name")},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Violations) != 3 {
		t.Fatalf("got %v, want 3 violations", result)
	}
	for _, violation := range result.Violations {
		if !strings.HasPrefix(violation.Location, "Rows[1].") {
			t.Fatalf("unexpected per-candidate coverage: %v", result)
		}
	}
}

func TestCandidatePoliciesRepeatedMissingReferencesSQLite(t *testing.T) {
	h := sqlite.New(t,
		"CREATE TABLE parents(id INTEGER PRIMARY KEY)",
		"INSERT INTO parents VALUES(1)",
	)
	h.DB.SetMaxOpenConns(1)
	conn, err := h.DB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = conn.Raw(func(connection any) error {
		connection.(*sqlite3.SQLiteConn).SetLimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 1)
		return nil
	})
	_ = conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		Parent *int `sqlx:"parent,refColumn=id,refTable=parents"`
	}
	missing, present, another := 404, 1, 405
	rows := []*row{{&missing}, {&present}, {&missing}, {&missing}, {&another}, {&present}}
	for _, explicit := range []bool{false, true} {
		name := "default coverage"
		if explicit {
			name = "candidate coverage"
		}
		t.Run(name, func(t *testing.T) {
			opts := []validator.Option{validator.WithShallow(true), validator.WithLocation("Rows"), validator.WithMaxPlaceholders(1)}
			want := []string{"Rows[0].Parent", "Rows[2].Parent", "Rows[3].Parent", "Rows[4].Parent"}
			if explicit {
				policies := make([]validator.CandidatePolicy, len(rows))
				policies[2].FieldFilter = includeFields()
				opts = append(opts, validator.WithCandidatePolicies(policies))
				want = []string{"Rows[0].Parent", "Rows[3].Parent", "Rows[4].Parent"}
			}
			result, err := validator.New().Validate(context.Background(), h.DB, rows, opts...)
			if err != nil {
				t.Fatal(err)
			}
			var locations []string
			for _, violation := range result.Violations {
				locations = append(locations, violation.Location)
				if violation.Check != "refKey" || violation.Field != "Parent" || (violation.Value != missing && violation.Value != another) {
					t.Fatalf("unexpected reference diagnostic: %+v", violation)
				}
			}
			if !result.Failed || !reflect.DeepEqual(locations, want) {
				t.Fatalf("got locations %v, want %v", locations, want)
			}
		})
	}
}

type candidateValueProbe struct{ calls *int }

func (v candidateValueProbe) Value() (driver.Value, error) {
	*v.calls++
	return int64(1), nil
}

type candidatePreflightRow struct {
	ID       int                 `sqlx:"id,primaryKey"`
	Required candidateValueProbe `sqlx:"required,required"`
	Name     string              `sqlx:"name,unique,table=records"`
}

func TestCandidatePoliciesPreviousTypePreflightSQLite(t *testing.T) {
	h := sqlite.New(t,
		"CREATE TABLE records(id INTEGER PRIMARY KEY,required INTEGER,name TEXT UNIQUE)",
		"INSERT INTO records VALUES(1,1,'taken')",
	)
	for _, rules := range []string{"none", "not null", "not null and unique"} {
		t.Run(rules, func(t *testing.T) {
			for _, prior := range []string{"nil", "same row nil pointer", "wrong row nil pointer", "nil map", "nil slice", "nil double pointer"} {
				t.Run(prior, func(t *testing.T) {
					filterCalls, valueCalls := 0, 0
					var row any = &candidatePlain{ID: 1}
					switch rules {
					case "not null":
						row = &requiredRow[candidateValueProbe]{Value: candidateValueProbe{&valueCalls}}
					case "not null and unique":
						row = &candidatePreflightRow{ID: 1, Required: candidateValueProbe{&valueCalls}, Name: "taken"}
					}
					var previous any
					valid := prior == "nil" || prior == "same row nil pointer"
					switch prior {
					case "same row nil pointer":
						previous = reflect.Zero(reflect.TypeOf(row)).Interface()
					case "wrong row nil pointer":
						previous = (*otherCandidatePlain)(nil)
					case "nil map":
						previous = map[string]int(nil)
					case "nil slice":
						previous = reflect.Zero(reflect.SliceOf(reflect.TypeOf(row))).Interface()
					case "nil double pointer":
						previous = reflect.Zero(reflect.PointerTo(reflect.TypeOf(row))).Interface()
					}
					policies := []validator.CandidatePolicy{{}, {Previous: previous, FieldFilter: func(string) bool {
						filterCalls++
						return true
					}}}
					// An invalid later policy must prevent checks on earlier rows too.
					result, err := validator.New().Validate(context.Background(), h.DB, []any{row, row},
						validator.WithShallow(true), validator.WithCandidatePolicies(policies))
					if !reflect.DeepEqual(policies[1].Previous, previous) {
						t.Fatal("preflight mutated the caller's policy slice")
					}
					if !valid {
						if err == nil || !strings.Contains(err.Error(), "previous row 1 type") {
							t.Fatalf("got %v, want previous type error", err)
						}
						if filterCalls != 0 || valueCalls != 0 {
							t.Fatalf("checks ran before preflight: filters=%d conversions=%d", filterCalls, valueCalls)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if rules != "none" && (filterCalls == 0 || valueCalls == 0) {
						t.Fatalf("accepted policy did not run checks: filters=%d conversions=%d", filterCalls, valueCalls)
					}
					if rules == "not null and unique" && len(result.Violations) != 2 {
						t.Fatalf("nil priors must participate as inserts: %v", result)
					}
				})
			}
		})
	}
}

func TestCandidatePoliciesEmptyElementTypes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rows  any
		valid bool
	}{
		{"primitive", []int{}, false},
		{"primitive pointer", []*int{}, false},
		{"nil primitive slice", []int(nil), false},
		{"pointer to primitive slice", &[]int{}, false},
		{"map", []map[string]int{}, false},
		{"nested slice", [][]candidatePlain{}, false},
		{"double row pointer", []**candidatePlain{}, false},
		{"interface pointer", []*any{}, false},
		{"struct", []candidatePlain{}, true},
		{"struct pointer", []*candidatePlain{}, true},
		{"nil struct slice", []candidatePlain(nil), true},
		{"pointer to struct slice", &[]candidatePlain{}, true},
		{"interface", []any{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validator.New().Validate(context.Background(), nil, tc.rows,
				validator.WithShallow(true), validator.WithCandidatePolicies(nil))
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "candidate element type") {
				t.Fatalf("got %v, want candidate element type error", err)
			}
		})
	}
	if _, err := validator.New().Validate(context.Background(), nil, []int{}); err != nil {
		t.Fatalf("changed empty batch behavior without candidate policies: %v", err)
	}
}

func TestCandidatePoliciesMixedDuplicateTuplesSQLite(t *testing.T) {
	one := 1
	h := sqlite.New(t,
		"CREATE TABLE records(id INTEGER PRIMARY KEY,tenant_id INTEGER,name TEXT,UNIQUE(tenant_id,name))",
		"INSERT INTO records VALUES(1,1,'old')",
	)
	rows := []*scopedUniqueRow{
		{ID: 1, Tenant: &one, Name: "new"},
		{ID: 2, Tenant: &one, Name: "new"},
	}
	result, err := validator.New().Validate(context.Background(), h.DB, rows,
		validator.WithShallow(true),
		validator.WithCandidatePolicies([]validator.CandidatePolicy{
			{Previous: &scopedUniqueRow{ID: 1, Tenant: &one, Name: "old"}},
			{},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Violations) != 2 {
		t.Fatalf("got %v, want duplicate violations for both candidates", result)
	}
}

func TestCandidatePoliciesSparseDependencyFallbackSQLite(t *testing.T) {
	one := 1
	h := sqlite.New(t,
		"CREATE TABLE records(id INTEGER PRIMARY KEY,tenant_id INTEGER,name TEXT,UNIQUE(tenant_id,name))",
		"INSERT INTO records VALUES(1,1,'old'),(2,1,'taken')",
	)
	row := &scopedUniqueRow{ID: 1, Name: "taken"}
	result, err := validator.New().Validate(context.Background(), h.DB, row,
		validator.WithShallow(true),
		validator.WithCandidatePolicies([]validator.CandidatePolicy{
			{Previous: &scopedUniqueRow{ID: 1, Tenant: &one, Name: "old"}, FieldFilter: includeFields("Name")},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Failed {
		t.Fatalf("sparse dependency did not fall back to previous tenant")
	}
}

func TestCandidatePoliciesScopedSwapsAndCollisionsSQLite(t *testing.T) {
	t.Run("cross scope swap allowed", func(t *testing.T) {
		one, two := 1, 2
		h := sqlite.New(t,
			"CREATE TABLE records(id INTEGER PRIMARY KEY,tenant_id INTEGER,name TEXT,UNIQUE(tenant_id,name))",
			"INSERT INTO records VALUES(1,1,'alpha'),(2,2,'beta')",
		)
		rows := []*scopedUniqueRow{
			{ID: 1, Tenant: &two},
			{ID: 2, Tenant: &one},
		}
		result, err := validator.New().Validate(context.Background(), h.DB, rows,
			validator.WithShallow(true),
			validator.WithCandidatePolicies([]validator.CandidatePolicy{
				{Previous: &scopedUniqueRow{ID: 1, Tenant: &one, Name: "alpha"}, FieldFilter: includeFields("Tenant")},
				{Previous: &scopedUniqueRow{ID: 2, Tenant: &two, Name: "beta"}, FieldFilter: includeFields("Tenant")},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		if result.Failed {
			t.Fatalf("got %v, want cross-scope swap allowed", result)
		}
	})

	t.Run("same scope swap rejects stored collisions", func(t *testing.T) {
		one := 1
		h := sqlite.New(t,
			"CREATE TABLE records(id INTEGER PRIMARY KEY,tenant_id INTEGER,name TEXT,UNIQUE(tenant_id,name))",
			"INSERT INTO records VALUES(1,1,'alpha'),(2,1,'beta')",
		)
		rows := []*scopedUniqueRow{
			{ID: 1, Tenant: &one, Name: "beta"},
			{ID: 2, Tenant: &one, Name: "alpha"},
		}
		result, err := validator.New().Validate(context.Background(), h.DB, rows,
			validator.WithShallow(true),
			validator.WithCandidatePolicies([]validator.CandidatePolicy{
				{Previous: &scopedUniqueRow{ID: 1, Tenant: &one, Name: "alpha"}, FieldFilter: includeFields("Name")},
				{Previous: &scopedUniqueRow{ID: 2, Tenant: &one, Name: "beta"}, FieldFilter: includeFields("Name")},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Violations) != 2 {
			t.Fatalf("got %v, want same-scope stored collisions", result)
		}
	})
}

func TestCandidatePoliciesTransactionVisibilitySQLite(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(tenant_id INTEGER,id INTEGER,name TEXT UNIQUE,PRIMARY KEY(tenant_id,id))")
	h.DB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(1,1,'pending')"); err != nil {
		t.Fatal(err)
	}
	result, err := validator.New().Validate(ctx, h.DB, &compositeUniqueRow{Tenant: 2, ID: 2, Name: "pending"},
		validator.WithTransaction(tx),
		validator.WithShallow(true),
		validator.WithCandidatePolicies([]validator.CandidatePolicy{{}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Failed {
		t.Fatalf("transaction-local unique row was not visible")
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(3,3,'still-open')"); err != nil {
		t.Fatalf("transaction no longer owned by caller: %v", err)
	}
}

func TestCandidatePoliciesPlaceholderBudgetSQLite(t *testing.T) {
	h := sqlite.New(t,
		"CREATE TABLE records(tenant_id INTEGER,id INTEGER,name TEXT UNIQUE,PRIMARY KEY(tenant_id,id))",
		"INSERT INTO records VALUES(1,0,'zero')",
	)
	_, err := validator.New().Validate(context.Background(), h.DB, &compositeUniqueRow{Tenant: 1, ID: 0, Name: "zero"},
		validator.WithShallow(true),
		validator.WithMaxPlaceholders(2),
		validator.WithCandidatePolicies([]validator.CandidatePolicy{
			{Previous: &compositeUniqueRow{Tenant: 1, ID: 0, Name: "zero"}},
		}),
	)
	if err == nil || !strings.Contains(err.Error(), "requires 3 placeholders") {
		t.Fatalf("got %v, want placeholder budget error", err)
	}
}

type candidatePlain struct {
	ID int `sqlx:"id"`
}
type otherCandidatePlain struct {
	ID int `sqlx:"id"`
}

func TestCandidatePoliciesPreflightGuards(t *testing.T) {
	var nilPrevious *candidatePlain
	tests := []struct {
		name     string
		rows     any
		options  []validator.Option
		errorHas string
	}{
		{
			name:     "empty count mismatch",
			rows:     []*candidatePlain{},
			options:  []validator.Option{validator.WithShallow(true), validator.WithCandidatePolicies([]validator.CandidatePolicy{{}})},
			errorHas: "policy count 1",
		},
		{
			name:     "empty requires shallow",
			rows:     []*candidatePlain{},
			options:  []validator.Option{validator.WithCandidatePolicies(nil)},
			errorHas: "requires shallow",
		},
		{
			name:     "previous conflict",
			rows:     []*candidatePlain{{ID: 1}},
			options:  []validator.Option{validator.WithShallow(true), validator.WithPrevious(nil), validator.WithCandidatePolicies([]validator.CandidatePolicy{{}})},
			errorHas: "WithPrevious",
		},
		{
			name:     "field filter conflict",
			rows:     []*candidatePlain{{ID: 1}},
			options:  []validator.Option{validator.WithShallow(true), validator.WithFieldFilter(nil), validator.WithCandidatePolicies([]validator.CandidatePolicy{{}})},
			errorHas: "WithFieldFilter",
		},
		{
			name:     "set marker conflict",
			rows:     []*candidatePlain{{ID: 1}},
			options:  []validator.Option{validator.WithShallow(true), validator.WithSetMarker(), validator.WithCandidatePolicies([]validator.CandidatePolicy{{}})},
			errorHas: "WithSetMarker",
		},
		{
			name:     "nil candidate",
			rows:     []any{(*candidatePlain)(nil)},
			options:  []validator.Option{validator.WithShallow(true), validator.WithCandidatePolicies([]validator.CandidatePolicy{{}})},
			errorHas: "candidate 0 is nil",
		},
		{
			name:     "heterogeneous candidate",
			rows:     []any{&candidatePlain{ID: 1}, &otherCandidatePlain{ID: 2}},
			options:  []validator.Option{validator.WithShallow(true), validator.WithCandidatePolicies([]validator.CandidatePolicy{{}, {}})},
			errorHas: "candidate 1 type",
		},
		{
			name:     "previous type mismatch before checks",
			rows:     []*candidatePlain{{ID: 1}},
			options:  []validator.Option{validator.WithShallow(true), validator.WithCandidatePolicies([]validator.CandidatePolicy{{Previous: &otherCandidatePlain{ID: 1}}})},
			errorHas: "previous row 0 type",
		},
		{
			name:    "typed nil previous is insert",
			rows:    []*candidatePlain{{ID: 1}},
			options: []validator.Option{validator.WithShallow(true), validator.WithCandidatePolicies([]validator.CandidatePolicy{{Previous: nilPrevious}})},
		},
		{
			name:    "empty exact count",
			rows:    []*candidatePlain{},
			options: []validator.Option{validator.WithShallow(true), validator.WithCandidatePolicies(nil)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validator.New().Validate(context.Background(), nil, tc.rows, tc.options...)
			if tc.errorHas == "" {
				if err != nil {
					t.Fatalf("got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.errorHas) {
				t.Fatalf("got %v, want %q", err, tc.errorHas)
			}
		})
	}
}

func includeFields(fields ...string) func(string) bool {
	included := map[string]bool{}
	for _, field := range fields {
		included[field] = true
	}
	return func(field string) bool { return included[field] }
}
