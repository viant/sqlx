package validator_test

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
)

type producerRow struct {
	ID     int     `sqlx:"id,primaryKey"`
	Tenant *int    `sqlx:"tenant_id,required"`
	Name   *string `sqlx:"name,required,unique,uniqueDep=tenant_id,table=records,refColumn=code,refTable=parents"`
	Other  *int    `sqlx:"other,refColumn=id,refTable=others"`
}

func TestProducerPolicyIndependentChecksSQLite(t *testing.T) {
	h := sqlite.New(t,
		"CREATE TABLE records(id INTEGER PRIMARY KEY,tenant_id INTEGER,name TEXT,UNIQUE(tenant_id,name))",
		"INSERT INTO records VALUES(1,1,'taken')",
		"CREATE TABLE parents(code TEXT)",
		"CREATE TABLE others(id INTEGER)",
	)
	one, missing, taken := 1, 404, "taken"
	receipt := validator.Reference{Field: "Name", Table: "parents", Column: "code"}
	for _, tc := range []struct {
		name   string
		row    *producerRow
		policy validator.CandidatePolicy
		want   []string
	}{
		{"deferred dependency retains value required", &producerRow{Other: &missing},
			validator.CandidatePolicy{DeferredFields: includeFields("Tenant")},
			[]string{"notnull:Rows[0].Name", "refKey:Rows[0].Other"}},
		{"deferred dependency retains supplied value reference", &producerRow{Name: &taken, Other: &missing},
			validator.CandidatePolicy{DeferredFields: includeFields("Tenant")},
			[]string{"refKey:Rows[0].Name", "refKey:Rows[0].Other"}},
		{"deferred value retains unrelated checks", &producerRow{Other: &missing},
			validator.CandidatePolicy{DeferredFields: includeFields("Name")},
			[]string{"notnull:Rows[0].Tenant", "refKey:Rows[0].Other"}},
		{"unique and reference on same column", &producerRow{Tenant: &one, Name: &taken},
			validator.CandidatePolicy{},
			[]string{"refKey:Rows[0].Name", "unique:Rows[0].Name"}},
		{"exact receipt retains unique and other reference", &producerRow{Tenant: &one, Name: &taken, Other: &missing},
			validator.CandidatePolicy{SatisfiedReferences: []validator.Reference{receipt}},
			[]string{"refKey:Rows[0].Other", "unique:Rows[0].Name"}},
		{"receipt retains required", &producerRow{Tenant: &one},
			validator.CandidatePolicy{SatisfiedReferences: []validator.Reference{receipt}},
			[]string{"notnull:Rows[0].Name"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := validator.New().Validate(context.Background(), h.DB, []*producerRow{tc.row},
				validator.WithShallow(true), validator.WithLocation("Rows"),
				validator.WithCandidatePolicies([]validator.CandidatePolicy{tc.policy}))
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, violation := range result.Violations {
				got = append(got, violation.Check+":"+violation.Location)
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
	t.Run("both checks without candidate policies", func(t *testing.T) {
		result, err := validator.New().Validate(context.Background(), h.DB,
			&producerRow{ID: 2, Tenant: &one, Name: &taken}, validator.WithShallow(true))
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Violations) != 2 || result.Violations[0].Check != "unique" || result.Violations[1].Check != "refKey" {
			t.Fatalf("both independent checks must run for existing callers: %v", result)
		}
	})
}

func TestProducerPolicyDeferralBeforeUniqueInputs(t *testing.T) {
	type row struct {
		Tenant candidateValueProbe `sqlx:"tenant_id"`
		Name   candidateValueProbe `sqlx:"name,unique,uniqueDep=tenant_id,table=records"`
	}
	for _, field := range []string{"Tenant", "Name"} {
		t.Run(field, func(t *testing.T) {
			calls, filters := 0, 0
			current := &row{candidateValueProbe{&calls}, candidateValueProbe{&calls}}
			result, err := validator.New().Validate(context.Background(), nil, []*row{current, current},
				validator.WithShallow(true), validator.WithCandidatePolicies([]validator.CandidatePolicy{
					{DeferredFields: includeFields(field), FieldFilter: func(string) bool { filters++; return false }},
					{DeferredFields: includeFields(field)},
				}))
			if err != nil || result.Failed || calls != 0 || filters != 0 {
				t.Fatalf("deferral must precede coverage, fallback, conversion, queries and duplicates: result=%v err=%v conversions=%d filters=%d", result, err, calls, filters)
			}
		})
	}
}

func TestProducerPolicyReferencesCompiledIdentity(t *testing.T) {
	type row struct {
		Parent int `sqlx:"parent_id,refDb=catalog,refTable=parents,refColumn=id"`
	}
	checks, err := validator.NewChecks(reflect.TypeOf(row{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	exact := validator.Reference{Field: "Parent", Schema: "catalog", Table: "parents", Column: "id"}
	if len(checks.RefKey) != 1 || checks.RefKey[0].Reference != exact {
		t.Fatalf("incorrect compiled identity: %+v", checks.RefKey)
	}
	checks.RefKey[0].SQL = "not SQL: identity must come from metadata"
	for _, tc := range []struct {
		name  string
		refs  []validator.Reference
		valid bool
	}{
		{"empty", nil, true},
		{"exact", []validator.Reference{exact}, true},
		{"duplicate", []validator.Reference{exact, exact}, false},
		{"empty schema is distinct", []validator.Reference{{Field: "Parent", Table: "parents", Column: "id"}}, false},
		{"wrong table", []validator.Reference{{Field: "Parent", Schema: "catalog", Table: "other", Column: "id"}}, false},
		{"wrong column", []validator.Reference{{Field: "Parent", Schema: "catalog", Table: "parents", Column: "other"}}, false},
		{"column name is not Go field", []validator.Reference{{Field: "parent_id", Schema: "catalog", Table: "parents", Column: "id"}}, false},
		{"case is exact", []validator.Reference{{Field: "parent", Schema: "catalog", Table: "parents", Column: "id"}}, false},
		{"zero descriptor", []validator.Reference{{}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := checks.ValidateReferences(tc.refs); (err == nil) != tc.valid {
				t.Fatalf("valid=%v, got %v", tc.valid, err)
			}
		})
	}
}

func TestProducerPolicyPreflightBeforeCallbacks(t *testing.T) {
	type row struct {
		Required candidateValueProbe `sqlx:"required,required"`
		Name     string              `sqlx:"name,unique,table=records,refColumn=code,refTable=parents"`
	}
	exact := validator.Reference{Field: "Name", Table: "parents", Column: "code"}
	for _, tc := range []struct {
		name     string
		policy   validator.CandidatePolicy
		errorHas string
	}{
		{"update deferral even false", validator.CandidatePolicy{Previous: &row{}, DeferredFields: includeFields()}, "DeferredFields requires"},
		{"update receipt", validator.CandidatePolicy{Previous: &row{}, SatisfiedReferences: []validator.Reference{exact}}, "SatisfiedReferences requires"},
		{"receipt and deferral even false", validator.CandidatePolicy{DeferredFields: includeFields(), SatisfiedReferences: []validator.Reference{exact}}, "SatisfiedReferences requires"},
		{"normalized insert receipt and deferral", validator.CandidatePolicy{Previous: (*row)(nil), DeferredFields: includeFields(), SatisfiedReferences: []validator.Reference{exact}}, "SatisfiedReferences requires"},
		{"wrong typed nil prior", validator.CandidatePolicy{Previous: (*producerRow)(nil), DeferredFields: includeFields()}, "previous row 1 type"},
		{"nil map prior", validator.CandidatePolicy{Previous: map[string]int(nil), SatisfiedReferences: []validator.Reference{exact}}, "previous row 1 type"},
		{"unknown field", validator.CandidatePolicy{SatisfiedReferences: []validator.Reference{{Field: "Missing", Table: "parents", Column: "code"}}}, "unknown reference"},
		{"non reference field", validator.CandidatePolicy{SatisfiedReferences: []validator.Reference{{Field: "Required", Table: "parents", Column: "code"}}}, "unknown reference"},
		{"wrong schema", validator.CandidatePolicy{SatisfiedReferences: []validator.Reference{{Field: "Name", Schema: "main", Table: "parents", Column: "code"}}}, "unknown reference"},
		{"wrong table", validator.CandidatePolicy{SatisfiedReferences: []validator.Reference{{Field: "Name", Table: "other", Column: "code"}}}, "unknown reference"},
		{"wrong column", validator.CandidatePolicy{SatisfiedReferences: []validator.Reference{{Field: "Name", Table: "parents", Column: "id"}}}, "unknown reference"},
		{"duplicate", validator.CandidatePolicy{SatisfiedReferences: []validator.Reference{exact, exact}}, "duplicate reference"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, enabled := range []bool{false, true} {
				calls, filters, deferrals := 0, 0, 0
				current := &row{Required: candidateValueProbe{&calls}}
				policy := tc.policy
				policy.FieldFilter = func(string) bool { filters++; return true }
				if policy.DeferredFields != nil {
					policy.DeferredFields = func(string) bool { deferrals++; return false }
				}
				_, err := validator.New().Validate(context.Background(), nil, []*row{current, current},
					validator.WithShallow(true), validator.WithUnique(enabled), validator.WithRef(enabled),
					validator.WithCandidatePolicies([]validator.CandidatePolicy{{FieldFilter: policy.FieldFilter}, policy}))
				if err == nil || !strings.Contains(err.Error(), tc.errorHas) {
					t.Fatalf("got %v, want %q", err, tc.errorHas)
				}
				if calls != 0 || filters != 0 || deferrals != 0 {
					t.Fatalf("preflight ran callbacks: conversions=%d filters=%d deferrals=%d", calls, filters, deferrals)
				}
			}
		})
	}
	t.Run("no rules still rejects receipts", func(t *testing.T) {
		_, err := validator.New().Validate(context.Background(), nil, &candidatePlain{}, validator.WithShallow(true),
			validator.WithCandidatePolicies([]validator.CandidatePolicy{{SatisfiedReferences: []validator.Reference{exact}}}))
		if err == nil || !strings.Contains(err.Error(), "unknown reference") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("no rules still rejects update deferral", func(t *testing.T) {
		_, err := validator.New().Validate(context.Background(), nil, &candidatePlain{}, validator.WithShallow(true),
			validator.WithCandidatePolicies([]validator.CandidatePolicy{{Previous: &candidatePlain{}, DeferredFields: includeFields()}}))
		if err == nil || !strings.Contains(err.Error(), "DeferredFields requires") {
			t.Fatalf("got %v", err)
		}
	})
}

func TestProducerPolicyTypedNilInsert(t *testing.T) {
	type row struct {
		Parent *int `sqlx:"parent,required,refColumn=id,refTable=parents"`
	}
	for _, receipt := range []bool{false, true} {
		one := 1
		current := &row{}
		policy := validator.CandidatePolicy{Previous: (*row)(nil)}
		if receipt {
			current.Parent = &one
			policy.SatisfiedReferences = []validator.Reference{{Field: "Parent", Table: "parents", Column: "id"}}
		} else {
			policy.DeferredFields = includeFields("Parent")
		}
		policies := []validator.CandidatePolicy{policy}
		result, err := validator.New().Validate(context.Background(), nil, current,
			validator.WithShallow(true), validator.WithCandidatePolicies(policies))
		if err != nil || result.Failed {
			t.Fatalf("normalized insert rejected: %v %v", result, err)
		}
		if policies[0].Previous == nil {
			t.Fatal("normalization mutated caller policy")
		}
	}
}

func TestProducerPolicyPostpassWholeCohortSQLite(t *testing.T) {
	h := sqlite.New(t,
		"CREATE TABLE records(id INTEGER PRIMARY KEY,tenant_id INTEGER,name TEXT,UNIQUE(tenant_id,name))",
		"CREATE TABLE parents(code TEXT)",
		"INSERT INTO parents VALUES('shared')",
	)
	one, name := 1, "shared"
	rows := []*producerRow{{ID: 1, Name: &name}, {ID: 2, Tenant: &one, Name: &name}}
	service := validator.New()
	result, err := service.Validate(context.Background(), h.DB, rows,
		validator.WithShallow(true), validator.WithCandidatePolicies([]validator.CandidatePolicy{
			{DeferredFields: includeFields("Tenant")}, {},
		}))
	if err != nil || result.Failed {
		t.Fatalf("business pass: %v %v", result, err)
	}
	rows[0].Tenant = &one
	result, err = service.Validate(context.Background(), h.DB, rows,
		validator.WithShallow(true), validator.WithLocation("Rows"), validator.WithCandidatePolicies([]validator.CandidatePolicy{
			{SatisfiedReferences: []validator.Reference{{Field: "Name", Table: "parents", Column: "code"}}}, {},
		}))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Violations) != 2 {
		t.Fatalf("want produced/supplied collision on both candidates: %v", result)
	}
	for i, violation := range result.Violations {
		if violation.Check != "unique" || violation.Location != []string{"Rows[0].Name", "Rows[1].Name"}[i] {
			t.Fatalf("unexpected violation: %+v", violation)
		}
	}
}

func TestProducerPolicyReferenceTransactionAndChunksSQLite(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE parents(id INTEGER PRIMARY KEY)")
	h.DB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := h.DB.Conn(ctx)
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
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO parents VALUES(1),(2)"); err != nil {
		t.Fatal(err)
	}
	type row struct {
		Parent int `sqlx:"parent,refColumn=id,refTable=parents"`
	}
	rows := []*row{{404}, {404}, {1}, {405}, {2}, {404}, {405}}
	policies := make([]validator.CandidatePolicy, len(rows))
	policies[0].SatisfiedReferences = []validator.Reference{{Field: "Parent", Table: "parents", Column: "id"}}
	policies[3].DeferredFields = includeFields("Parent")
	result, err := validator.New().Validate(ctx, h.DB, rows, validator.WithTransaction(tx),
		validator.WithShallow(true), validator.WithMaxPlaceholders(1), validator.WithLocation("Rows"),
		validator.WithCandidatePolicies(policies))
	if err != nil {
		t.Fatal(err)
	}
	var locations []string
	for _, violation := range result.Violations {
		locations = append(locations, violation.Location)
	}
	if !reflect.DeepEqual(locations, []string{"Rows[1].Parent", "Rows[5].Parent", "Rows[6].Parent"}) {
		t.Fatalf("unexpected reference coverage/transaction visibility: %v", result)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO parents VALUES(3)"); err != nil {
		t.Fatalf("validator took transaction ownership: %v", err)
	}
}

func TestProducerPolicyUniqueTransactionAndBudgetSQLite(t *testing.T) {
	h := sqlite.New(t,
		"CREATE TABLE records(id INTEGER PRIMARY KEY,tenant_id INTEGER,name TEXT,UNIQUE(tenant_id,name))",
		"CREATE TABLE parents(code TEXT)",
	)
	h.DB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO records VALUES(1,1,'pending'),(2,1,'own')"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO parents VALUES('pending'),('own')"); err != nil {
		t.Fatal(err)
	}
	one, pending, own := 1, "pending", "own"
	rows := []*producerRow{
		{ID: 3, Name: &pending},
		{ID: 4, Tenant: &one, Name: &pending},
		{ID: 2, Name: &own},
	}
	policies := []validator.CandidatePolicy{
		{DeferredFields: includeFields("Tenant")},
		{SatisfiedReferences: []validator.Reference{{Field: "Name", Table: "parents", Column: "code"}}},
		{Previous: &producerRow{ID: 2, Tenant: &one, Name: &own}, FieldFilter: includeFields("Name")},
	}
	for _, limit := range []int{2, 3} {
		result, err := validator.New().Validate(ctx, h.DB, rows, validator.WithTransaction(tx),
			validator.WithShallow(true), validator.WithLocation("Rows"), validator.WithMaxPlaceholders(limit),
			validator.WithCandidatePolicies(policies))
		if limit == 2 {
			if err == nil || !strings.Contains(err.Error(), "requires 3 placeholders, limit is 2") {
				t.Fatalf("exact prior exclusion must count toward budget: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Violations) != 1 || result.Violations[0].Check != "unique" || result.Violations[0].Location != "Rows[1].Name" {
			t.Fatalf("unexpected deferral, receipt or prior exclusion behavior: %v", result)
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO parents VALUES('still-open')"); err != nil {
		t.Fatalf("validator took transaction ownership: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	type count struct{ N int }
	sqlite.AssertRows(t, h, "SELECT COUNT(*) AS N FROM records", []count{{}})
}
