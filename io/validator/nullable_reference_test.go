package validator_test

import (
	"context"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

type nullableReferenceRow struct {
	Scope  *string `sqlx:"scope_id,required=true,refTable=targets,refColumn=scope_id" refKey:"scope_id,parent_id"`
	Parent *string `sqlx:"parent_id,refTable=targets,refColumn=parent_id" refKey:"scope_id,parent_id"`
	Owner  *string `sqlx:"owner_id,refTable=owners,refColumn=id"`
}

func TestNullableReferenceGroupUsesEffectiveCandidate(t *testing.T) {
	text := func(s string) *string { return &s }
	for _, tc := range []struct {
		name          string
		row, previous *nullableReferenceRow
		include       func(string) bool
		violations    int
	}{
		{name: "nullable insert", row: &nullableReferenceRow{Scope: text("new")}},
		{name: "complete missing ref", row: &nullableReferenceRow{Scope: text("missing"), Parent: text("parent")}, violations: 1},
		{name: "unrelated missing ref", row: &nullableReferenceRow{Scope: text("new"), Owner: text("absent")}, violations: 1},
		{name: "required still enforced", row: &nullableReferenceRow{}, violations: 1},
		{name: "omitted sibling uses nonnull previous", row: &nullableReferenceRow{Scope: text("missing")}, previous: &nullableReferenceRow{Scope: text("old"), Parent: text("parent")}, include: func(s string) bool { return s == "Scope" }, violations: 1},
		{name: "omitted sibling uses null previous", row: &nullableReferenceRow{Scope: text("new")}, previous: &nullableReferenceRow{Scope: text("old")}, include: func(s string) bool { return s == "Scope" }},
		{name: "explicit null clears previous reference", row: &nullableReferenceRow{Scope: text("new")}, previous: &nullableReferenceRow{Scope: text("old"), Parent: text("parent")}},
		{name: "zero string is not null", row: &nullableReferenceRow{Scope: text("new"), Parent: text("")}, violations: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE targets(scope_id TEXT,parent_id TEXT,PRIMARY KEY(scope_id,parent_id))", "INSERT INTO targets VALUES('old','parent')", "CREATE TABLE owners(id TEXT PRIMARY KEY)")
			result, err := validator.New().Validate(context.Background(), h.DB, tc.row, validator.WithShallow(true), validator.WithCandidatePolicies([]validator.CandidatePolicy{{Previous: tc.previous, FieldFilter: tc.include}}))
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Violations) != tc.violations {
				t.Fatalf("violations=%v want%d", result, tc.violations)
			}
		})
	}
}

func TestSQLiteForeignKeyMetadataKeepsIndependentConstraintsDistinct(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE targets(id TEXT PRIMARY KEY)", "CREATE TABLE children(first_id TEXT,second_id TEXT,FOREIGN KEY(first_id) REFERENCES targets(id),FOREIGN KEY(second_id) REFERENCES targets(id))")
	var keys []sink.Key
	if err := metadata.New().Info(context.Background(), h.DB, info.KindForeignKeys, &keys, option.NewArgs("", "", "children")); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0].Name == keys[1].Name {
		t.Fatalf("independent references merged: %+v", keys)
	}
}

func TestNullableReferenceMixedBatchKeepsPerRowCoverage(t *testing.T) {
	text := func(s string) *string { return &s }
	h := sqlite.New(t, "CREATE TABLE targets(scope_id TEXT,parent_id TEXT,PRIMARY KEY(scope_id,parent_id))", "INSERT INTO targets VALUES('old','parent')", "CREATE TABLE owners(id TEXT PRIMARY KEY)")
	rows := []*nullableReferenceRow{{Scope: text("missing")}, {Scope: text("missing"), Parent: text("parent")}}
	result, err := validator.New().Validate(context.Background(), h.DB, rows, validator.WithShallow(true), validator.WithLocation("Rows"))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Violations) != 1 || result.Violations[0].Location != "Rows[1].Scope" {
		t.Fatalf("per-row reference coverage: %v", result)
	}
}

func TestSparseReferenceWithoutPreviousDoesNotAssumeNull(t *testing.T) {
	scope := "missing"
	h := sqlite.New(t, "CREATE TABLE targets(scope_id TEXT,parent_id TEXT,PRIMARY KEY(scope_id,parent_id))", "CREATE TABLE owners(id TEXT PRIMARY KEY)")
	result, err := validator.New().Validate(context.Background(), h.DB, &nullableReferenceRow{Scope: &scope}, validator.WithShallow(true), validator.WithFieldFilter(func(name string) bool { return name == "Scope" }))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Violations) != 1 || result.Violations[0].Field != "Scope" {
		t.Fatalf("omitted value assumed null without previous: %v", result)
	}
}
