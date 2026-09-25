package delete_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx/io/delete"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/option"
)

type matchedRow struct {
	ID      int    `sqlx:"name=row_id,primaryKey=true"`
	Version int    `sqlx:"name=version"`
	Name    string `sqlx:"name=name"`
	Hidden  string `sqlx:"name=hidden,transient=true"`
}

type compositeMatchedRow struct {
	Tenant  int `sqlx:"name=tenant_id,primaryKey=true"`
	ID      int `sqlx:"name=row_id,primaryKey=true"`
	Version int `sqlx:"name=version"`
}

func TestExecIfMatch(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"CREATE TABLE matched_rows (row_id INTEGER PRIMARY KEY, version INTEGER, name TEXT)",
		"INSERT INTO matched_rows VALUES (1, 2, 'first'), (2, 2, 'second')",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	service, err := delete.New(ctx, db, "matched_rows")
	if err != nil {
		t.Fatal(err)
	}
	row := &matchedRow{ID: 1}
	for _, tc := range []struct {
		name   string
		input  interface{}
		match  option.IfMatch
		want   int64
		errMsg string
	}{
		{"stale", row, option.IfMatch{Column: "version", Value: 1}, 0, ""},
		{"unknown", row, option.IfMatch{Column: "missing", Value: 2}, 0, "not a mapped"},
		{"injection", row, option.IfMatch{Column: "version OR 1=1", Value: 2}, 0, "not a mapped"},
		{"transient", row, option.IfMatch{Column: "hidden", Value: 2}, 0, "not a mapped"},
		{"key", row, option.IfMatch{Column: "row_id", Value: 1}, 0, "not a mapped"},
		{"nil", row, option.IfMatch{Column: "version", Value: nil}, 0, "value is nil"},
		{"typed nil", row, option.IfMatch{Column: "version", Value: (*int)(nil)}, 0, "value is nil"},
		{"slice", []matchedRow{{ID: 1}}, option.IfMatch{Column: "version", Value: 2}, 0, "one record"},
		{"interface list", []interface{}{row}, option.IfMatch{Column: "version", Value: 2}, 0, "one record"},
		{"match", row, option.IfMatch{Column: " VERSION ", Value: 2}, 1, ""},
		{"already deleted", row, option.IfMatch{Column: "version", Value: 2}, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			affected, err := service.Exec(ctx, tc.input, tc.match)
			if tc.errMsg != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errMsg) {
					t.Fatalf("expected error containing %q, got %v", tc.errMsg, err)
				}
			} else if tc.want == 0 && (tc.name == "stale" || tc.name == "already deleted") {
				if !errors.Is(err, option.ErrNoMatch) {
					t.Fatalf("expected ErrNoMatch, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if affected != tc.want {
				t.Fatalf("affected = %d, want %d", affected, tc.want)
			}
		})
	}
	var remaining int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM matched_rows").Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("remaining = %d, want 1", remaining)
	}
}

func TestExecIfMatchCallerTransaction(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"CREATE TABLE matched_rows (row_id INTEGER PRIMARY KEY, version INTEGER)",
		"INSERT INTO matched_rows VALUES (1, 2)",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	service, err := delete.New(ctx, db, "matched_rows")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	affected, err := service.Exec(ctx, &matchedRow{ID: 1}, option.IfMatch{Column: "version", Value: 2}, tx)
	if err != nil || affected != 1 {
		t.Fatalf("affected = %d, err = %v", affected, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM matched_rows").Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("rollback did not restore row: %d", remaining)
	}
}

func TestExecIfMatchCompositeKey(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"CREATE TABLE matched_rows (tenant_id INTEGER, row_id INTEGER, version INTEGER, PRIMARY KEY (tenant_id, row_id))",
		"INSERT INTO matched_rows VALUES (1, 1, 2), (2, 1, 2)",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	service, err := delete.New(ctx, db, "matched_rows")
	if err != nil {
		t.Fatal(err)
	}
	affected, err := service.Exec(ctx, &compositeMatchedRow{Tenant: 1, ID: 1}, option.IfMatch{Column: "version", Value: 2})
	if err != nil || affected != 1 {
		t.Fatalf("affected = %d, err = %v", affected, err)
	}
	var tenant int
	if err := db.QueryRowContext(ctx, "SELECT tenant_id FROM matched_rows").Scan(&tenant); err != nil {
		t.Fatal(err)
	}
	if tenant != 2 {
		t.Fatalf("remaining tenant = %d, want 2", tenant)
	}
}
