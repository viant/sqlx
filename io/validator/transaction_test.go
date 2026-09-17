package validator_test

import (
	"context"
	"testing"
	"time"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
)

type transactionRef struct {
	Parent int `sqlx:"parent,refColumn=id,refTable=parents"`
}

func TestValidationUsesCallerTransactionSQLite(t *testing.T) {
	for _, mode := range []string{"reference", "unique", "unique previous"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE records(tenant_id INTEGER,id INTEGER,name TEXT UNIQUE,PRIMARY KEY(tenant_id,id))")
			h.DB.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.ExecContext(ctx, "INSERT INTO parents VALUES(7)"); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(1,0,'pending')"); err != nil {
				t.Fatal(err)
			}
			var row any = &compositeUniqueRow{Tenant: 2, ID: 2, Name: "pending"}
			opts := []validator.Option{validator.WithTransaction(tx), validator.WithShallow(true)}
			failed := true
			switch mode {
			case "reference":
				row = &transactionRef{Parent: 7}
				failed = false
			case "unique previous":
				opts = append(opts, validator.WithPrevious(nil))
			}
			result, err := validator.New().Validate(ctx, h.DB, row, opts...)
			if err != nil {
				t.Fatal(err)
			}
			if result.Failed != failed {
				t.Fatalf("got %v, want failed=%v", result, failed)
			}
			// Validator has not completed the caller transaction.
			if _, err = tx.ExecContext(ctx, "INSERT INTO parents VALUES(8)"); err != nil {
				t.Fatalf("transaction no longer owned by caller: %v", err)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			type count struct {
				N int `sqlx:"n"`
			}
			sqlite.AssertRows(t, h, "SELECT COUNT(*) n FROM parents", []count{{}})
		})
	}
}
