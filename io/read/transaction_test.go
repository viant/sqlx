package read_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

type neverCache struct{ cache.Cache }

func TestReaderUsesCallerTransactionAndRollback(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER)")
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO records(id) VALUES(7)"); err != nil {
		t.Fatal(err)
	}
	reader, err := read.New(ctx, h.DB, "SELECT id FROM records", func() any { return &struct{ ID int }{} }, read.WithTx(tx))
	if err != nil {
		t.Fatal(err)
	}
	var ids []int
	if err := reader.QueryAll(ctx, func(row any) error { ids = append(ids, row.(*struct{ ID int }).ID); return nil }); err != nil {
		t.Fatal(err)
	}
	if stmt := reader.Stmt(); stmt != nil {
		_ = stmt.Close()
	}
	if len(ids) != 1 || ids[0] != 7 {
		t.Fatalf("transaction rows=%v", ids)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var persisted int
	if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&persisted); err != nil || persisted != 0 {
		t.Fatalf("rolled-back row count=%d err=%v", persisted, err)
	}
}

func TestReaderRejectsTransactionCacheAndReconnect(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER)")
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	cached, err := read.New(ctx, h.DB, "SELECT id FROM records", func() any { return &struct{ ID int }{} },
		read.WithTx(tx), read.WithCache(neverCache{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := cached.QueryAll(ctx, func(any) error { return nil }); !errors.Is(err, read.ErrTransactionCache) {
		t.Fatalf("transaction cache error=%v", err)
	}
	cacheOnly, err := read.New(ctx, h.DB, "SELECT id FROM records", func() any { return &struct{ ID int }{} },
		read.WithTx(tx), read.WithCacheOnly(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := cacheOnly.QueryAll(ctx, func(any) error { return nil }); !errors.Is(err, read.ErrTransactionCache) {
		t.Fatalf("transaction cache-only error=%v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	reconnects := 0
	closed, err := read.New(ctx, h.DB, "SELECT id FROM records", func() any { return &struct{ ID int }{} },
		read.WithTx(tx), read.WithRetry(read.RetryPolicy{Attempts: 2,
			Recoverable: func(error) bool { return true },
			Reconnect:   func(context.Context) (*sql.DB, error) { reconnects++; return h.DB, nil }}))
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.QueryAll(ctx, func(any) error { return nil }); !errors.Is(err, read.ErrRetryUnsafe) || reconnects != 0 {
		t.Fatalf("closed transaction error=%v reconnects=%d", err, reconnects)
	}
}
