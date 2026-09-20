package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx/io/read"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/testutil/sqlfault"
)

// Harness owns isolated native SQLX integration-test databases.
type Harness struct {
	DB  *sql.DB
	dsn string
}

func New(t testing.TB, statements ...string) *Harness {
	t.Helper()
	return NewWithDSN(t, filepath.Join(t.TempDir(), "data.db"), statements...)
}

// NewWithDSN supports tests of connection-local pragmas and separate pools.
func NewWithDSN(t testing.TB, dsn string, statements ...string) *Harness {
	t.Helper()
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	result := &Harness{DB: db, dsn: dsn}
	result.Exec(t, statements...)
	return result
}

func (h *Harness) FaultDB(t testing.TB, before func(context.Context, sqlfault.Call) error) *sql.DB {
	t.Helper()
	db := sql.OpenDB(&sqlfault.Connector{Base: h.DB.Driver(), DSN: h.dsn, Before: before})
	t.Cleanup(func() { _ = db.Close() })
	return db
}
func (h *Harness) Exec(t testing.TB, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if _, err := h.DB.ExecContext(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
	}
}

// AssertRows verifies typed results through the native SQLX reader.
func AssertRows[T any](t testing.TB, h *Harness, query string, expected []T) {
	t.Helper()
	ctx := context.Background()
	reader, err := read.New(ctx, h.DB, query, func() any { return new(T) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if statement := reader.Stmt(); statement != nil {
			_ = statement.Close()
		}
	}()
	actual := make([]T, 0)
	if err = reader.QueryAll(ctx, func(row any) error { actual = append(actual, *row.(*T)); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(actual) == 0 && len(expected) == 0 {
		return
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("query %s\ngot %#v\nwant %#v", query, actual, expected)
	}
}
