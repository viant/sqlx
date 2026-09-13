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
)

// Harness owns isolated native SQLX integration-test databases.
type Harness struct{ DB *sql.DB }

func New(t testing.TB, statements ...string) *Harness {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	result := &Harness{DB: db}
	result.Exec(t, statements...)
	return result
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
