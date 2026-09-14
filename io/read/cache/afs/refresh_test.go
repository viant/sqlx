package afs_test

import (
	"context"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	afscache "github.com/viant/sqlx/io/read/cache/afs"
	"testing"
	"time"
)

func TestNativeQueryRefreshSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t, `CREATE TABLE items(id INTEGER, name TEXT)`, `INSERT INTO items VALUES(1,'before')`)
	service, err := afscache.NewCache(t.TempDir(), time.Minute, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		ID   int
		Name string
	}
	for _, tc := range []struct {
		name    string
		refresh bool
		want    string
	}{{"fill", false, "before"}, {"cached", false, "before"}, {"refresh", true, "after"}, {"refreshed cache", false, "after"}} {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := read.New(ctx, h.DB, `SELECT id,name FROM items`, func() any { return &row{} }, read.WithCache(service), read.WithCacheRefresh(cache.Refresh(tc.refresh)))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if stmt := reader.Stmt(); stmt != nil {
					_ = stmt.Close()
				}
			}()
			var rows []*row
			if err := reader.QueryAll(ctx, func(value any) error { rows = append(rows, value.(*row)); return nil }); err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].Name != tc.want {
				t.Fatalf("rows=%+v want=%s", rows, tc.want)
			}
		})
		h.Exec(t, `UPDATE items SET name='after'`)
	}
}
