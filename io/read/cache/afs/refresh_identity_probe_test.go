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

func TestRefreshThenIdentityReplaySQLite(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "group"}[grouped], func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t, `CREATE TABLE items(id INTEGER, name TEXT)`, `INSERT INTO items VALUES(1,'before')`)
			service, err := afscache.NewCache(t.TempDir(), time.Minute, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			const identity = "SELECT id,name FROM items"
			query := "SELECT id,name FROM items LIMIT 1"
			column := ""
			var args []any
			if grouped {
				column = "id"
				query = "SELECT id,name FROM items WHERE id = ? LIMIT 1"
				args = []any{1}
			}
			if _, err := service.IndexBy(ctx, h.DB, column, identity, nil); err != nil {
				t.Fatal(err)
			}
			h.Exec(t, `UPDATE items SET name='after'`)
			type row struct {
				ID   int
				Name string
			}
			for _, tc := range []struct {
				name    string
				refresh bool
				want    string
			}{
				{"stale control", false, "before"}, {"refresh", true, "after"}, {"following replay", false, "after"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					matcher := &cache.ParmetrizedQuery{SQL: query, Args: args, IdentitySQL: identity, Limit: 1}
					if grouped {
						matcher.By = "id"
						matcher.In = []any{1}
						matcher.IdentityArgs = []any{}
					}
					reader, err := read.New(ctx, h.DB, query, func() any { return &row{} }, read.WithCache(service), read.WithCacheRefresh(cache.Refresh(tc.refresh)), read.WithInMatcher(matcher))
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if stmt := reader.Stmt(); stmt != nil {
							_ = stmt.Close()
						}
					}()
					var got []row
					if err := reader.QueryAll(ctx, func(value any) error { got = append(got, *value.(*row)); return nil }, args...); err != nil {
						t.Fatal(err)
					}
					if len(got) != 1 || got[0].Name != tc.want {
						t.Fatalf("got=%+v want=%s", got, tc.want)
					}
				})
			}
		})
	}
}

func TestRefreshPreservesActiveWarmupLease(t *testing.T) {
	ctx := context.Background()
	service, err := afscache.NewCache(t.TempDir(), time.Minute, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	const identity = "SELECT id,name FROM items"
	entry, err := service.Get(ctx, identity, nil)
	if err != nil || entry == nil {
		t.Fatalf("writer=%v err=%v", entry, err)
	}
	defer service.Rollback(ctx, entry)
	matcher := &cache.ParmetrizedQuery{IdentitySQL: identity}
	if _, err := service.Get(ctx, identity+" LIMIT 1", nil, matcher, cache.Refresh(true)); err == nil {
		t.Fatal("refresh accepted active warmup writer")
	}
	another, err := service.Get(ctx, identity, nil)
	if err != nil || another != nil {
		t.Fatalf("original lease lost: entry=%v err=%v", another, err)
	}
}

func TestRefreshRejectsActiveExactWriter(t *testing.T) {
	ctx := context.Background()
	service, err := afscache.NewCache(t.TempDir(), time.Minute, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	const query = "SELECT id,name FROM items LIMIT 1"
	entry, err := service.Get(ctx, query, nil)
	if err != nil || entry == nil {
		t.Fatalf("writer=%v err=%v", entry, err)
	}
	defer service.Rollback(ctx, entry)
	if _, err := service.Get(ctx, query, nil, cache.Refresh(true)); err == nil {
		t.Fatal("refresh silently accepted active exact writer")
	}
	another, err := service.Get(ctx, query, nil)
	if err != nil || another != nil {
		t.Fatalf("original lease lost: entry=%v err=%v", another, err)
	}
}
