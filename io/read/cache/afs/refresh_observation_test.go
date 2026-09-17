package afs_test

import (
	"context"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	afs "github.com/viant/sqlx/io/read/cache/afs"
	"testing"
	"time"
)

func TestRefreshAndStatsIdentitySQLite(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		name := "query"
		if grouped {
			name = "group"
		}
		t.Run(name, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE items(id INTEGER,name TEXT)", "INSERT INTO items VALUES(1,'before')")
			ctx := context.Background()
			c, err := afs.NewCache(t.TempDir(), time.Minute, "identity-stats", nil)
			if err != nil {
				t.Fatal(err)
			}
			identity := "SELECT id,name FROM items"
			query := identity + " LIMIT 1"
			var args []any
			column := ""
			if grouped {
				column = "id"
				query = identity + " WHERE id=? LIMIT 1"
				args = []any{1}
			}
			if _, err = c.IndexBy(ctx, h.DB, column, identity, nil); err != nil {
				t.Fatal(err)
			}
			h.Exec(t, "UPDATE items SET name='after'")
			var warmupKey string
			for _, step := range []struct {
				name, want string
				refresh    bool
				kind       cache.Type
			}{{"warmup control", "before", false, cache.TypeReadMulti}, {"refresh", "after", true, cache.TypeWrite}, {"retired identity replay", "after", false, cache.TypeReadSingle}} {
				t.Run(step.name, func(t *testing.T) {
					stats := &cache.Stats{FoundLazy: true, FoundWarmup: true, ErrorType: "old"}
					matcher := &cache.ParmetrizedQuery{SQL: query, Args: args, IdentitySQL: identity, Limit: 1}
					if grouped {
						matcher.By = "id"
						matcher.In = []any{1}
						matcher.IdentityArgs = []any{}
					}
					type row struct {
						ID   int
						Name string
					}
					reader, err := read.New(ctx, h.DB, query, func() any { return &row{} }, read.WithCache(c), read.WithCacheStats(stats), read.WithCacheRefresh(cache.Refresh(step.refresh)), read.WithInMatcher(matcher))
					if err != nil {
						t.Fatal(err)
					}
					var rows []row
					err = reader.QueryAll(ctx, func(v any) error { rows = append(rows, *v.(*row)); return nil }, args...)
					if reader.Stmt() != nil {
						reader.Stmt().Close()
					}
					if err != nil || len(rows) != 1 || rows[0].Name != step.want {
						t.Fatalf("rows=%v error=%v", rows, err)
					}
					if stats.Type != step.kind || stats.ErrorType != "" || stats.Key == "" || stats.Namespace == "" || stats.Dataset != "identity-stats" {
						t.Fatalf("stats=%+v", stats)
					}
					if step.kind == cache.TypeReadMulti {
						warmupKey = stats.Key
						if !stats.FoundWarmup || stats.FoundLazy || stats.WarmupKey != warmupKey || grouped && stats.MarkerKey != warmupKey {
							t.Fatalf("warmup stats=%+v", stats)
						}
					}
					if step.refresh {
						if stats.FoundWarmup || stats.FoundLazy || stats.WarmupKey != warmupKey || grouped && stats.MarkerKey != warmupKey {
							t.Fatalf("refresh must be a miss with original retirement identity: %+v", stats)
						}
						h.Exec(t, "DROP TABLE items")
					}
					if step.kind == cache.TypeReadSingle && (!stats.FoundLazy || stats.FoundWarmup) {
						t.Fatalf("replay stats=%+v", stats)
					}
				})
			}
		})
	}
}
func TestRefreshConflictStatsPreserveLease(t *testing.T) {
	for _, warmup := range []bool{false, true} {
		name := "exact"
		if warmup {
			name = "query identity"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			c, _ := afs.NewCache(t.TempDir(), time.Minute, "conflicts", nil)
			query := "SELECT id FROM items"
			initial := &cache.Stats{}
			entry, err := c.Get(ctx, query, nil, initial)
			if err != nil || entry == nil {
				t.Fatal(err)
			}
			defer c.Rollback(ctx, entry)
			args := []any{cache.Refresh(true)}
			target := query
			if warmup {
				target += " LIMIT 1"
				args = append(args, &cache.ParmetrizedQuery{IdentitySQL: query})
			}
			stats := &cache.Stats{FoundLazy: true, FoundWarmup: true}
			args = append(args, stats)
			if _, err = c.Get(ctx, target, nil, args...); err == nil {
				t.Fatal("refresh conflict accepted")
			}
			if stats.ErrorType != "afs" || stats.FoundLazy || stats.FoundWarmup || stats.Key != initial.Key {
				t.Fatalf("conflict stats=%+v initial=%+v", stats, initial)
			}
			if warmup && stats.WarmupKey != initial.Key {
				t.Fatal("warmup conflict identity lost")
			}
			if second, err := c.Get(ctx, query, nil); err != nil || second != nil {
				t.Fatal("active writer lease stolen")
			}
		})
	}
}
