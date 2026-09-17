package afs_test

import (
	"context"
	"testing"
	"time"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/afs"
)

func TestAFSNativeReadStatsSQLite(t *testing.T) {
	for _, warm := range []bool{false, true} {
		name := "lazy"
		if warm {
			name = "warmup"
		}
		t.Run(name, func(t *testing.T) {
			db := sqlite.New(t, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2)")
			ctx := context.Background()
			c, err := afs.NewCache(t.TempDir(), time.Minute, "stats", nil)
			if err != nil {
				t.Fatal(err)
			}
			query := "SELECT id FROM records"
			if warm {
				if _, err := c.IndexBy(ctx, db.DB, "", query, nil); err != nil {
					t.Fatal(err)
				}
			}
			for pass := 0; pass < 2; pass++ {
				stats := &cache.Stats{ErrorType: "stale"}
				options := []read.Option{read.WithCache(c), read.WithCacheStats(stats)}
				if warm {
					options = append(options, read.WithInMatcher(&cache.ParmetrizedQuery{SQL: query, IdentitySQL: query}))
				}
				type row struct {
					ID int `sqlx:"id"`
				}
				reader, err := read.New(ctx, db.DB, query, func() any { return &row{} }, options...)
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				if err = reader.QueryAll(ctx, func(any) error { count++; return nil }); err != nil {
					t.Fatal(err)
				}
				if reader.Stmt() != nil {
					reader.Stmt().Close()
				}
				if count != 2 || stats.ErrorType != "" {
					t.Fatalf("count=%d stats=%+v", count, stats)
				}
				if pass == 0 && !warm {
					if stats.Type != cache.TypeWrite {
						t.Fatal("missing write decision")
					}
				} else if warm && !stats.FoundWarmup || !warm && !stats.FoundLazy {
					t.Fatalf("source stats=%+v", stats)
				}
				if pass == 0 {
					db.Exec(t, "DROP TABLE records")
				}
			}
		})
	}
}
