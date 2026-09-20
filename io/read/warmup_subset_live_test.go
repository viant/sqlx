package read_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	cacheaero "github.com/viant/sqlx/io/read/cache/aerospike"
	cacheafs "github.com/viant/sqlx/io/read/cache/afs"
)

func TestFullWarmupTwoColumnProjectionLive(t *testing.T) {
	for _, backend := range []string{"afs", "aerospike"} {
		t.Run(backend, func(t *testing.T) {
			if backend == "aerospike" && os.Getenv("SQLX_TEST_AEROSPIKE") != "1" {
				t.Skip("set SQLX_TEST_AEROSPIKE=1 for the dedicated localhost:3000/test fixture")
			}
			ctx := context.Background()
			db := sqlite.New(t, "CREATE TABLE records(id INTEGER,name TEXT,secret TEXT,amount INTEGER)", "INSERT INTO records VALUES(1,'one','hidden',42),(2,'two','private',99)")
			var native cache.Cache
			var err error
			if backend == "afs" {
				native, err = cacheafs.NewCache(t.TempDir(), time.Minute, t.Name(), nil)
			} else {
				client, e := as.NewClient("127.0.0.1", 3000)
				require.NoError(t, e)
				defer client.Close()
				native, err = cacheaero.New("test", fmt.Sprintf("subset_%d", time.Now().UnixNano()), client, 60)
			}
			require.NoError(t, err)
			fullSQL := "SELECT id,name,secret,amount FROM records"
			fields := []cache.ProjectionField{{Name: "id", ColumnName: "id"}, {Name: "name", ColumnName: "name"}, {Name: "secret", ColumnName: "secret"}, {Name: "amount", ColumnName: "amount"}}
			_, err = native.IndexBy(ctx, db.DB, "id", fullSQL, nil, &cache.ParmetrizedQuery{SQL: fullSQL, Args: []any{}, StoredFields: fields})
			require.NoError(t, err)
			db.Exec(t, "DROP TABLE records")
			type projected struct {
				Name string `sqlx:"name"`
				ID   int    `sqlx:"id"`
			}
			matcher := &cache.ParmetrizedQuery{SQL: fullSQL, Args: []any{}, By: "id", In: []any{2}, RequestedFields: []cache.ProjectionField{fields[1], fields[0]}}
			stats := &cache.Stats{}
			var columns []string
			reader, err := read.New(ctx, db.DB, "SELECT name,id FROM records WHERE id = ?", func() any { return &projected{} }, read.WithCache(native), read.WithInMatcher(matcher), read.WithCacheStats(stats), read.WithColumnsObserver(func(found []sqlio.Column) error {
				for _, c := range found {
					columns = append(columns, c.Name())
				}
				return nil
			}))
			require.NoError(t, err)
			defer func() {
				if stmt := reader.Stmt(); stmt != nil {
					_ = stmt.Close()
				}
			}()
			var got []projected
			require.NoError(t, reader.QueryAll(ctx, func(row any) error { got = append(got, *row.(*projected)); return nil }, 2))
			require.Equal(t, []projected{{Name: "two", ID: 2}}, got)
			require.Equal(t, []string{"name", "id"}, columns)
			require.True(t, stats.FoundWarmup, "must hit warmup after source table was removed")
		})
	}
}

func TestFullCubeWarmupMeasureSubsetLive(t *testing.T) {
	for _, backend := range []string{"afs", "aerospike"} {
		t.Run(backend, func(t *testing.T) {
			if backend == "aerospike" && os.Getenv("SQLX_TEST_AEROSPIKE") != "1" {
				t.Skip("set SQLX_TEST_AEROSPIKE=1 for the dedicated localhost:3000/test fixture")
			}
			ctx := context.Background()
			db := sqlite.New(t, "CREATE TABLE sales(region TEXT,amount INTEGER)", "INSERT INTO sales VALUES('west',10),('west',20),('east',7)")
			var native cache.Cache
			var err error
			if backend == "afs" {
				native, err = cacheafs.NewCache(t.TempDir(), time.Minute, t.Name(), nil)
			} else {
				client, e := as.NewClient("127.0.0.1", 3000)
				require.NoError(t, e)
				defer client.Close()
				native, err = cacheaero.New("test", fmt.Sprintf("cube_%d", time.Now().UnixNano()), client, 60)
			}
			require.NoError(t, err)
			fullSQL := "SELECT region,SUM(amount) AS total,COUNT(*) AS count FROM sales GROUP BY region"
			dimension := cache.ProjectionField{Name: "region", ColumnName: "region", DimensionKey: "sales.region"}
			total := cache.ProjectionField{Name: "total", ColumnName: "total", MeasureKey: "sum(sales.amount)"}
			count := cache.ProjectionField{Name: "count", ColumnName: "count", MeasureKey: "count(*)"}
			_, err = native.IndexBy(ctx, db.DB, "region", fullSQL, nil, &cache.ParmetrizedQuery{SQL: fullSQL, Args: []any{}, StoredFields: []cache.ProjectionField{dimension, total, count}})
			require.NoError(t, err)
			db.Exec(t, "DROP TABLE sales")
			type projected struct {
				Region string `sqlx:"region"`
				Total  int    `sqlx:"total"`
			}
			matcher := &cache.ParmetrizedQuery{SQL: fullSQL, Args: []any{}, By: "region", In: []any{"west"}, RequestedFields: []cache.ProjectionField{dimension, total}}
			stats := &cache.Stats{}
			var columns []string
			reader, err := read.New(ctx, db.DB, "SELECT region,SUM(amount) AS total FROM sales WHERE region = ? GROUP BY region", func() any { return &projected{} }, read.WithCache(native), read.WithInMatcher(matcher), read.WithCacheStats(stats), read.WithColumnsObserver(func(found []sqlio.Column) error {
				for _, c := range found {
					columns = append(columns, c.Name())
				}
				return nil
			}))
			require.NoError(t, err)
			defer func() {
				if stmt := reader.Stmt(); stmt != nil {
					_ = stmt.Close()
				}
			}()
			var got []projected
			require.NoError(t, reader.QueryAll(ctx, func(row any) error { got = append(got, *row.(*projected)); return nil }, "west"))
			require.Equal(t, []projected{{Region: "west", Total: 30}}, got)
			require.Equal(t, []string{"region", "total"}, columns)
			require.True(t, stats.FoundWarmup)
			// Dropping a grouping dimension cannot reinterpret cached aggregates.
			bad := &cache.ParmetrizedQuery{SQL: fullSQL, Args: []any{}, By: "region", In: []any{"west"}, RequestedFields: []cache.ProjectionField{total}}
			entry, e := native.Get(ctx, "SELECT SUM(amount) AS total FROM sales", nil, bad)
			require.NoError(t, e)
			if entry != nil {
				require.False(t, entry.Has(), "missing cube dimension must reject reuse")
				require.NoError(t, native.Rollback(ctx, entry))
			}
		})
	}
}

func TestUnknownWarmupTypeStillValidatesPayloadLive(t *testing.T) {
	for _, backend := range []string{"afs", "aerospike"} {
		t.Run(backend, func(t *testing.T) {
			if backend == "aerospike" && os.Getenv("SQLX_TEST_AEROSPIKE") != "1" {
				t.Skip("set SQLX_TEST_AEROSPIKE=1 for the dedicated localhost:3000/test fixture")
			}
			ctx := context.Background()
			db := sqlite.New(t, "CREATE TABLE sales(region TEXT,name TEXT)", "INSERT INTO sales VALUES('west','not-a-number')")
			var native cache.Cache
			var err error
			if backend == "afs" {
				native, err = cacheafs.NewCache(t.TempDir(), time.Minute, t.Name(), nil)
			} else {
				client, e := as.NewClient("127.0.0.1", 3000)
				require.NoError(t, e)
				defer client.Close()
				native, err = cacheaero.New("test", fmt.Sprintf("badtype_%d", time.Now().UnixNano()), client, 60)
			}
			require.NoError(t, err)
			fullSQL := "SELECT region,MAX(name) AS total FROM sales GROUP BY region"
			fields := []cache.ProjectionField{{Name: "region", ColumnName: "region", DimensionKey: "sales.region"}, {Name: "total", ColumnName: "total", MeasureKey: "max(sales.name)"}}
			_, err = native.IndexBy(ctx, db.DB, "region", fullSQL, nil, &cache.ParmetrizedQuery{SQL: fullSQL, Args: []any{}, StoredFields: fields})
			require.NoError(t, err)
			db.Exec(t, "DROP TABLE sales")
			type output struct {
				Region string `sqlx:"region"`
				Total  int    `sqlx:"total"`
			}
			stats := &cache.Stats{}
			matcher := &cache.ParmetrizedQuery{SQL: fullSQL, Args: []any{}, By: "region", In: []any{"west"}, RequestedFields: fields}
			reader, err := read.New(ctx, db.DB, fullSQL, func() any { return &output{} }, read.WithCache(native), read.WithInMatcher(matcher), read.WithCacheStats(stats))
			require.NoError(t, err)
			defer func() {
				if s := reader.Stmt(); s != nil {
					_ = s.Close()
				}
			}()
			calls := 0
			err = reader.QueryAll(ctx, func(any) error { calls++; return nil })
			require.Error(t, err)
			require.NotContains(t, err.Error(), "no such table")
			require.Zero(t, calls, "incompatible cached value must not be emitted")
			require.True(t, stats.FoundWarmup)
		})
	}
}

func TestWarmupConfigurationKeepsLazyFallbackLive(t *testing.T) {
	for _, backend := range []string{"afs", "aerospike"} {
		t.Run(backend, func(t *testing.T) {
			if backend == "aerospike" && os.Getenv("SQLX_TEST_AEROSPIKE") != "1" {
				t.Skip("set SQLX_TEST_AEROSPIKE=1 for the dedicated localhost:3000/test fixture")
			}
			ctx := context.Background()
			db := sqlite.New(t, "CREATE TABLE records(id INTEGER,name TEXT,extra TEXT)", "INSERT INTO records VALUES(1,'one','a'),(2,'two','b')")
			var native cache.Cache
			var err error
			if backend == "afs" {
				native, err = cacheafs.NewCache(t.TempDir(), time.Minute, t.Name(), nil)
			} else {
				client, e := as.NewClient("127.0.0.1", 3000)
				require.NoError(t, e)
				defer client.Close()
				native, err = cacheaero.New("test", fmt.Sprintf("fallback_%d", time.Now().UnixNano()), client, 60)
			}
			require.NoError(t, err)
			fullSQL := "SELECT id,name,extra FROM records"
			fields := []cache.ProjectionField{{Name: "id", ColumnName: "id"}, {Name: "name", ColumnName: "name"}, {Name: "extra", ColumnName: "extra"}}
			type row struct {
				ID   int    `sqlx:"id"`
				Name string `sqlx:"name"`
			}
			run := func(id int, kind, name string) {
				t.Helper()
				stats := &cache.Stats{}
				matcher := &cache.ParmetrizedQuery{SQL: fullSQL, Args: []any{}, By: "id", In: []any{id}, RequestedFields: fields[:2]}
				reader, e := read.New(ctx, db.DB, "SELECT id,name FROM records WHERE id = ?", func() any { return &row{} }, read.WithCache(native), read.WithInMatcher(matcher), read.WithCacheStats(stats))
				require.NoError(t, e)
				defer func() {
					if stmt := reader.Stmt(); stmt != nil {
						_ = stmt.Close()
					}
				}()
				var got []row
				require.NoError(t, reader.QueryAll(ctx, func(value any) error { got = append(got, *value.(*row)); return nil }, id))
				require.Equal(t, []row{{ID: id, Name: name}}, got)
				require.Equal(t, kind, string(stats.Type))
				if kind == cache.TypeReadSingle {
					require.True(t, stats.FoundLazy)
				}
				if kind == cache.TypeReadMulti {
					require.True(t, stats.FoundWarmup)
				}
			}
			run(2, cache.TypeWrite, "two") // Warmup configured but not run: ordinary fill.
			db.Exec(t, "UPDATE records SET name='changed' WHERE id=2")
			run(2, cache.TypeReadSingle, "two")
			_, err = native.IndexBy(ctx, db.DB, "id", fullSQL, nil, &cache.ParmetrizedQuery{SQL: fullSQL, Args: []any{}, StoredFields: fields})
			require.NoError(t, err)
			db.Exec(t, "DROP TABLE records")
			run(1, cache.TypeReadMulti, "one") // Different key has no lazy entry: warmup supplies it.
		})
	}
}
