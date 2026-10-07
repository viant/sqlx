package read_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/afs"
)

type indexOnlyRow struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}
type cacheWithoutIndexedCapability struct{ cache.Cache }

func TestIndexedOnlyPreservesIndexedHitsWithoutExactFallbackOrWrites(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t, "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,'before'),(2,'other')")
	native, err := afs.NewCache(t.TempDir(), time.Minute, "indexed-only", nil)
	require.NoError(t, err)
	query := "SELECT id,name FROM records WHERE id = ?"
	identity := "SELECT id,name FROM records"
	matcher := func() *cache.ParmetrizedQuery {
		return &cache.ParmetrizedQuery{SQL: query, Args: []any{1}, IdentitySQL: identity, By: "id", In: []any{1}}
	}
	readName := func(options ...read.Option) (string, error) {
		options = append([]read.Option{read.WithCache(native)}, options...)
		r, err := read.New(ctx, db.DB, query, func() any { return &indexOnlyRow{} }, options...)
		if err != nil {
			return "", err
		}
		defer func() {
			if r.Stmt() != nil {
				_ = r.Stmt().Close()
			}
		}()
		name := ""
		err = r.QueryAll(ctx, func(v any) error { name = v.(*indexOnlyRow).Name; return nil }, 1)
		return name, err
	}
	before, err := readName()
	require.NoError(t, err)
	require.Equal(t, "before", before)
	_, err = db.DB.ExecContext(ctx, "UPDATE records SET name='after' WHERE id=1")
	require.NoError(t, err)
	current, err := readName(read.WithInMatcher(matcher()), read.WithCacheIndexedOnly(true))
	require.NoError(t, err)
	require.Equal(t, "after", current, "missing index must query source instead of lazy cache")
	stale, err := readName()
	require.NoError(t, err)
	require.Equal(t, "before", stale, "indexed-only source fallback must not overwrite/create exact cache")
	groups, err := native.IndexBy(ctx, db.DB, "id", identity, nil)
	require.NoError(t, err)
	require.Equal(t, 2, groups)
	_, err = db.DB.ExecContext(ctx, "DROP TABLE records")
	require.NoError(t, err)
	stats := &cache.Stats{}
	hit, err := readName(read.WithInMatcher(matcher()), read.WithCacheIndexedOnly(true), read.WithCacheStats(stats))
	require.NoError(t, err)
	require.Equal(t, "after", hit)
	require.True(t, stats.FoundWarmup)
	require.False(t, stats.FoundLazy)
	_, err = readName(read.WithCacheIndexedOnly(true))
	require.Error(t, err, "missing indexed matcher must never recover exact cached row")
	bad := matcher()
	bad.By = "missing_index"
	_, err = readName(read.WithInMatcher(bad), read.WithCacheIndexedOnly(true))
	require.Error(t, err, "missing indexed entry must never recover exact cached row")
	_, err = readName(read.WithInMatcher(bad), read.WithCacheIndexedOnly(true), read.WithCacheOnly(true))
	require.ErrorIs(t, err, cache.ErrMiss)
	_, err = readName(read.WithInMatcher(matcher()), read.WithCacheIndexedOnly(true), read.WithCacheRefresh(true))
	require.ErrorIs(t, err, cache.ErrIndexedRefresh)
	hit, err = readName(read.WithInMatcher(matcher()), read.WithCacheIndexedOnly(true))
	require.NoError(t, err)
	require.Equal(t, "after", hit, "rejected refresh must not retire the warmup")
	_, err = readName(read.WithCache(cacheWithoutIndexedCapability{native}), read.WithCacheIndexedOnly(true))
	require.True(t, errors.Is(err, cache.ErrIndexedLookupUnsupported), "custom cache must not silently ignore restricted replay: %v", err)
}
