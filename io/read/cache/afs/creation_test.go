package afs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/afs"
)

func TestNativeCreationMetrics(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t, "CREATE TABLE items(id INTEGER)", "INSERT INTO items VALUES(1)")
	c, err := afs.NewCache(t.TempDir(), time.Minute, "metrics", nil)
	require.NoError(t, err)
	var lazy, warm atomic.Int64
	c.SetCreationObserver(func(kind string, count int) {
		if kind == cache.CreationWarmup {
			warm.Add(int64(count))
		} else {
			lazy.Add(int64(count))
		}
	})
	run := func(refresh, fail bool) *cache.Stats {
		stats := &cache.Stats{}
		type row struct{ ID int }
		r, err := read.New(ctx, h.DB, "SELECT id FROM items", func() any { return &row{} }, read.WithCache(c), read.WithCacheStats(stats), read.WithCacheRefresh(cache.Refresh(refresh)))
		require.NoError(t, err)
		err = r.QueryAll(ctx, func(any) error {
			if fail {
				return errors.New("consumer")
			}
			return nil
		})
		if stmt := r.Stmt(); stmt != nil {
			require.NoError(t, stmt.Close())
		}
		if fail {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		return stats
	}
	first := run(false, false)
	require.NotNil(t, first.CreatedTime)
	require.NotNil(t, first.ExpiryTime)
	require.Equal(t, time.Minute, first.ExpiryTime.Sub(*first.CreatedTime))
	second := run(false, false)
	require.Equal(t, first.CreatedTime, second.CreatedTime, "hits preserve creation time")
	require.Equal(t, first.ExpiryTime, second.ExpiryTime, "hits preserve expiry time")
	require.Equal(t, int64(1), lazy.Load())
	run(true, true)
	require.Equal(t, int64(1), lazy.Load())
	for i := 0; i < 2; i++ {
		_, err = c.IndexBy(ctx, h.DB, "", "SELECT id FROM items ORDER BY id", nil)
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), warm.Load(), "reusing a warmup must not report creation")
	for i := 0; i < 2; i++ {
		_, err = c.IndexBy(ctx, h.DB, "id", "SELECT id FROM items", nil)
		require.NoError(t, err)
	}
	require.Equal(t, int64(3), warm.Load(), "one group and one marker")
	_, err = c.IndexBy(ctx, h.DB, "id", "SELECT absent FROM items", nil)
	require.Error(t, err)
	require.Equal(t, int64(3), warm.Load())
	_, err = c.IndexBy(ctx, h.DB, "id", "SELECT id FROM items WHERE 1=0", nil)
	require.NoError(t, err)
	require.Equal(t, int64(4), warm.Load(), "empty indexed result publishes a marker")
	_, err = c.IndexBy(ctx, h.DB, "", "SELECT id FROM items WHERE 2=0", nil)
	require.NoError(t, err)
	require.Equal(t, int64(5), warm.Load(), "empty query result publishes an entry")
	require.Equal(t, int64(1), lazy.Load(), "warmups must not inflate lazy counts")
}
