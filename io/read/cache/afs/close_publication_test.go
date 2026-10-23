package afs_test

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	afscache "github.com/viant/sqlx/io/read/cache/afs"
	"testing"
	"time"
)

func TestPublishedCacheCloseIsIdempotent(t *testing.T) {
	for _, memory := range []bool{false, true} {
		t.Run(map[bool]string{false: "file", true: "memory"}[memory], func(t *testing.T) {
			location := t.TempDir()
			if memory {
				location = "mem://localhost/sqlx-close/" + t.Name()
			}
			ctx := context.Background()
			c, err := afscache.NewCache(location, time.Minute, "close", nil)
			require.NoError(t, err)
			h := sqlite.New(t, "CREATE TABLE items(id INTEGER)", "INSERT INTO items VALUES(1)")
			entry, err := c.Get(ctx, "SELECT id FROM items", nil)
			require.NoError(t, err)
			require.NotNil(t, entry)
			rows, err := h.DB.QueryContext(ctx, "SELECT id FROM items")
			require.NoError(t, err)
			require.NoError(t, entry.AssignRows(rows))
			require.NoError(t, rows.Close())
			require.NoError(t, c.AddValues(ctx, entry, []interface{}{int64(1)}))
			require.NoError(t, c.Close(ctx, entry))
			require.NoError(t, c.Close(ctx, entry), "closing a published entry again must not move its already removed temporary file")
			require.NoError(t, c.Rollback(ctx, entry), "late cleanup cannot remove a committed entry")
			hit, err := c.Get(ctx, "SELECT id FROM items", nil)
			require.NoError(t, err)
			require.NotNil(t, hit)
			require.True(t, hit.Has())
			require.True(t, hit.Next())
			require.JSONEq(t, "[1]", string(hit.Data))
			require.False(t, hit.Next())
			require.NoError(t, c.Close(ctx, hit))
		})
	}
}
