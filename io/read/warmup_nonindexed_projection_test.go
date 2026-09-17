package read_test

import (
	"context"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	cacheaero "github.com/viant/sqlx/io/read/cache/aerospike"
	cacheafs "github.com/viant/sqlx/io/read/cache/afs"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNonindexedWarmupProjectionWindowLive(t *testing.T) {
	for _, backend := range []string{"afs", "aerospike"} {
		t.Run(backend, func(t *testing.T) {
			if backend == "aerospike" && os.Getenv("SQLX_TEST_AEROSPIKE") != "1" {
				t.Skip("set SQLX_TEST_AEROSPIKE=1 for dedicated localhost:3000/test")
			}
			ctx := context.Background()
			db := sqlite.New(t, "CREATE TABLE records(id INTEGER,name TEXT,tenant INTEGER)", "INSERT INTO records VALUES(1,'one',7),(2,'two',7),(3,'three',7),(4,'four',7),(99,'other',8)")
			var native cache.Cache
			var err error
			if backend == "afs" {
				native, err = cacheafs.NewCache(t.TempDir(), time.Minute, t.Name(), nil)
			} else {
				client, e := as.NewClient("127.0.0.1", 3000)
				require.NoError(t, e)
				defer client.Close()
				native, err = cacheaero.New("test", fmt.Sprintf("nonidx_%d", time.Now().UnixNano()), client, 60)
			}
			require.NoError(t, err)
			const fullSQL = "SELECT id,name FROM records WHERE tenant = ? ORDER BY id"
			fields := []cache.ProjectionField{{Name: "id", ColumnName: "id"}, {Name: "name", ColumnName: "name"}}
			_, err = native.IndexBy(ctx, db.DB, "", fullSQL, []any{7}, &cache.ParmetrizedQuery{SQL: fullSQL, Args: []any{7}, StoredFields: fields})
			require.NoError(t, err)
			db.Exec(t, "DROP TABLE records")
			type row struct {
				ID int `sqlx:"id"`
			}
			for _, tc := range []struct {
				offset, limit int
				want          []row
			}{{1, 1, []row{{2}}}, {3, 1, []row{{4}}}, {10, 1, []row{}}, {1, 0, []row{{2}, {3}, {4}}}} {
				matcher := &cache.ParmetrizedQuery{IdentitySQL: fullSQL, IdentityArgs: []any{7}, Offset: tc.offset, Limit: tc.limit, RequestedFields: fields[:1]}
				stats := &cache.Stats{}
				reader, err := read.New(ctx, db.DB, "SELECT id FROM records WHERE tenant = ? ORDER BY id", func() any { return &row{} }, read.WithCache(native), read.WithInMatcher(matcher), read.WithCacheStats(stats))
				require.NoError(t, err)
				actual := []row{}
				require.NoError(t, reader.QueryAll(ctx, func(value any) error { actual = append(actual, *value.(*row)); return nil }, 7))
				require.Equal(t, tc.want, actual, "offset=%d limit=%d", tc.offset, tc.limit)
				require.True(t, stats.FoundWarmup)
				require.Equal(t, cache.Type(cache.TypeReadMulti), stats.Type)
			}
			rawMatcher := &cache.ParmetrizedQuery{IdentitySQL: fullSQL, IdentityArgs: []any{7}, Offset: 1, Limit: 1, RequestedFields: fields[:1]}
			rawEntry, err := native.Get(ctx, "SELECT id FROM records WHERE tenant = ? ORDER BY id", []any{7}, rawMatcher, &cache.Stats{})
			require.NoError(t, err)
			require.NotNil(t, rawEntry)
			require.NotNil(t, rawEntry.ReadCloser)
			raw, err := io.ReadAll(rawEntry.ReadCloser)
			require.NoError(t, err)
			require.JSONEq(t, `[2,"two"]`, strings.TrimSpace(string(raw)))
			require.NoError(t, native.Close(ctx, rawEntry))
			matcher := &cache.ParmetrizedQuery{IdentitySQL: fullSQL, IdentityArgs: []any{8}, RequestedFields: fields[:1]}
			stats := &cache.Stats{}
			reader, err := read.New(ctx, db.DB, "SELECT id FROM records WHERE tenant = ? ORDER BY id", func() any { return &row{} }, read.WithCache(native), read.WithInMatcher(matcher), read.WithCacheStats(stats))
			require.NoError(t, err)
			emitted := 0
			require.Error(t, reader.QueryAll(ctx, func(any) error { emitted++; return nil }, 8))
			require.Zero(t, emitted)
			require.False(t, stats.FoundWarmup, "another tenant must not reuse warmed rows")
		})
	}
}
