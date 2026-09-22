package aerospike

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

func TestNativeAerospikeCreationRefreshAndExpiry(t *testing.T) {
	endpoint := os.Getenv("DATLY_TEST_AEROSPIKE")
	if endpoint == "" {
		t.Skip("set DATLY_TEST_AEROSPIKE to a dedicated test instance")
	}
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)
	policy := as.NewClientPolicy()
	policy.Timeout = time.Second
	client, err := as.NewClientWithPolicy(policy, u.Hostname(), port)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	for _, grouped := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "indexed"}[grouped], func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t, "CREATE TABLE items(id INTEGER,name TEXT)", "INSERT INTO items VALUES(1,'before')")
			c, err := New(strings.TrimPrefix(u.Path, "/"), "sqlx_cache_metrics_test", client, 2)
			require.NoError(t, err)
			c.identityPrefix = uuid.NewString() + "/"
			var lazy, warm atomic.Int64
			c.SetCreationObserver(func(kind string, count int) {
				if kind == cache.CreationWarmup {
					warm.Add(int64(count))
				} else {
					lazy.Add(int64(count))
				}
			})
			column := ""
			expectedWarm := int64(1)
			if grouped {
				column = "id"
				expectedWarm = 2
			}
			_, err = c.IndexBy(ctx, h.DB, column, "SELECT id,name FROM items", nil)
			require.NoError(t, err)
			require.Equal(t, expectedWarm, warm.Load())
			require.Zero(t, lazy.Load())
			lookup := func(refresh bool) (string, *cache.Stats) {
				m := &cache.ParmetrizedQuery{SQL: "SELECT id,name FROM items WHERE id = ?", Args: []any{1}, IdentitySQL: "SELECT id,name FROM items", IdentityArgs: []any{}, Limit: 10}
				if grouped {
					m.By = "id"
					m.In = []any{1}
				}
				type row struct {
					ID   int
					Name string
				}
				stats := &cache.Stats{}
				r, err := read.New(ctx, h.DB, m.SQL, func() any { return &row{} }, read.WithCache(c), read.WithInMatcher(m), read.WithCacheStats(stats), read.WithCacheRefresh(cache.Refresh(refresh)))
				require.NoError(t, err)
				defer func() {
					if stmt := r.Stmt(); stmt != nil {
						_ = stmt.Close()
					}
				}()
				name := ""
				require.NoError(t, r.QueryAll(ctx, func(v any) error { name = v.(*row).Name; return nil }, 1))
				return name, stats
			}
			h.Exec(t, "UPDATE items SET name='after'")
			name, stats := lookup(false)
			require.Equal(t, "before", name)
			require.True(t, stats.FoundWarmup)
			require.NotNil(t, stats.CreatedTime)
			require.NotNil(t, stats.ExpiryTime)
			require.Equal(t, 2*time.Second, stats.ExpiryTime.Sub(*stats.CreatedTime))
			warmCreated := *stats.CreatedTime
			_, warmAgain := lookup(false)
			require.Equal(t, warmCreated, *warmAgain.CreatedTime)
			name, refreshed := lookup(true)
			require.NotNil(t, refreshed.CreatedTime)
			require.NotNil(t, refreshed.ExpiryTime)
			require.False(t, refreshed.CreatedTime.Before(warmCreated))
			require.Equal(t, "after", name)
			require.Equal(t, int64(1), lazy.Load())
			name, stats = lookup(false)
			require.Equal(t, "after", name)
			require.True(t, stats.FoundLazy)
			require.Equal(t, refreshed.CreatedTime, stats.CreatedTime)
			require.Equal(t, refreshed.ExpiryTime, stats.ExpiryTime)
			require.Equal(t, int64(1), lazy.Load())
			h.Exec(t, "UPDATE items SET name='expired'")
			deadline := time.Now().Add(5 * time.Second)
			for {
				name, stats = lookup(false)
				if name == "expired" || time.Now().After(deadline) {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			require.Equal(t, "expired", name)
			require.False(t, stats.FoundAny())
			require.Equal(t, int64(2), lazy.Load())
			require.Equal(t, expectedWarm, warm.Load())
		})
	}
}
