package aerospike

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

func TestIndexedWarmupCoverage(t *testing.T) {
	for _, test := range []struct {
		name                string
		partial             bool
		missingCompleteness bool
		ids                 []interface{}
		want                []coverageRow
	}{
		{name: "incomplete absent index falls back", partial: true, ids: []interface{}{2}, want: []coverageRow{{2, 20}}},
		{name: "incomplete populated group falls back", partial: true, ids: []interface{}{1}, want: []coverageRow{{1, 10}, {1, 11}}},
		{name: "incomplete mixed groups fall back", partial: true, ids: []interface{}{1, 2}, want: []coverageRow{{1, 10}, {1, 11}, {2, 20}}},
		{name: "unknown completeness falls back", missingCompleteness: true, ids: []interface{}{2}, want: []coverageRow{{2, 20}}},
		{name: "complete absent index is empty hit", ids: []interface{}{3}, want: []coverageRow{}},
		{name: "complete populated group replays", ids: []interface{}{1}, want: []coverageRow{{1, 10}, {1, 11}}},
		{name: "complete mixed present and absent groups replay", ids: []interface{}{1, 3}, want: []coverageRow{{1, 10}, {1, 11}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var sourceReads atomic.Int64
			driverName := "sqlx_coverage_" + uuid.NewString()
			sql.Register(driverName, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				return conn.RegisterFunc("source_read", func() int { sourceReads.Add(1); return 1 }, false)
			}})
			db, err := sql.Open(driverName, ":memory:")
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			_, err = db.Exec("CREATE TABLE facts(campaign_id INTEGER, amount INTEGER)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO facts VALUES(1,10),(1,11),(2,20)")
			require.NoError(t, err)

			var mu sync.Mutex
			records := map[string]as.BinMap{}
			native := &Cache{namespace: "fixture", set: "indexed_coverage", timeToLiveInSec: 60}
			native.putFn = func(key *as.Key, bins as.BinMap) error {
				mu.Lock()
				defer mu.Unlock()
				records[key.String()] = cloneBinMap(bins)
				return nil
			}
			native.getRecordFn = func(key *as.Key, bins ...string) (*as.Record, error) {
				mu.Lock()
				defer mu.Unlock()
				record, ok := records[key.String()]
				if !ok {
					return nil, types.NewAerospikeError(types.KEY_NOT_FOUND_ERROR)
				}
				return &as.Record{Bins: cloneBinMap(record)}, nil
			}
			const identitySQL = "SELECT campaign_id,amount FROM facts ORDER BY campaign_id,amount"
			warmupSQL := identitySQL
			if test.partial {
				warmupSQL += " LIMIT 1"
			}
			result, err := native.IndexByWithResult(context.Background(), db, "campaign_id", warmupSQL, nil, &cache.ParmetrizedQuery{SQL: warmupSQL, IdentitySQL: identitySQL})
			require.NoError(t, err)
			markerKey, err := native.key(result.MarkerKey)
			require.NoError(t, err)
			wantComplete := 1
			if test.partial {
				wantComplete = 0
			}
			require.Equal(t, wantComplete, records[markerKey.String()][completeBin])
			if test.missingCompleteness {
				delete(records[markerKey.String()], completeBin)
			}
			fallback := test.partial || test.missingCompleteness
			if !fallback {
				// A false miss must fail, even for an expected empty result.
				_, err = db.Exec("DROP TABLE facts")
				require.NoError(t, err)
			}
			placeholders := strings.TrimSuffix(strings.Repeat("?,", len(test.ids)), ",")
			executionSQL := fmt.Sprintf("SELECT campaign_id,amount FROM facts WHERE campaign_id IN (%s) AND source_read()=1 ORDER BY campaign_id,amount", placeholders)
			matcher := &cache.ParmetrizedQuery{SQL: executionSQL, Args: test.ids, IdentitySQL: identitySQL, By: "campaign_id", In: test.ids}
			stats := &cache.Stats{}
			reader, err := read.New(context.Background(), db, executionSQL, func() interface{} { return &coverageRow{} }, read.WithCache(native), read.WithInMatcher(matcher), read.WithCacheStats(stats))
			require.NoError(t, err)
			defer func() {
				if stmt := reader.Stmt(); stmt != nil {
					_ = stmt.Close()
				}
			}()
			rows := []coverageRow{}
			err = reader.QueryAll(context.Background(), func(value interface{}) error { rows = append(rows, *value.(*coverageRow)); return nil }, test.ids...)
			require.NoError(t, err)
			require.Equal(t, test.want, rows)
			require.Equal(t, !fallback, stats.FoundWarmup)
			require.False(t, stats.FoundLazy)
			if fallback {
				require.Greater(t, sourceReads.Load(), int64(0), "database fallback did not execute")
			} else {
				require.Zero(t, sourceReads.Load(), "warmup hit executed SQL")
			}
		})
	}
}

type coverageRow struct {
	CampaignID int `sqlx:"campaign_id"`
	Amount     int `sqlx:"amount"`
}
