package aerospike

import (
	"context"
	"database/sql"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"path/filepath"
	"sync"
	"testing"
)

func scalarCompositeDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "facts.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec("CREATE TABLE facts(campaign_id INTEGER,account_id INTEGER,currency_id INTEGER,bucket INTEGER,amount INTEGER)")
	require.NoError(t, err)
	for bucket := 0; bucket < 55; bucket++ {
		for _, tuple := range [][3]int{{1, 3333, 1}, {1, 2222, 3}, {1, 2222, 1}, {1, 2222, 2}, {2, 2222, 1}} {
			_, err = db.Exec("INSERT INTO facts VALUES(?,?,?,?,?)", tuple[0], tuple[1], tuple[2], bucket, 10)
			require.NoError(t, err)
		}
	}
	return db
}

const scalarCompositeSQL = "SELECT campaign_id,account_id,currency_id,bucket,amount FROM facts ORDER BY bucket"

func scalarCompositeMatcher() *cache.ParmetrizedQuery {
	return &cache.ParmetrizedQuery{SQL: scalarCompositeSQL, IdentitySQL: scalarCompositeSQL, IdentityArgs: []any{}, By: "campaign_id", ByColumns: []string{"campaign_id", "account_id", "currency_id"}, InTuples: [][]any{{1, 2222, 1}, {1, 2222, 2}, {1, 2222, 1}, {2, 2222, 1}}, Limit: 40, Offset: 3}
}

type scalarCompositeRow struct {
	CampaignID int `sqlx:"campaign_id"`
	AccountID  int `sqlx:"account_id"`
	CurrencyID int `sqlx:"currency_id"`
	Bucket     int `sqlx:"bucket"`
	Amount     int `sqlx:"amount"`
}

func scalarCompositeRead(t *testing.T, db *sql.DB, native cache.Cache, matcher *cache.ParmetrizedQuery) ([]scalarCompositeRow, *cache.Stats, error) {
	t.Helper()
	stats := &cache.Stats{}
	reader, err := read.New(context.Background(), db, scalarCompositeSQL, func() any { return &scalarCompositeRow{} }, read.WithCache(native), read.WithInMatcher(matcher), read.WithCacheStats(stats))
	if err != nil {
		return nil, stats, err
	}
	defer func() {
		if stmt := reader.Stmt(); stmt != nil {
			_ = stmt.Close()
		}
	}()
	var rows []scalarCompositeRow
	err = reader.QueryAll(context.Background(), func(row any) error { rows = append(rows, *row.(*scalarCompositeRow)); return nil })
	return rows, stats, err
}
func assertScalarCompositeRows(t *testing.T, rows []scalarCompositeRow) {
	t.Helper()
	require.Len(t, rows, 120)
	counts := map[[3]int]int{}
	for _, row := range rows {
		require.Equal(t, 2222, row.AccountID)
		require.Contains(t, []int{1, 2}, row.CurrencyID)
		require.GreaterOrEqual(t, row.Bucket, 3)
		require.LessOrEqual(t, row.Bucket, 42)
		require.Equal(t, 10, row.Amount)
		counts[[3]int{row.CampaignID, row.AccountID, row.CurrencyID}]++
	}
	require.Equal(t, map[[3]int]int{{1, 2222, 1}: 40, {1, 2222, 2}: 40, {2, 2222, 1}: 40}, counts)
}

// Exercise actual native index publication, marker lookup, group streaming,
// projection and typed SQLX row windows with an in-memory Aerospike transport.
func TestScalarWarmupCompositeReplay(t *testing.T) {
	for _, publication := range []string{"complete", "partial", "unknown", "legacy_bool", "int64", "invalid"} {
		t.Run(publication, func(t *testing.T) {
			db := scalarCompositeDatabase(t)
			var mu sync.Mutex
			records := map[string]as.BinMap{}
			native := &Cache{namespace: "fixture", set: "scalar_composite", timeToLiveInSec: 60}
			native.putFn = func(key *as.Key, bins as.BinMap) error {
				mu.Lock()
				defer mu.Unlock()
				// Exercise the real v4 bin encoder before storing a transport fixture.
				// A plain map copy hides unsupported wire types such as bool.
				encoded := as.BinMap{}
				for name, value := range bins {
					require.NotPanics(t, func() {
						bin := as.NewBin(name, value)
						_, err := bin.Value.EstimateSize()
						require.NoError(t, err)
						encoded[name] = bin.Value.GetObject()
					}, "bin %s", name)
				}
				records[key.String()] = encoded
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
			SQL := scalarCompositeSQL
			if publication == "partial" {
				SQL += " LIMIT 100"
			}
			result, err := native.IndexByWithResult(context.Background(), db, "campaign_id", SQL, nil, &cache.ParmetrizedQuery{SQL: SQL, IdentitySQL: scalarCompositeSQL, IdentityArgs: []any{}})
			require.NoError(t, err)
			markerKey, err := native.key(result.MarkerKey)
			require.NoError(t, err)
			if publication == "unknown" {
				delete(records[markerKey.String()], completeBin)
			} else {
				wantMarker := 1
				if publication == "partial" {
					wantMarker = 0
				}
				require.Equal(t, wantMarker, records[markerKey.String()][completeBin])
			}
			switch publication {
			case "legacy_bool":
				records[markerKey.String()][completeBin] = true
			case "int64":
				records[markerKey.String()][completeBin] = int64(1)
			case "invalid":
				records[markerKey.String()][completeBin] = 2
			}
			_, err = db.Exec("DROP TABLE facts")
			require.NoError(t, err)
			matcher := scalarCompositeMatcher()
			rows, stats, err := scalarCompositeRead(t, db, native, matcher)
			if publication == "partial" || publication == "unknown" || publication == "invalid" {
				require.Error(t, err)
				require.False(t, stats.FoundWarmup)
				scalar := scalarCompositeMatcher()
				scalar.ByColumns, scalar.InTuples, scalar.In = nil, nil, []any{1}
				scalarRows, scalarStats, scalarErr := scalarCompositeRead(t, db, native, scalar)
				require.NoError(t, scalarErr)
				require.True(t, scalarStats.FoundWarmup)
				require.Len(t, scalarRows, 40)
				return
			}
			require.NoError(t, err)
			require.True(t, stats.FoundWarmup)
			require.False(t, stats.FoundLazy)
			assertScalarCompositeRows(t, rows)
			require.Equal(t, 40, matcher.Limit)
			require.Equal(t, 3, matcher.Offset)
			require.Len(t, matcher.ByColumns, 3)
		})
	}
}

func TestScalarCompositeLazyReplay(t *testing.T) {
	db := scalarCompositeDatabase(t)
	var mu sync.Mutex
	records := map[string]as.BinMap{}
	native := &Cache{namespace: "fixture", set: "scalar_lazy", timeToLiveInSec: 60}
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
	rows, _, err := scalarCompositeRead(t, db, native, scalarCompositeMatcher())
	require.NoError(t, err)
	assertScalarCompositeRows(t, rows)
	_, err = db.Exec("DROP TABLE facts")
	require.NoError(t, err)
	rows, stats, err := scalarCompositeRead(t, db, native, scalarCompositeMatcher())
	require.NoError(t, err)
	require.True(t, stats.FoundLazy)
	require.False(t, stats.FoundWarmup)
	assertScalarCompositeRows(t, rows)
}
