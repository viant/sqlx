package read_test

import (
	"context"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/afs"
	"path/filepath"
	"testing"
	"time"
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

func TestScalarWarmupCompositeReplay(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "partial"}[partial], func(t *testing.T) {
			db := scalarCompositeDatabase(t)
			native, err := afs.NewCache(t.TempDir(), time.Minute, t.Name(), nil)
			require.NoError(t, err)
			SQL := scalarCompositeSQL
			if partial {
				SQL += " LIMIT 100"
			}
			_, err = native.IndexBy(context.Background(), db, "campaign_id", SQL, nil, &cache.ParmetrizedQuery{SQL: SQL, IdentitySQL: scalarCompositeSQL, IdentityArgs: []any{}})
			require.NoError(t, err)
			_, err = db.Exec("DROP TABLE facts")
			require.NoError(t, err)
			matcher := scalarCompositeMatcher()
			rows, stats, err := scalarCompositeRead(t, db, native, matcher)
			if partial {
				require.Error(t, err)
				require.False(t, stats.FoundWarmup)
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
	native, err := afs.NewCache(t.TempDir(), time.Minute, t.Name(), nil)
	require.NoError(t, err)
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

func TestNativeCompositeIndexRemainsCompatible(t *testing.T) {
	db := scalarCompositeDatabase(t)
	native, err := afs.NewCache(t.TempDir(), time.Minute, t.Name(), nil)
	require.NoError(t, err)
	warm := scalarCompositeMatcher()
	warm.By = ""
	_, err = native.IndexBy(context.Background(), db, "", scalarCompositeSQL, nil, warm)
	require.NoError(t, err)
	_, err = db.Exec("DROP TABLE facts")
	require.NoError(t, err)
	// Supplying an additional scalar candidate must preserve exact tuple indexes.
	rows, stats, err := scalarCompositeRead(t, db, native, scalarCompositeMatcher())
	require.NoError(t, err)
	require.True(t, stats.FoundWarmup)
	assertScalarCompositeRows(t, rows)
}
