package read_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/afs"
	"github.com/viant/sqlx/io/read/cache/hash"
)

type lookupRow struct {
	ID int `sqlx:"id"`
}

func TestReadOnlyNativeCacheSQLite(t *testing.T) {
	for _, mode := range []string{"hit", "missing", "expired", "empty", "emit error", "scope mismatch", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			db := sqlite.New(t, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(7)")
			root := t.TempDir()
			native, err := afs.NewCache(root, time.Minute, "readonly", nil)
			require.NoError(t, err)
			query := "SELECT id FROM records WHERE id = ?"
			args := []interface{}{7}
			if mode == "empty" {
				args = []interface{}{9}
			}
			if mode != "missing" {
				writer, err := read.New(context.Background(), db.DB, query, func() any { return &lookupRow{} }, read.WithCache(native))
				require.NoError(t, err)
				require.NoError(t, writer.QueryAll(context.Background(), func(any) error { return nil }, args...))
				if writer.Stmt() != nil {
					require.NoError(t, writer.Stmt().Close())
				}
			}
			before, err := filepath.Glob(filepath.Join(root, "*.json"))
			require.NoError(t, err)
			if mode == "expired" {
				payload, err := os.ReadFile(before[0])
				require.NoError(t, err)
				// Use actual native metadata rather than waiting or modifying its clock.
				var boundary int
				for boundary < len(payload) && payload[boundary] != '\n' {
					boundary++
				}
				var meta map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(payload[:boundary], &meta))
				meta["ExpiryTimeMs"] = json.RawMessage("1")
				header, err := json.Marshal(meta)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(before[0], append(header, payload[boundary:]...), 0600))
			}
			db.Exec(t, "DROP TABLE records")
			scope, err := read.NewQueryScope([]cache.ParmetrizedQuery{{SQL: query, Args: args}})
			require.NoError(t, err)
			if mode == "scope mismatch" {
				args = []interface{}{99}
			}
			stats := &cache.Stats{}
			reader, err := read.New(context.Background(), db.DB, query, func() any { return &lookupRow{} }, read.WithCache(native), read.WithCacheOnly(true), read.WithQueryScope(scope), read.WithCacheStats(stats))
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			sentinel := errors.New("consumer failed")
			err = reader.QueryAll(ctx, func(row any) error {
				calls++
				if mode == "canceled" {
					cancel()
				}
				if mode == "emit error" {
					return sentinel
				}
				require.Equal(t, 7, row.(*lookupRow).ID)
				return nil
			}, args...)
			switch mode {
			case "missing", "expired":
				require.ErrorIs(t, err, cache.ErrMiss)
				require.Zero(t, calls)
			case "scope mismatch":
				require.ErrorIs(t, err, read.ErrQueryOutsideScope)
				require.Zero(t, calls)
			case "canceled":
				require.ErrorIs(t, err, context.Canceled)
			case "emit error":
				require.ErrorIs(t, err, sentinel)
			default:
				require.NoError(t, err)
				require.True(t, stats.FoundAny())
				if mode == "empty" {
					require.Zero(t, calls)
				} else {
					require.Equal(t, 1, calls)
				}
			}
			require.Nil(t, reader.Stmt(), "cache-only lookup prepared a database statement")
			after, err := filepath.Glob(filepath.Join(root, "*.json"))
			require.NoError(t, err)
			require.Equal(t, before, after, "read-only lookup changed cache entries")
			// A miss must not leave a writer lease that prevents subsequent population.
			if mode == "missing" {
				db.Exec(t, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(7)")
				writer, err := read.New(context.Background(), db.DB, query, func() any { return &lookupRow{} }, read.WithCache(native))
				require.NoError(t, err)
				require.NoError(t, writer.QueryAll(context.Background(), func(any) error { return nil }, 7))
				if writer.Stmt() != nil {
					writer.Stmt().Close()
				}
				entry, err := native.Lookup(context.Background(), query, []interface{}{7})
				require.NoError(t, err)
				require.NotNil(t, entry)
				require.NoError(t, entry.Close())
			}
		})
	}
}
func TestRecordedQueryScopeExactExecutionIdentity(t *testing.T) {
	scope, err := read.NewQueryScope([]cache.ParmetrizedQuery{{SQL: "SELECT ?", Args: []interface{}{json.Number("9007199254740993")}}})
	require.NoError(t, err)
	require.NoError(t, scope.Validate("SELECT ?", []interface{}{int64(9007199254740993)}))
	require.ErrorIs(t, scope.Validate("SELECT ?", []interface{}{int64(9007199254740992)}), read.ErrQueryOutsideScope)
	// Case and whitespace variants may share a native cache key but must not
	// silently expand the recorded execution scope.
	args := []interface{}{int64(9007199254740993)}
	key, err := hash.GenerateURL("SELECT ?", "", ".json", args)
	require.NoError(t, err)
	for _, query := range []string{"select ?", "SELECT  ?", "SELECT\t?", "select\n ?"} {
		t.Run(query, func(t *testing.T) {
			alias, err := hash.GenerateURL(query, "", ".json", args)
			require.NoError(t, err)
			require.Equal(t, key, alias)
			require.ErrorIs(t, scope.Validate(query, args), read.ErrQueryOutsideScope)
		})
	}
}

func TestReadOnlyNativeWarmupSQLite(t *testing.T) {
	db := sqlite.New(t, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(7)")
	native, err := afs.NewCache(t.TempDir(), time.Minute, "warm lookup", nil)
	require.NoError(t, err)
	query := "SELECT id FROM records"
	_, err = native.IndexBy(context.Background(), db.DB, "", query, nil)
	require.NoError(t, err)
	db.Exec(t, "DROP TABLE records")
	stats := &cache.Stats{}
	reader, err := read.New(context.Background(), db.DB, query, func() any { return &lookupRow{} }, read.WithCache(native), read.WithCacheOnly(true), read.WithCacheStats(stats), read.WithInMatcher(&cache.ParmetrizedQuery{SQL: query, IdentitySQL: query}))
	require.NoError(t, err)
	count := 0
	require.NoError(t, reader.QueryAll(context.Background(), func(row any) error { count++; require.Equal(t, 7, row.(*lookupRow).ID); return nil }))
	require.Equal(t, 1, count)
	require.True(t, stats.FoundWarmup)
	require.Nil(t, reader.Stmt())
}

// Normal cache reads and explicit refresh reject query drift before preparing SQL
// or acquiring a native cache writer, including when there is no cached entry.
func TestRecordedQueryScopeGuardsNormalFallbackSQLite(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		t.Run(fmt.Sprint(refresh), func(t *testing.T) {
			db := sqlite.New(t) // No records table: reaching SQL would return a different error.
			root := t.TempDir()
			native, err := afs.NewCache(root, time.Minute, "guard", nil)
			require.NoError(t, err)
			scope, err := read.NewQueryScope([]cache.ParmetrizedQuery{{SQL: "SELECT id FROM records WHERE id = ?", Args: []interface{}{7}}})
			require.NoError(t, err)
			for _, single := range []bool{false, true} {
				reader, err := read.New(context.Background(), db.DB, "SELECT id FROM records WHERE id = ?", func() any { return &lookupRow{} }, read.WithCache(native), read.WithCacheRefresh(cache.Refresh(refresh)), read.WithQueryScope(scope))
				require.NoError(t, err)
				query := reader.QueryAll
				if single {
					query = reader.QuerySingle
				}
				err = query(context.Background(), func(any) error { t.Fatal("rejected query emitted a row"); return nil }, 8)
				require.ErrorIs(t, err, read.ErrQueryOutsideScope)
				require.Nil(t, reader.Stmt())
			}
			files, err := filepath.Glob(filepath.Join(root, "*.json"))
			require.NoError(t, err)
			require.Empty(t, files)
		})
	}
}
