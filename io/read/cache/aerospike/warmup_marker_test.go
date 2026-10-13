package aerospike

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/read/cache"
)

func TestCompleteWarmupMarker(t *testing.T) {
	for _, tc := range []struct {
		name     string
		value    interface{}
		complete bool
	}{
		{"int_complete", 1, true},
		{"int_partial", 0, false},
		{"int64_complete", int64(1), true},
		{"int64_partial", int64(0), false},
		{"legacy_complete", true, true},
		{"legacy_partial", false, false},
		{"missing", nil, false},
		{"invalid_positive", 2, false},
		{"invalid_negative", -1, false},
		{"invalid_int64", int64(2), false},
		{"string", "true", false},
		{"float", 1.0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.complete, isCompleteWarmup(tc.value))
		})
	}
}

// Non-indexed warmup publishes metadata on its root data record too, so the
// driver contract must hold even when no separate index marker is written.
func TestNonIndexedWarmupMarkerEncoding(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "partial"}[partial], func(t *testing.T) {
			db := scalarCompositeDatabase(t)
			native := &Cache{namespace: "fixture", set: "marker_encoding", timeToLiveInSec: 60}
			writes := 0
			native.putFn = func(key *as.Key, bins as.BinMap) error {
				writes++
				for name, value := range bins {
					require.NotPanics(t, func() {
						_, err := as.NewBin(name, value).Value.EstimateSize()
						require.NoError(t, err)
					}, "bin %s", name)
				}
				want := 1
				if partial {
					want = 0
				}
				require.Equal(t, want, bins[completeBin])
				return nil
			}
			SQL := scalarCompositeSQL
			if partial {
				SQL += " LIMIT 100"
			}
			_, err := native.IndexByWithResult(context.Background(), db, "", SQL, nil, &cache.ParmetrizedQuery{SQL: SQL, IdentitySQL: scalarCompositeSQL, IdentityArgs: []any{}})
			require.NoError(t, err)
			require.Greater(t, writes, 0)
		})
	}
}

func TestIndexedBooleanRowsRemainJSONBooleans(t *testing.T) {
	native := &Cache{namespace: "fixture", set: "boolean_rows"}
	native.putFn = func(key *as.Key, bins as.BinMap) error {
		require.Equal(t, "[true,false]", bins[dataBin])
		require.NotPanics(t, func() { as.NewBin(dataBin, bins[dataBin]) })
		return nil
	}
	indexed, err := newIndexedValue(native, 1, "id", "boolean_fixture", as.BinMap{}, false)
	require.NoError(t, err)
	_, err = indexed.appendRow([]interface{}{true, false})
	require.NoError(t, err)
	require.NoError(t, indexed.close())
}

func TestNativeScalarWarmupMarkerReplay(t *testing.T) {
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
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "partial"}[partial], func(t *testing.T) {
			db := scalarCompositeDatabase(t)
			native, err := New(strings.TrimPrefix(u.Path, "/"), "sqlx_warm_marker_test", client, 60)
			require.NoError(t, err)
			native.identityPrefix = uuid.NewString() + "/"
			SQL := scalarCompositeSQL
			want := 1
			if partial {
				SQL += " LIMIT 100"
				want = 0
			}
			result, err := native.IndexByWithResult(context.Background(), db, "campaign_id", SQL, nil, &cache.ParmetrizedQuery{SQL: SQL, IdentitySQL: scalarCompositeSQL, IdentityArgs: []any{}})
			require.NoError(t, err)
			key, err := native.key(result.MarkerKey)
			require.NoError(t, err)
			record, err := client.Get(nil, key, completeBin)
			require.NoError(t, err)
			require.Equal(t, want, record.Bins[completeBin])
			_, err = db.Exec("DROP TABLE facts")
			require.NoError(t, err)
			rows, stats, err := scalarCompositeRead(t, db, native, scalarCompositeMatcher())
			if partial {
				require.Error(t, err)
				require.False(t, stats.FoundWarmup)
				return
			}
			require.NoError(t, err)
			require.True(t, stats.FoundWarmup)
			assertScalarCompositeRows(t, rows)
		})
	}
}
