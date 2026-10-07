package aerospike

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"github.com/viant/sqlx/io/read/cache"
)

func TestIndexedOnlyMissNeverReadsLazyOrCreatesWriter(t *testing.T) {
	c := &Cache{namespace: "synthetic", set: "indexed-only"}
	var calls atomic.Int32
	c.getRecordFn = func(*as.Key, ...string) (*as.Record, error) {
		calls.Add(1)
		return nil, types.NewAerospikeError(types.KEY_NOT_FOUND_ERROR)
	}
	matcher := &cache.ParmetrizedQuery{SQL: "SELECT id FROM records WHERE id=?", Args: []any{1}, IdentitySQL: "SELECT id FROM records", By: "id", In: []any{1}}
	entry, err := c.LookupIndexed(context.Background(), matcher.SQL, matcher.Args, matcher)
	if err != nil || entry != nil || calls.Load() != 1 {
		t.Fatalf("index miss escaped indexed-only path: entry=%v err=%v reads=%d", entry, err, calls.Load())
	}
	calls.Store(0)
	entry, err = c.LookupIndexed(context.Background(), matcher.SQL, matcher.Args, matcher, cache.Refresh(true))
	if !errors.Is(err, cache.ErrIndexedRefresh) || entry != nil || calls.Load() != 0 {
		t.Fatalf("refresh touched cache: entry=%v err=%v reads=%d", entry, err, calls.Load())
	}
	entry, err = c.LookupIndexed(context.Background(), matcher.SQL, matcher.Args)
	if err != nil || entry != nil || calls.Load() != 0 {
		t.Fatalf("missing index touched lazy cache: entry=%v err=%v reads=%d", entry, err, calls.Load())
	}
}

func TestIndexedReadOnlyEntryCannotDeleteSharedCache(t *testing.T) {
	c := &Cache{}
	for _, closeEntry := range []func(context.Context, *cache.Entry) error{c.Delete, c.Rollback, c.Close} {
		if err := closeEntry(context.Background(), &cache.Entry{ReadOnly: true}); err != nil {
			t.Fatalf("read-only cleanup reached backend: %v", err)
		}
	}
}
