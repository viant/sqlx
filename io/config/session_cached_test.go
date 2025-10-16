package config

import (
    "context"
    "database/sql"
    "sync"
    "testing"

    "github.com/viant/sqlx/metadata/database"
    "github.com/viant/sqlx/metadata/info"
    "github.com/viant/sqlx/metadata/registry"
    "github.com/viant/sqlx/metadata/sink"
)

// helper to register a KindSession query with a PreHandler that populates the sink
func registerTestSessionHandler(t *testing.T, productName string, onCall func()) {
    t.Helper()

    // Build a PreHandler that sets the session and prevents running any SQL
    handler := info.NewHandler(func(ctx context.Context, db *sql.DB, target interface{}, options ...interface{}) (bool, error) {
        if onCall != nil {
            onCall()
        }
        if sess, ok := target.(*sink.Session); ok {
            // populate with predictable values
            sess.PID = "p1"
            sess.Username = "u1"
            sess.Region = "r1"
            sess.Catalog = "c1"
            sess.Schema = "s1"
            sess.AppName = "a1"
        }
        // doNext=false to short-circuit query execution
        return false, nil
    })

    // Register a query for KindSession for the provided product
    q := info.NewQuery(info.KindSession, "SELECT 1", database.Product{Name: productName})
    q.OnPre(handler)

    if err := registry.Register(q); err != nil {
        t.Fatalf("failed to register test query: %v", err)
    }
}

func TestSessionCached_DialectNil(t *testing.T) {
    ctx := context.Background()
    _, err := SessionCached(ctx, nil, nil, "", &sync.Map{})
    if err == nil {
        t.Fatal("expected error when dialect is nil, got nil")
    }
}

func TestSessionCached_NoCacheFallback(t *testing.T) {
    ctx := context.Background()
    productName := "unittest"
    // Ensure metadata.Session uses our handler and does not touch DB
    registerTestSessionHandler(t, productName, nil)

    d := &info.Dialect{Product: database.Product{Name: productName}}

    got, err := SessionCached(ctx, nil, d, "key", nil) // cache is nil -> fallback to Session
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    want := &sink.Session{PID: "p1", Username: "u1", Region: "r1", Catalog: "c1", Schema: "s1", AppName: "a1"}
    if *got != *want {
        t.Fatalf("unexpected session. got %#v, want %#v", *got, *want)
    }
}

func TestSessionCached_CacheHit(t *testing.T) {
    ctx := context.Background()
    productName := "unittest_hit"
    calls := 0
    registerTestSessionHandler(t, productName, func() { calls++ })

    d := &info.Dialect{Product: database.Product{Name: productName}}
    cache := &sync.Map{}
    key := metaKey{hashKey: "abc", dialect: d.Name}
    cached := &sink.Session{PID: "cached"}
    cache.Store(key, cached)

    got, err := SessionCached(ctx, nil, d, "abc", cache)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if got != cached {
        t.Fatalf("expected cached pointer to be returned")
    }
    if calls != 0 {
        t.Fatalf("expected underlying Session not to be called on cache hit; calls=%d", calls)
    }
}

func TestSessionCached_CacheMissStoresAndReturns(t *testing.T) {
    ctx := context.Background()
    productName := "unittest_miss"
    calls := 0
    registerTestSessionHandler(t, productName, func() { calls++ })

    d := &info.Dialect{Product: database.Product{Name: productName}}
    cache := &sync.Map{}
    metaKeyStr := "misskey"

    got, err := SessionCached(ctx, nil, d, metaKeyStr, cache)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if calls != 1 {
        t.Fatalf("expected underlying Session to be called once on miss; calls=%d", calls)
    }

    want := &sink.Session{PID: "p1", Username: "u1", Region: "r1", Catalog: "c1", Schema: "s1", AppName: "a1"}
    if *got != *want {
        t.Fatalf("unexpected session. got %#v, want %#v", *got, *want)
    }

    // ensure stored in cache under the computed key
    key := metaKey{hashKey: metaKeyStr, dialect: d.Name}
    if v, ok := cache.Load(key); !ok {
        t.Fatalf("expected value stored in cache for key: %#v", key)
    } else {
        if v != got {
            t.Fatalf("expected same session pointer stored in cache")
        }
    }
}

