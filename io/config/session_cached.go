package config

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
)

type metaKey struct {
	hashKey string
	dialect string
}

func SessionCached(ctx context.Context, db *sql.DB, aDialect *info.Dialect, metaSessionCacheKey string, cache *sync.Map) (*sink.Session, error) {
	// Resolve dialect from options or detect it.
	if aDialect == nil {
		return nil, fmt.Errorf("dialect was not provided")
	}

	// If no cache or key provided, fallback to creating a fresh session
	if cache == nil {
		return Session(ctx, db, aDialect)
	}

	key := metaKey{
		hashKey: metaSessionCacheKey,
		dialect: aDialect.Name,
	}

	if v, ok := cache.Load(key); ok {
		return v.(*sink.Session), nil
	}

	// Miss: create and store
	sess, err := Session(ctx, db, aDialect)

	if err != nil {
		return nil, err
	}

	// Double-check to avoid races
	if v, ok := cache.Load(key); ok {
		return v.(*sink.Session), nil
	}
	cache.Store(key, sess)
	return sess, nil
}
