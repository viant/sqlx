package config

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"unsafe"

	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
)

var metaSessCache sync.Map

type metaKey struct {
	dbPtr   uintptr
	dialect string
}

func SessionCached(ctx context.Context, db *sql.DB, aDialect *info.Dialect) (*sink.Session, error) {

	//todo delete
	metaSessCache.Range(func(key, value any) bool {
		fmt.Printf("metaSessCache Key: %v, Value: %v\n", key, value)
		return true
	})
	// Resolve dialect from options or detect it.
	if aDialect == nil {
		return nil, fmt.Errorf("dialect was not provided")
	}
	//todo this need to be fixed, uintptr(unsafe.Pointer(db)) always different due to db, err := s.config.Connection.OpenDB(ctx)  for each insertService
	// Build cache key (db pointer + dialect name)
	key := metaKey{
		dbPtr:   uintptr(unsafe.Pointer(db)),
		dialect: aDialect.Name,
	}
	fmt.Printf("key=%v\n", key)

	if v, ok := metaSessCache.Load(key); ok {
		return v.(*sink.Session), nil
	}

	// Miss: create and store
	sess, err := Session(ctx, db, aDialect)
	if err != nil {
		return nil, err
	}

	// Double-check to avoid races
	if v, ok := metaSessCache.Load(key); ok {
		return v.(*sink.Session), nil
	}
	metaSessCache.Store(key, sess)
	return sess, nil
}
