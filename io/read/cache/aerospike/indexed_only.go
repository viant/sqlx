package aerospike

import (
	"context"
	"github.com/viant/sqlx/io/read/cache"
)

func (a *Cache) LookupIndexed(ctx context.Context, SQL string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	options = append(append([]interface{}(nil), options...), cache.IndexedOnly(true))
	return a.Get(ctx, SQL, args, options...)
}

var _ cache.IndexedLookup = (*Cache)(nil)
