package afs

import (
	"context"
	"fmt"
	"github.com/viant/sqlx/io/read/cache"
)

type lookupOnly bool

func (c *Cache) Lookup(ctx context.Context, SQL string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	for _, option := range options {
		if refresh, ok := option.(cache.Refresh); ok && bool(refresh) {
			return nil, fmt.Errorf("read-only cache lookup cannot refresh")
		}
	}
	options = append(append([]interface{}(nil), options...), lookupOnly(true))
	entry, err := c.Get(ctx, SQL, args, options...)
	if entry != nil {
		entry.ReadOnly = true
	}
	return entry, err
}

// LookupIndexed bypasses exact-query reads and writers even on an index miss.
func (c *Cache) LookupIndexed(ctx context.Context, SQL string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	options = append(append([]interface{}(nil), options...), cache.IndexedOnly(true))
	return c.Lookup(ctx, SQL, args, options...)
}

var _ cache.IndexedLookup = (*Cache)(nil)
