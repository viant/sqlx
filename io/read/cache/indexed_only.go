package cache

import (
	"context"
	"errors"
)

// IndexedOnly permits only a native indexed warmup hit. It never reads or
// creates an exact-query entry. A miss leaves ordinary source-query policy to
// the reader. It cannot be combined with Refresh.
type IndexedOnly bool

var ErrIndexedLookupUnsupported = errors.New("cache does not support indexed-only lookup")
var ErrIndexedRefresh = errors.New("indexed-only cache lookup cannot refresh")

// IndexedLookup is explicit so custom caches cannot silently ignore the
// indexed-only option and return an unguarded exact-query entry.
type IndexedLookup interface {
	LookupIndexed(context.Context, string, []interface{}, ...interface{}) (*Entry, error)
}
