package cache

import (
	"context"
	"errors"
)

var ErrMiss = errors.New("native read cache entry is missing or expired")
var ErrLookupUnsupported = errors.New("native cache does not support read-only lookup")

// Lookup is an optional native capability that never acquires a writer lease or
// creates/deletes a payload. Missing/expired entries return nil; Reader maps them
// to ErrMiss without falling through to a database query.
type Lookup interface {
	Lookup(context.Context, string, []interface{}, ...interface{}) (*Entry, error)
}
