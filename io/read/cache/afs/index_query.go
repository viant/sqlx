package afs

import (
	"bufio"
	"context"
	"github.com/viant/sqlx/io/read/cache"
)

func (c *Cache) queryEntry(ctx context.Context, matcher *cache.ParmetrizedQuery) (*cache.Entry, error) {
	entry, err := c.Get(ctx, matcher.IdentitySQL, matcher.IdentityArgs)
	if err != nil || entry == nil {
		return nil, err
	}
	if !entry.Has() {
		c.discardIndexEntry(ctx, entry)
		return nil, nil
	}
	if !entry.Meta.ApplyProjection(matcher.RequestedFields) {
		c.discardIndexEntry(ctx, entry)
		return nil, nil
	}
	reader := &indexedReader{entries: []*cache.Entry{entry}, offset: matcher.Offset, limit: matcher.Limit}
	result := &cache.Entry{Meta: entry.Meta, Windowed: true}
	result.SetReader(bufio.NewReader(reader), reader)
	return result, nil
}
