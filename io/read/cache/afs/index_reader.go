package afs

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/viant/sqlx/io/read/cache"
)

func (c *Cache) indexedEntry(ctx context.Context, matcher *cache.ParmetrizedQuery) (*cache.Entry, error) {
	SQL, args, _, err := matcher.WarmupIdentity()
	if err != nil {
		return nil, err
	}
	markerSQL := SQL + "\n-- sqlx-cache-index:" + strings.ToLower(matcher.By)
	if len(matcher.ByColumns) > 0 {
		encoded, _ := json.Marshal(matcher.ByColumns)
		markerSQL = SQL + "\n-- sqlx-cache-columns:" + string(encoded)
	}
	marker, err := c.Get(ctx, markerSQL, args)
	if err != nil || marker == nil {
		return nil, err
	}
	defer c.discardIndexEntry(ctx, marker)
	if !marker.Has() {
		return nil, nil
	}
	if !marker.Meta.ApplyProjection(matcher.RequestedFields) {
		return nil, nil
	}
	keys := map[string]int{}
	for marker.Next() {
		var record []json.RawMessage
		if err := json.Unmarshal(marker.Data, &record); err != nil || len(record) != 2 {
			return nil, fmt.Errorf("invalid cache index marker")
		}
		var key string
		var count int
		if json.Unmarshal(record[0], &key) != nil || json.Unmarshal(record[1], &count) != nil || count < 0 {
			return nil, fmt.Errorf("invalid cache index membership")
		}
		keys[key] = count
	}
	reader := &indexedReader{offset: matcher.Offset, limit: matcher.Limit}
	seen := map[string]bool{}
	requested := matcher.In
	if len(matcher.ByColumns) > 0 {
		requested = make([]any, len(matcher.InTuples))
		for i, tuple := range matcher.InTuples {
			if len(tuple) != len(matcher.ByColumns) {
				return nil, fmt.Errorf("invalid matcher tuple width")
			}
			requested[i] = tuple
		}
	}
	for _, value := range requested {
		key, err := json.Marshal(value)
		if err != nil {
			_ = reader.Close()
			return nil, err
		}
		count, known := keys[string(key)]
		if marker.Meta.Partial && (!known || matcher.Limit <= 0 || matcher.Offset > count || matcher.Limit > count-matcher.Offset) {
			_ = reader.Close()
			return nil, nil
		}
		if seen[string(key)] || !known {
			continue
		}
		seen[string(key)] = true
		entry, err := c.Get(ctx, markerSQL+"\n-- generation:"+marker.Meta.Generation+"\n-- group:"+string(key), args)
		if err != nil || entry == nil || !entry.Has() {
			c.discardIndexEntry(ctx, entry)
			_ = reader.Close()
			return nil, err // Expired or unavailable groups must fall through to SQL.
		}
		reader.entries = append(reader.entries, entry)
	}
	result := &cache.Entry{Meta: marker.Meta, Windowed: true}
	result.SetReader(bufio.NewReader(reader), reader)
	return result, nil
}

// indexedReader streams the requested groups and applies pagination per group;
// native SQLX scanners retain ownership of typed destination binding.
type indexedReader struct {
	entries                     []*cache.Entry
	index, count, offset, limit int
	buffer                      []byte
}

func (r *indexedReader) Read(target []byte) (int, error) {
	for len(r.buffer) == 0 {
		if r.index >= len(r.entries) {
			return 0, io.EOF
		}
		entry := r.entries[r.index]
		if r.limit > 0 && r.count-r.offset >= r.limit {
			r.index++
			r.count = 0
			continue
		}
		if !entry.Next() {
			r.index++
			r.count = 0
			continue
		}
		r.count++
		if r.count <= r.offset || r.limit > 0 && r.count > r.offset+r.limit {
			continue
		}
		r.buffer = append(append(r.buffer[:0], entry.Data...), '\n')
	}
	n := copy(target, r.buffer)
	r.buffer = r.buffer[n:]
	return n, nil
}

func (r *indexedReader) Close() error {
	var first error
	for _, entry := range r.entries {
		if err := entry.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
