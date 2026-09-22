package afs

import (
	"github.com/viant/sqlx/io/read/cache"
)

// observeEntry reports the actual source decision without reading extra rows.
// AFS does not expose physical cache-record counts; RecordsCounter stays zero.
func (c *Cache) observeEntry(stats *cache.Stats, entry *cache.Entry, err error) {
	if stats == nil {
		return
	}
	stats.Namespace = c.storage
	stats.Dataset = c.signature
	if err != nil {
		stats.ErrorType = "afs"
		return
	}
	if entry == nil {
		stats.Type = cache.TypeReadSingle
		return
	}
	if stats.Key == "" || stats.FoundWarmup {
		stats.Key = entry.Meta.URL
	}
	entry.Meta.ObserveTimes(stats)

	if entry.Has() {
		if !stats.FoundWarmup {
			stats.Type = cache.TypeReadSingle
			stats.FoundLazy = true
		}
		return
	}
	stats.Type = cache.TypeWrite
}
