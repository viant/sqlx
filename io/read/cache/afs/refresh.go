package afs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/hash"
	"strings"
)

// indexSQL is the common identity used by group publication and lookup.
func indexSQL(SQL, column string, columns []string) string {
	if len(columns) > 0 {
		encoded, _ := json.Marshal(columns)
		return SQL + "\n-- sqlx-cache-columns:" + string(encoded)
	}
	return SQL + "\n-- sqlx-cache-index:" + strings.ToLower(column)
}

// refreshWarmup retires the matching prewarmed publication before refreshing the
// exact query. Existing readers may finish, but later readers cannot select the
// stale publication over the newly refreshed query. Group data remains governed
// by its marker generation and TTL; retiring the marker makes it unreachable.
func (c *Cache) refreshWarmup(ctx context.Context, query *cache.ParmetrizedQuery, stats *cache.Stats) error {
	SQL, args := query.IdentitySQL, query.IdentityArgs
	if query.By != "" || len(query.ByColumns) > 0 {
		var err error
		SQL, args, _, err = query.WarmupIdentity()
		if err != nil {
			return err
		}
		SQL = indexSQL(SQL, query.By, query.ByColumns)
	} else if SQL == "" {
		return nil
	}
	URL, err := hash.GenerateURL(SQL, c.storage, c.extension, args)
	if err != nil {
		return err
	}
	if stats != nil {
		stats.Key = URL
		stats.WarmupKey = URL
		if query.By != "" || len(query.ByColumns) > 0 {
			stats.MarkerKey = URL
		}
	}
	if c.mark(URL) {
		return fmt.Errorf("cache refresh conflicts with an active warmup writer")
	}
	defer c.unmark(URL)
	exists, err := c.afs.Exists(ctx, URL)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	return c.afs.Delete(ctx, URL)
}
