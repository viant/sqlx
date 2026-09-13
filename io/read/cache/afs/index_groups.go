package afs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/sqlx/io/read/cache"
)

func (c *Cache) indexGroups(ctx context.Context, db *sql.DB, column string, query *cache.ParmetrizedQuery) (int, error) {
	if db == nil || strings.TrimSpace(query.SQL) == "" {
		return 0, fmt.Errorf("cache warmup requires a database and SQL")
	}
	markerSQL := query.IdentitySQL + "\n-- sqlx-cache-index:" + strings.ToLower(column)
	columns := query.ByColumns
	if len(columns) > 0 {
		encoded, _ := json.Marshal(columns)
		markerSQL = query.IdentitySQL + "\n-- sqlx-cache-columns:" + string(encoded)
	} else {
		columns = []string{column}
	}
	marker, err := c.Get(ctx, markerSQL, query.IdentityArgs)
	if err != nil {
		return 0, err
	}
	if marker == nil {
		return 0, fmt.Errorf("cache warmup index is already being written")
	}
	if marker.Has() {
		count := 0
		for marker.Next() {
			count++
		}
		return count, c.Close(ctx, marker)
	}
	entries := map[string]*cache.Entry{}
	counts := map[string]int{}
	committed := false
	defer func() {
		if !committed {
			for _, entry := range entries {
				c.discardIndexEntry(ctx, entry)
			}
			c.discardIndexEntry(ctx, marker)
		}
	}()
	rows, err := db.QueryContext(ctx, query.SQL, query.Args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if err := c.AssignRows(marker, rows); err != nil {
		return 0, err
	}
	marker.Meta.StoredFields = query.StoredFields
	marker.Meta.Partial = query.SQL != query.IdentitySQL
	marker.Meta.Generation = marker.Id
	columnIndexes := make([]int, 0, len(columns))
	for _, name := range columns {
		columnIndex := -1
		for i, field := range marker.Meta.Fields {
			if strings.EqualFold(field.Name(), name) {
				columnIndex = i
				break
			}
		}
		if columnIndex == -1 {
			return 0, fmt.Errorf("cache warmup index column %q was not returned by SQL", name)
		}
		columnIndexes = append(columnIndexes, columnIndex)
	}
	values := make([]interface{}, len(marker.Meta.Fields))
	for i := range values {
		values[i] = reflect.New(marker.Meta.Fields[i].ScanType()).Interface()
	}
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return 0, err
		}
		var keyValue any = values[columnIndexes[0]]
		if len(query.ByColumns) > 0 {
			tuple := make([]any, len(columnIndexes))
			for i, index := range columnIndexes {
				tuple[i] = values[index]
			}
			keyValue = tuple
		}
		key, err := json.Marshal(keyValue)
		if err != nil {
			return 0, err
		}
		entry := entries[string(key)]
		counts[string(key)]++
		if entry == nil {
			entry, err = c.Get(ctx, markerSQL+"\n-- generation:"+marker.Meta.Generation+"\n-- group:"+string(key), query.IdentityArgs)
			if err != nil {
				return 0, err
			}
			if entry == nil {
				return 0, fmt.Errorf("cache warmup group is already being written")
			}
			entry.Meta.Fields = marker.Meta.Fields
			entries[string(key)] = entry
		}
		if !entry.Has() {
			if err := c.AddValues(ctx, entry, values); err != nil {
				return 0, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for key, entry := range entries {
		if err := c.Close(ctx, entry); err != nil {
			return 0, err
		}
		// Marker membership distinguishes an absent group from an expired group.
		if err := c.AddValues(ctx, marker, []interface{}{key, counts[key]}); err != nil {
			return 0, err
		}
	}
	if err := c.writeMetaIfNeeded(ctx, marker); err != nil {
		return 0, err
	}
	if err := c.Close(ctx, marker); err != nil {
		return 0, err
	}
	committed = true
	return len(entries), nil
}

func (c *Cache) discardIndexEntry(ctx context.Context, entry *cache.Entry) {
	if entry == nil {
		return
	}
	_ = entry.Close()
	if !entry.Has() {
		_ = c.Rollback(context.WithoutCancel(ctx), entry)
	}
	if !entry.Has() {
		c.unmark(strings.ReplaceAll(entry.Meta.URL, ".json"+entry.Id, ".json"))
	}
}
