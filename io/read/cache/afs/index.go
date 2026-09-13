package afs

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io/read/cache"
	"reflect"
	"strings"
)

// IndexBy warms the exact SQL-and-arguments cache entry using the same native
// writer as lazy reads. A column creates native group entries for matcher reads.
func (c *Cache) IndexBy(ctx context.Context, db *sql.DB, column, SQL string, args []interface{}, options ...interface{}) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	column = strings.TrimSpace(column)
	query := &cache.ParmetrizedQuery{SQL: SQL, Args: args, IdentitySQL: SQL, IdentityArgs: args}
	for _, option := range options {
		if supplied, ok := option.(*cache.ParmetrizedQuery); ok && supplied != nil {
			if supplied.IdentitySQL != "" {
				query.IdentitySQL = supplied.IdentitySQL
				query.IdentityArgs = supplied.IdentityArgs
			}
			query.StoredFields = append([]cache.ProjectionField(nil), supplied.StoredFields...)
			query.ByColumns = append([]string(nil), supplied.ByColumns...)
		}
	}
	if strings.TrimSpace(column) != "" || len(query.ByColumns) > 0 {
		if query.IdentityArgs == nil {
			query.IdentityArgs = []interface{}{}
		}
		return c.indexGroups(ctx, db, column, query)
	}
	if db == nil || strings.TrimSpace(SQL) == "" {
		return 0, fmt.Errorf("cache warmup requires a database and SQL")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	entry, err := c.Get(ctx, query.IdentitySQL, query.IdentityArgs)
	if err != nil {
		return 0, err
	}
	if entry == nil {
		return 0, fmt.Errorf("cache warmup entry is already being written")
	}
	if entry.Has() {
		return 1, c.Close(ctx, entry)
	}
	committed := false
	defer func() {
		if !committed {
			_ = entry.Close()
			_ = c.Rollback(context.WithoutCancel(ctx), entry)
			c.unmark(strings.ReplaceAll(entry.Meta.URL, ".json"+entry.Id, ".json"))
		}
	}()
	rows, err := db.QueryContext(ctx, SQL, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if err := c.AssignRows(entry, rows); err != nil {
		return 0, err
	}
	entry.Meta.StoredFields = query.StoredFields
	values := make([]interface{}, len(entry.Meta.Fields))
	for i := range values {
		values[i] = reflect.New(entry.Meta.Fields[i].ScanType()).Interface()
	}
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return 0, err
		}
		if err := c.AddValues(ctx, entry, values); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	// An empty result still needs metadata so a later read can replay it.
	if err := c.writeMetaIfNeeded(ctx, entry); err != nil {
		return 0, err
	}
	if err := c.Close(ctx, entry); err != nil {
		return 0, err
	}
	committed = true
	return 1, nil
}
