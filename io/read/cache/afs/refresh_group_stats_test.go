package afs

import (
	"context"
	"github.com/viant/sqlx/io/read/cache"
	"testing"
	"time"
)

func TestGroupedRefreshConflictStats(t *testing.T) {
	ctx := context.Background()
	c, _ := NewCache(t.TempDir(), time.Minute, "group-conflict", nil)
	matcher := &cache.ParmetrizedQuery{IdentitySQL: "SELECT id FROM items", IdentityArgs: []any{}, By: "id", In: []any{1}}
	SQL, args, _, err := matcher.WarmupIdentity()
	if err != nil {
		t.Fatal(err)
	}
	markerSQL := indexSQL(SQL, matcher.By, matcher.ByColumns)
	initial := &cache.Stats{}
	entry, err := c.Get(ctx, markerSQL, args, initial)
	if err != nil || entry == nil {
		t.Fatal(err)
	}
	defer c.Rollback(ctx, entry)
	stats := &cache.Stats{FoundWarmup: true, FoundLazy: true}
	if _, err = c.Get(ctx, "SELECT id FROM items WHERE id=?", []any{1}, matcher, cache.Refresh(true), stats); err == nil {
		t.Fatal("active marker writer refreshed")
	}
	if stats.ErrorType != "afs" || stats.Key != initial.Key || stats.MarkerKey != initial.Key || stats.WarmupKey != initial.Key || stats.FoundLazy || stats.FoundWarmup {
		t.Fatalf("conflict metadata %+v, owner %+v", stats, initial)
	}
	if another, err := c.Get(ctx, markerSQL, args); err != nil || another != nil {
		t.Fatal("marker lease lost")
	}
}
