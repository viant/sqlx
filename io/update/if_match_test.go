package update_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx/io/update"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/option"
)

type matchedUpdateRecord struct {
	Title      string `sqlx:"title"`
	Generation int    `sqlx:"generation"`
	ID         int    `sqlx:"id,primaryKey=true"`
}

func TestExecIfMatchUsesPersistedToken(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "match.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, `CREATE TABLE records (id INTEGER PRIMARY KEY, title TEXT, generation INTEGER); INSERT INTO records VALUES (1, 'old', 7)`); err != nil {
		t.Fatal(err)
	}
	service, err := update.New(ctx, db, "records")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		expected int
		changed  int64
	}{
		{name: "current", expected: 7, changed: 1},
		{name: "stale", expected: 7, changed: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed, err := service.Exec(ctx, &matchedUpdateRecord{ID: 1, Title: test.name, Generation: 8}, option.IfMatch{Column: "generation", Value: test.expected})
			if test.changed == 0 && !errors.Is(err, option.ErrNoMatch) {
				t.Fatalf("stale update error=%v, want ErrNoMatch", err)
			}
			if test.changed != 0 && err != nil || changed != test.changed {
				t.Fatalf("changed=%d err=%v, want %d", changed, err, test.changed)
			}
		})
	}
	var title string
	var generation int
	if err = db.QueryRowContext(ctx, `SELECT title,generation FROM records WHERE id=1`).Scan(&title, &generation); err != nil {
		t.Fatal(err)
	}
	if title != "current" || generation != 8 {
		t.Fatalf("persisted title=%q generation=%d", title, generation)
	}
	if _, err = service.Exec(ctx, &matchedUpdateRecord{ID: 1, Title: "invalid", Generation: 9}, option.IfMatch{Column: "unknown", Value: 8}); err == nil {
		t.Fatal("unmapped match column was accepted")
	}
}
