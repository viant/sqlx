package insert_test

import (
	"context"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/option"
	"sync"
	"testing"
	"time"
)

func TestNextSequenceUsesCallerTransactionForColdMetadata(t *testing.T) {
	type row struct {
		ID   int64  `sqlx:"id,primaryKey"`
		Name string `sqlx:"name"`
	}
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary metadata", true: "cache miss metadata"}[cached], func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)")
			h.DB.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(77,'uncommitted')"); err != nil {
				t.Fatal(err)
			}
			var options []option.Option
			if cached {
				options = append(options, option.MetaSessionCacheKey("records"), option.MetaSessionCache{Map: &sync.Map{}})
			}
			service, err := insert.New(ctx, h.DB, "records", options...)
			if err != nil {
				t.Fatal(err)
			}
			sequence, err := service.NextSequence(ctx, &row{Name: "next"}, 2, tx, dialect.PresetIDWithMax)
			if err != nil {
				t.Fatal(err)
			}
			if got := sequence.MinValue(2); got != 78 {
				t.Fatalf("transaction MAX invisible: %d", got)
			}
		})
	}
}
