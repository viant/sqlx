package insert_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/option"
	"github.com/viant/sqlx/testutil/sqlfault"
)

func TestExplicitCompositeInsertDoesNotDiscoverSession(t *testing.T) {
	type row struct {
		Order  int `sqlx:"order_id,primaryKey"`
		Region int `sqlx:"region_id,primaryKey"`
	}
	for _, mode := range []string{"success", "duplicate rollback", "caller transaction"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE mappings(order_id INTEGER,region_id INTEGER,PRIMARY KEY(order_id,region_id))")
			ctx := context.Background()
			dialect, err := config.Dialect(ctx, h.DB)
			if err != nil {
				t.Fatal(err)
			}
			metadataCalls := 0
			db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
				if strings.Contains(call.SQL, "pragma_database_list") {
					metadataCalls++
					return errors.New("explicit keys must not discover session metadata")
				}
				return nil
			})
			service, err := insert.New(ctx, db, "mappings", dialect)
			if err != nil {
				t.Fatal(err)
			}
			records := []*row{{Order: 1, Region: 2}, {Order: 1, Region: 3}}
			opts := []option.Option{option.BatchSize(1)}
			if mode == "duplicate rollback" {
				records[1].Region = 2
			}
			if mode == "caller transaction" {
				tx, txErr := db.BeginTx(ctx, nil)
				if txErr != nil {
					t.Fatal(txErr)
				}
				defer tx.Rollback()
				opts = append(opts, tx)
				affected, _, execErr := service.Exec(ctx, records, opts...)
				if execErr != nil || affected != 2 {
					t.Fatalf("insert: affected=%d err=%v", affected, execErr)
				}
				var count int
				if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM mappings").Scan(&count); err != nil || count != 2 {
					t.Fatalf("caller transaction rows=%d err=%v", count, err)
				}
				if err = tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			} else {
				affected, _, execErr := service.Exec(ctx, records, opts...)
				if mode == "success" && (execErr != nil || affected != 2) {
					t.Fatalf("insert: affected=%d err=%v", affected, execErr)
				}
				if mode == "duplicate rollback" && (execErr == nil || !strings.Contains(strings.ToLower(execErr.Error()), "unique")) {
					t.Fatalf("expected duplicate constraint failure: %v", execErr)
				}
			}
			var count int
			if err = h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM mappings").Scan(&count); err != nil {
				t.Fatal(err)
			}
			expected := 0
			if mode == "success" {
				expected = 2
			}
			if count != expected {
				t.Fatalf("persisted rows=%d want=%d", count, expected)
			}
			if metadataCalls != 0 {
				t.Fatalf("session metadata calls=%d", metadataCalls)
			}
		})
	}
}

func TestTaggedDefaultsRetainSessionDiscovery(t *testing.T) {
	type row struct {
		Key  string  `sqlx:"record_key,primaryKey"`
		Name *string `sqlx:"name,generator=default"`
	}
	h := sqlite.New(t, "CREATE TABLE records(record_key TEXT PRIMARY KEY,name TEXT DEFAULT 'generated')")
	ctx := context.Background()
	dialect, err := config.Dialect(ctx, h.DB)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("default generation requires session metadata")
	calls := 0
	db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if strings.Contains(call.SQL, "pragma_database_list") {
			calls++
			return sentinel
		}
		return nil
	})
	service, err := insert.New(ctx, db, "records", dialect)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = service.Exec(ctx, &row{Key: "key"})
	if !errors.Is(err, sentinel) || calls != 1 {
		t.Fatalf("default metadata discovery: calls=%d err=%v", calls, err)
	}
	var count int
	if err = h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed default generation changed rows: count=%d err=%v", count, err)
	}
}
