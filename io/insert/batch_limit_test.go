package insert_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/option"
	"github.com/viant/sqlx/testutil/sqlfault"
)

func TestInsertBatchLimitPreparesOnlyAvailableRows(t *testing.T) {
	type row struct {
		Order  int `sqlx:"order_id,primaryKey"`
		Region int `sqlx:"region_id,primaryKey"`
	}
	for _, size := range []int{25, 8000, 8001} {
		for _, mode := range []string{"owned", "caller rollback", "duplicate rollback"} {
			t.Run(fmt.Sprintf("%d/%s", size, mode), func(t *testing.T) {
				h := sqlite.New(t, "CREATE TABLE mappings(order_id INTEGER,region_id INTEGER,PRIMARY KEY(order_id,region_id))")
				ctx := context.Background()
				dialect, err := config.Dialect(ctx, h.DB)
				if err != nil {
					t.Fatal(err)
				}
				var prepared []int
				db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
					if call.Phase == "prepare" && strings.HasPrefix(call.SQL, "INSERT INTO") {
						prepared = append(prepared, strings.Count(call.SQL, "?"))
					}
					return nil
				})
				svc, err := insert.New(ctx, db, "mappings", dialect)
				if err != nil {
					t.Fatal(err)
				}
				records := make([]*row, size)
				for i := range records {
					records[i] = &row{Order: 1, Region: i + 1}
				}
				if mode == "duplicate rollback" {
					records[size-1].Region = 1
				}
				opts := []option.Option{option.BatchSize(8000)}
				if mode == "caller rollback" {
					tx, e := db.BeginTx(ctx, nil)
					if e != nil {
						t.Fatal(e)
					}
					defer tx.Rollback()
					opts = append(opts, tx)
					affected, _, e := svc.Exec(ctx, records, opts...)
					if e != nil || affected != int64(size) {
						t.Fatalf("affected=%d err=%v", affected, e)
					}
					var count int
					if e = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM mappings").Scan(&count); e != nil || count != size {
						t.Fatalf("caller count=%d err=%v", count, e)
					}
					if e = tx.Rollback(); e != nil {
						t.Fatal(e)
					}
				} else {
					affected, _, e := svc.Exec(ctx, records, opts...)
					if mode == "owned" && (e != nil || affected != int64(size)) {
						t.Fatalf("affected=%d err=%v", affected, e)
					}
					if mode == "duplicate rollback" && e == nil {
						t.Fatal("duplicate accepted")
					}
				}
				want := []int{2 * size}
				if size > 8000 {
					want = []int{16000, 2}
				}
				if !reflect.DeepEqual(prepared, want) {
					t.Fatalf("prepared placeholder counts=%v want=%v", prepared, want)
				}
				var count int
				if err = h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM mappings").Scan(&count); err != nil {
					t.Fatal(err)
				}
				expected := 0
				if mode == "owned" {
					expected = size
				}
				if count != expected {
					t.Fatalf("persisted=%d want=%d", count, expected)
				}
			})
		}
	}
}

func TestInsertShortBatchRetainsGeneratedIdentities(t *testing.T) {
	type row struct {
		ID   int64  `sqlx:"id,primaryKey,autoincrement"`
		Name string `sqlx:"name"`
	}
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)")
	ctx := context.Background()
	svc, err := insert.New(ctx, h.DB, "records")
	if err != nil {
		t.Fatal(err)
	}
	records := make([]*row, 25)
	for i := range records {
		records[i] = &row{Name: fmt.Sprint(i)}
	}
	affected, _, err := svc.Exec(ctx, records, option.BatchSize(8000), dialect.PresetIDWithMax)
	if err != nil || affected != 25 {
		t.Fatalf("affected=%d err=%v", affected, err)
	}
	for i, r := range records {
		if r.ID != int64(i+1) {
			t.Fatalf("row %d ID=%d", i, r.ID)
		}
	}
	var count int
	if err = h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 25 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
