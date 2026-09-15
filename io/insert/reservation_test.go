package insert_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/testutil/sqlfault"
)

type reservationRow struct {
	ID int64 `sqlx:"id,primaryKey,autoincrement"`
}

func TestReservationSQLiteEngineCounterAndNoBusinessRows(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)", "CREATE TABLE audit(n INTEGER)", "CREATE TRIGGER track BEFORE INSERT ON records BEGIN INSERT INTO audit VALUES(1); END", "CREATE TABLE plain(id INTEGER PRIMARY KEY)")
	service, err := insert.New(ctx, h.DB, "records")
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.NextSequence(ctx, &reservationRow{}, 2, dialect.PresetIDWithReservation)
	if err != nil || result.MinValue(2) != 1 {
		t.Fatalf("range=%+v err=%v", result, err)
	}
	for _, table := range []string{"records", "audit"} {
		var n int
		if err = h.DB.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s rows=%d err=%v", table, n, err)
		}
	}
	if _, err = h.DB.ExecContext(ctx, "INSERT INTO records(name) VALUES('real')"); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err = h.DB.QueryRowContext(ctx, "SELECT id FROM records").Scan(&id); err != nil || id != 3 {
		t.Fatalf("automatic insert collided with reserved range: %d %v", id, err)
	}

	plain, _ := insert.New(ctx, h.DB, "plain")
	first, err := plain.NextSequence(ctx, &reservationRow{}, 2, dialect.PresetIDWithReservation)
	if err != nil || first.MinValue(2) != 1 {
		t.Fatalf("ordinary numeric range: %+v %v", first, err)
	}
	other, _ := insert.New(ctx, h.DB, "plain")
	second, err := other.NextSequence(ctx, &reservationRow{}, 1, dialect.PresetIDWithReservation)
	if err != nil || second.MinValue(1) != 3 {
		t.Fatalf("durable ordinary numeric range: %+v %v", second, err)
	}
	var n int
	if err = h.DB.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_sequence WHERE name='plain'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("invented SQLite engine counter: %d %v", n, err)
	}
	if err = h.DB.QueryRowContext(ctx, "SELECT value FROM sqlx_sequence_reservations WHERE table_name='plain'").Scan(&n); err != nil || n != 3 {
		t.Fatalf("native SQLX counter: %d %v", n, err)
	}

}

func TestReservationSQLiteStaleCallerSnapshotAndCancellation(t *testing.T) {
	for _, mode := range []string{"snapshot", "cancelled", "waiting"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "counter.db") + "?_busy_timeout=200&_journal_mode=WAL"
			h := sqlite.NewWithDSN(t, path, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)")
			db := h.DB
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var blocker *sql.Tx
			if mode == "snapshot" {
				var n int
				if err = tx.QueryRow("SELECT count(*) FROM records").Scan(&n); err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec("INSERT INTO records DEFAULT VALUES"); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "waiting" {
				blocker, err = db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer blocker.Rollback()
				if _, err = blocker.Exec("INSERT INTO records DEFAULT VALUES"); err != nil {
					t.Fatal(err)
				}
			}
			callCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			service, _ := insert.New(ctx, db, "records")
			row := &reservationRow{}
			result, err := service.NextSequence(callCtx, row, 1, tx, dialect.PresetIDWithReservation)
			if err == nil || result != nil || row.ID != 0 {
				t.Fatalf("failure assigned IDs: %+v %+v %v", result, row, err)
			}
			if mode != "snapshot" && !errors.Is(err, callCtx.Err()) {
				t.Fatalf("cancellation identity lost: %v (context %v)", err, callCtx.Err())
			}
			var n int
			if err = tx.QueryRow("SELECT count(*) FROM records").Scan(&n); err != nil {
				t.Fatalf("allocator completed caller transaction: %v", err)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReservationSQLiteConcurrentFirstUse(t *testing.T) {
	for _, journal := range []string{"DELETE", "WAL"} {
		t.Run(journal, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			h := sqlite.New(t, "PRAGMA journal_mode="+journal, "CREATE TABLE records(id INTEGER PRIMARY KEY)")
			creating := make(chan struct{}, 2)
			release := make(chan struct{})
			before := func(ctx context.Context, call sqlfault.Call) error {
				if call.Phase == "prepare" && strings.HasPrefix(call.SQL, `CREATE TABLE "main"."sqlx_sequence_reservations"`) {
					creating <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}
			type outcome struct {
				id  int64
				err error
			}
			done := make(chan outcome, 2)
			for i := 0; i < 2; i++ {
				db := h.FaultDB(t, before)
				go func() {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						done <- outcome{err: err}
						return
					}
					defer tx.Rollback()
					service, _ := insert.New(ctx, db, "records")
					result, err := service.NextSequence(ctx, &reservationRow{}, 1, tx, dialect.PresetIDWithReservation)
					var id int64
					if err == nil {
						id = result.MinValue(1)
						err = tx.Commit()
					}
					done <- outcome{id, err}
				}()
			}
			for i := 0; i < 2; i++ {
				select {
				case <-creating:
				case err := <-done:
					t.Fatalf("did not reach both CREATE barriers: %+v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			close(release)
			seen := map[int64]bool{}
			for i := 0; i < 2; i++ {
				select {
				case result := <-done:
					if result.err != nil || result.id == 0 || seen[result.id] {
						t.Fatalf("first-use reservation: %+v", result)
					}
					seen[result.id] = true
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}

func TestReservationSQLiteAllocatorWriteFailure(t *testing.T) {
	for _, decision := range []int{sqlite3.SQLITE_DENY, sqlite3.SQLITE_IGNORE} {
		t.Run(fmt.Sprint(decision), func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY)")
			h.DB.SetMaxOpenConns(1)
			service, _ := insert.New(ctx, h.DB, "records")
			if _, err := service.NextSequence(ctx, &reservationRow{}, 1, dialect.PresetIDWithReservation); err != nil {
				t.Fatal(err)
			}
			conn, err := h.DB.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = conn.Raw(func(raw any) error {
				raw.(*sqlite3.SQLiteConn).RegisterAuthorizer(func(op int, table, column, schema string) int {
					if op == sqlite3.SQLITE_UPDATE && table == "sqlx_sequence_reservations" {
						return decision
					}
					return sqlite3.SQLITE_OK
				})
				return nil
			})
			conn.Close()
			if err != nil {
				t.Fatal(err)
			}
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			result, err := service.NextSequence(ctx, &reservationRow{}, 1, tx, dialect.PresetIDWithReservation)
			if result != nil || err == nil {
				t.Fatalf("failed counter write published range: %+v %v", result, err)
			}
			var one int
			if err = tx.QueryRow("SELECT 1").Scan(&one); err != nil {
				t.Fatal("caller transaction completed:", err)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
