package insert_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/option"
	"github.com/viant/sqlx/testutil/reservationdb"
)

func TestMySQLOriginalDefaultRegistration(t *testing.T) {
	if got := registry.LookupDialect(mysql.MySQL5()).DefaultPresetIDStrategy; got != dialect.PresetIDWithTransientTransaction {
		t.Fatalf("default=%s", got)
	}
}
func assertNoAllocatorTable(t testing.TB, db *sql.DB) {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT count(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='sqlx_allocator' AND TABLE_NAME='sequence_reservations'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("default test requires a fresh server with no allocator table: count=%d err=%v", count, err)
	}
}
func TestMySQLOriginalLiveNoAllocatorTable(t *testing.T) {
	h := reservationdb.OpenTransient(t)
	h.CreateRecords(t)
	h.DB.SetMaxOpenConns(1)
	assertNoAllocatorTable(t, h.DB)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	svc, _ := insert.New(ctx, h.DB, h.Table("records"))
	original, err := svc.NextSequence(ctx, &liveReservationRow{Name: "original"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	adapted, err := svc.ReserveSequence(ctx, &liveReservationRow{Name: "adapter"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if original.MinValue(1) != 1 || !reflect.DeepEqual(adapted.Values, []int64{2, 3}) {
		t.Fatalf("original/adapted: %+v %+v", original, adapted)
	}
	if h.Count(t, "records") != 0 {
		t.Fatal("transient rows were not rolled back")
	}
	result, err := h.DB.ExecContext(ctx, "INSERT INTO "+h.Table("records")+"(name) VALUES('real')")
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil || id != 4 {
		t.Fatalf("original source AUTO_INCREMENT was not advanced: %d %v", id, err)
	}
	assertNoAllocatorTable(t, h.DB)
}
func TestMySQLOriginalLiveCallerOwnership(t *testing.T) {
	h := reservationdb.OpenTransient(t)
	h.CreateRecords(t)
	h.DB.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SET SESSION foreign_key_checks=0"); err != nil {
		t.Fatal(err)
	}
	svc, _ := insert.New(ctx, h.DB, h.Table("records"))
	r, err := svc.ReserveSequence(ctx, &liveReservationRow{Name: "caller"}, 2, tx)
	if err != nil {
		t.Fatal(err)
	}
	var fk, n int
	if err = tx.QueryRowContext(ctx, "SELECT @@SESSION.foreign_key_checks").Scan(&fk); err != nil || fk != 0 {
		t.Fatalf("caller session changed: %d %v", fk, err)
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM "+h.Table("records")).Scan(&n); err != nil || n != 0 {
		t.Fatalf("caller tx changed: %d %v", n, err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO "+h.Table("records")+"(id,name) VALUES(?,?)", r.Values[0], "queued"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal("caller tx was completed", err)
	}
	if h.Count(t, "records") != 0 {
		t.Fatal("caller insert committed")
	}
	// Native AUTO_INCREMENT advancement survives caller rollback.
	next, err := svc.ReserveSequence(ctx, &liveReservationRow{Name: "after rollback"}, 1)
	if err != nil || next.Values[0] <= r.Values[1] {
		t.Fatalf("counter rollback semantics: %+v %v", next, err)
	}
}
func TestMySQLOriginalLiveConnectionLimit(t *testing.T) {
	h := reservationdb.OpenTransient(t)
	h.CreateRecords(t)
	h.DB.SetMaxOpenConns(1)
	tx, err := h.DB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	svc, _ := insert.New(ctx, h.DB, h.Table("records"))
	r, err := svc.ReserveSequence(ctx, &liveReservationRow{Name: "blocked"}, 1, tx)
	if r != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("original separate-connection limit: %+v %v", r, err)
	}
	if rollbackErr := tx.Rollback(); rollbackErr != nil {
		t.Logf("go-sql-driver invalidated the caller transaction after cancellation: %v", rollbackErr)
	}
	if h.Count(t, "records") != 0 {
		t.Fatal("cancelled allocator committed source rows")
	}
	assertNoAllocatorTable(t, h.DB)
	t.Log("observed original allocator connection requirement; cancellation may invalidate the caller transaction, and no fallback was substituted")
}
func TestMySQLOriginalLiveCallerLockLimit(t *testing.T) {
	h := reservationdb.OpenTransient(t)
	h.Exec(t, "CREATE TABLE "+h.Table("records")+"(id BIGINT AUTO_INCREMENT PRIMARY KEY,name VARCHAR(64) NOT NULL UNIQUE) ENGINE=InnoDB")
	h.DB.SetMaxOpenConns(2)
	tx, err := h.DB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("INSERT INTO " + h.Table("records") + "(name) VALUES('locked')"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	svc, _ := insert.New(ctx, h.DB, h.Table("records"))
	r, err := svc.ReserveSequence(ctx, &liveReservationRow{Name: "locked"}, 1, tx)
	if r != nil || !errors.Is(err, context.DeadlineExceeded) || ctx.Err() == nil {
		t.Fatalf("original caller-lock dependency: %+v %v", r, err)
	}
	if rollbackErr := tx.Rollback(); rollbackErr != nil {
		t.Logf("go-sql-driver invalidated the caller transaction after cancellation: %v", rollbackErr)
	}
	if h.Count(t, "records") != 0 {
		t.Fatal("cancelled allocator committed caller source rows")
	}
	assertNoAllocatorTable(t, h.DB)
	t.Log("observed original allocator waiting on caller-held source locks; cancellation may invalidate the caller transaction")
}
func TestMySQLOriginalLiveSourceEffects(t *testing.T) {
	h := reservationdb.OpenTransient(t)
	h.CreateRecords(t)
	h.DB.SetMaxOpenConns(1)
	h.Exec(t, "SET @f1_original_trigger=0", "CREATE TRIGGER "+h.Table("observe_original")+" BEFORE INSERT ON "+h.Table("records")+" FOR EACH ROW SET @f1_original_trigger=COALESCE(@f1_original_trigger,0)+1")
	svc, _ := insert.New(context.Background(), h.DB, h.Table("records"))
	_, err := svc.ReserveSequence(context.Background(), &liveReservationRow{Name: "trigger"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	if err = h.DB.QueryRow("SELECT @f1_original_trigger").Scan(&calls); err != nil || calls != 2 {
		t.Fatalf("original source INSERT effects not observed: %d %v", calls, err)
	}
	if h.Count(t, "records") != 0 {
		t.Fatal("transient source rows committed")
	}
	t.Log("original triggers execute despite row rollback; nontransactional effects are not undone")
}
func TestMySQLOriginalLiveConcurrentAllocations(t *testing.T) {
	h := reservationdb.OpenTransient(t)
	h.CreateRecords(t)
	assertNoAllocatorTable(t, h.DB)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	done := make(chan struct {
		values []int64
		err    error
	}, 2)
	for i := 0; i < 2; i++ {
		db := h.Other(t)
		db.SetMaxOpenConns(2)
		go func(i int) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				done <- struct {
					values []int64
					err    error
				}{err: err}
				return
			}
			defer tx.Rollback()
			ready <- struct{}{}
			<-start
			svc, _ := insert.New(ctx, db, h.Table("records"))
			r, err := svc.ReserveSequence(ctx, &liveReservationRow{Name: fmt.Sprint(i)}, 3, tx)
			var values []int64
			if r != nil {
				values = r.Values
			}
			done <- struct {
				values []int64
				err    error
			}{values, err}
		}(i)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(start)
	seen := map[int64]bool{}
	for i := 0; i < 2; i++ {
		select {
		case r := <-done:
			if r.err != nil {
				t.Fatal(r.err)
			}
			for _, id := range r.values {
				if seen[id] {
					t.Fatal("overlapping original native ranges", id)
				}
				seen[id] = true
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if len(seen) != 6 || h.Count(t, "records") != 0 {
		t.Fatal("invalid transient ranges or committed placeholders")
	}
	assertNoAllocatorTable(t, h.DB)
}
func TestMySQLOptionalLiveReservation(t *testing.T) {
	h := reservationdb.Open(t, "mysql")
	h.CreateRecords(t)
	h.DB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	svc, _ := insert.New(ctx, h.DB, h.Table("records"))
	r, err := svc.ReserveSequence(ctx, &liveReservationRow{Name: "explicit"}, 2, tx, dialect.PresetIDWithReservation)
	if err != nil || len(r.Values) != 2 {
		t.Fatalf("explicit reservation: %+v %v", r, err)
	}
	if _, _, err = svc.Exec(ctx, []*liveReservationRow{{ID: r.Values[0], Name: "one"}, {ID: r.Values[1], Name: "two"}}, tx, option.BatchSize(2), dialect.PresetIDWithReservation); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if h.Count(t, "records") != 0 {
		t.Fatal("optional owner completed caller tx")
	}
}
