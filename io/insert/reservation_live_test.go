package insert_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/option"
	"github.com/viant/sqlx/testutil/reservationdb"
)

type liveReservationRow struct {
	ID   int64  `sqlx:"id,primaryKey,autoincrement"`
	Name string `sqlx:"name"`
}

func TestReservationLivePositive(t *testing.T) {
	for _, driver := range []string{"mysql", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			h := reservationdb.Open(t, driver)
			h.CreateRecords(t)
			h.DB.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			service, err := explicitReservationInserter(ctx, h.DB, h.Table("records"))
			if err != nil {
				t.Fatal(err)
			}
			metadata, err := service.SequenceInfo(ctx, &liveReservationRow{}, tx, dialect.PresetIDWithReservation)
			if err != nil {
				t.Fatal(err)
			}
			rows := []*liveReservationRow{{Name: "one"}, {Name: "two"}, {Name: "three"}}
			reserved, err := service.ReserveSequence(ctx, rows, 3, tx)
			if err != nil {
				t.Fatal(err)
			}
			if reserved.Sequence.Name != metadata.Name || len(reserved.Values) != 3 {
				t.Fatalf("identity/cardinality: %+v %+v", metadata, reserved)
			}
			for i, row := range rows {
				if row.ID != 0 {
					t.Fatal("reservation modified input")
				}
				row.ID = reserved.Values[i]
			}
			var before int
			if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM "+h.Table("records")).Scan(&before); err != nil || before != 0 {
				t.Fatalf("allocation inserted business rows: %d %v", before, err)
			}
			affected, _, err := service.Exec(ctx, rows, tx, option.BatchSize(3))
			if err != nil || affected != 3 {
				t.Fatalf("insert of reserved IDs: affected=%d err=%v", affected, err)
			}
			for i, row := range rows {
				if row.ID != reserved.Values[i] {
					t.Fatal("insert changed a stable ID")
				}
			}
			if err = tx.Rollback(); err != nil {
				t.Fatalf("caller transaction was completed: %v", err)
			}
			if h.Count(t, "records") != 0 {
				t.Fatal("caller rollback failed")
			}
			later, err := service.ReserveSequence(ctx, &liveReservationRow{}, 1)
			if err != nil {
				t.Fatal(err)
			}
			if driver == "postgres" {
				for _, id := range reserved.Values {
					if later.Values[0] == id {
						t.Fatal("PostgreSQL rolled-back sequence value reclaimed")
					}
				}
			}
		})
	}
}

// Exercise native Exec's value-assignment owner as well as reservation-only API.
func TestReservationLiveNativeAssignment(t *testing.T) {
	for _, driver := range []string{"mysql", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			h := reservationdb.Open(t, driver)
			h.CreateRecords(t)
			h.DB.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			service, _ := explicitReservationInserter(ctx, h.DB, h.Table("records"))
			rows := []*liveReservationRow{{Name: "first"}, {Name: "second"}}
			affected, _, err := service.Exec(ctx, rows, tx, option.BatchSize(2), dialect.PresetIDWithReservation)
			if err != nil || affected != 2 || rows[0].ID == 0 || rows[0].ID == rows[1].ID {
				t.Fatalf("native assignment: %+v %d %v", rows, affected, err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if h.Count(t, "records") != 2 {
				t.Fatal("insert not persisted")
			}
		})
	}
}

func TestReservationLiveMySQLCallerWriteAndSettings(t *testing.T) {
	h := reservationdb.Open(t, "mysql")
	h.CreateRecords(t)
	h.DB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, h.SeedIDSQL(40)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "SET SESSION auto_increment_increment=3,auto_increment_offset=2"); err != nil {
		t.Fatal(err)
	}
	service, _ := explicitReservationInserter(ctx, h.DB, h.Table("records"))
	result, err := service.ReserveSequence(ctx, &liveReservationRow{}, 3, tx)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range result.Values {
		if id <= 40 || (id-2)%3 != 0 || i > 0 && id-result.Values[i-1] != 3 {
			t.Fatalf("native increment/offset: %+v", result)
		}
	}
	var step, offset int
	if err = tx.QueryRowContext(ctx, "SELECT @@SESSION.auto_increment_increment,@@SESSION.auto_increment_offset").Scan(&step, &offset); err != nil || step != 3 || offset != 2 {
		t.Fatal("allocator changed session settings")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if h.Count(t, "records") != 0 {
		t.Fatal("allocator committed caller writes")
	}
}

// CACHE 3 makes a deterministic non-contiguous batch: backend A owns 1..3,
// backend B owns 4..6; A's next batch is exactly 2,3,7 (or -2,-3,-7).
func TestReservationLivePostgresExactCachedValues(t *testing.T) {
	for _, descending := range []bool{false, true} {
		t.Run(fmt.Sprint(descending), func(t *testing.T) {
			h := reservationdb.Open(t, "postgres")
			seq := h.Table("numbers")
			definition := " START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 1000"
			if descending {
				definition = " START -1 INCREMENT -1 MINVALUE -1000 MAXVALUE -1"
			}
			h.Exec(t, "CREATE SEQUENCE "+seq+definition+" CACHE 3", "CREATE TABLE "+h.Table("records")+"(id bigint DEFAULT nextval('"+seq+"'::regclass),name text NOT NULL)")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			a, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Rollback()
			b, err := h.Other(t).BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Rollback()
			for _, tx := range []*sql.Tx{a, b} {
				var id int64
				if err = tx.QueryRowContext(ctx, "SELECT nextval($1::regclass)", seq).Scan(&id); err != nil {
					t.Fatal(err)
				}
			}
			service, _ := explicitReservationInserter(ctx, h.DB, h.Table("records"))
			result, err := service.ReserveSequence(ctx, &liveReservationRow{}, 3, a)
			if err != nil {
				t.Fatal(err)
			}
			want := []int64{2, 3, 7}
			if descending {
				want = []int64{-2, -3, -7}
			}
			if !reflect.DeepEqual(want, result.Values) {
				t.Fatalf("fabricated range: got %v want %v", result.Values, want)
			}
			if _, err = service.NextSequence(ctx, &liveReservationRow{}, 1, a, dialect.PresetIDWithReservation); err == nil || !strings.Contains(err.Error(), "ReserveSequence") {
				t.Fatalf("old range API must diagnose exact-value requirement: %v", err)
			}
			var one int
			if err = a.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
				t.Fatal("range diagnostic aborted caller tx:", err)
			}
		})
	}
}

func TestReservationLiveShapeDiagnostics(t *testing.T) {
	for _, driver := range []string{"mysql", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			h := reservationdb.Open(t, driver)
			h.Exec(t, "CREATE TABLE "+h.Table("plain")+"(id BIGINT PRIMARY KEY)", "CREATE TABLE "+h.Table("textual")+"(id VARCHAR(30) PRIMARY KEY)")
			for _, name := range []string{"plain", "textual"} {
				s, _ := explicitReservationInserter(context.Background(), h.DB, h.Table(name))
				r, err := s.ReserveSequence(context.Background(), &liveReservationRow{}, 1)
				if err == nil || r != nil {
					t.Fatalf("accepted nongenerated shape %s: %+v %v", name, r, err)
				}
				if strings.Contains(err.Error(), "unsupported for MySQL") || strings.Contains(err.Error(), "unsupported for PostgreSQL") {
					t.Fatal("disabled whole product")
				}
				t.Log(name, err)
			}
			if driver == "postgres" {
				h.Exec(t, "CREATE SEQUENCE "+h.Table("cycling")+" CYCLE MAXVALUE 3", "CREATE TABLE "+h.Table("cycle_rows")+"(id BIGINT DEFAULT nextval('"+h.Table("cycling")+"'::regclass))")
				s, _ := explicitReservationInserter(context.Background(), h.DB, h.Table("cycle_rows"))
				r, err := s.ReserveSequence(context.Background(), &liveReservationRow{}, 1)
				if r != nil || err == nil || !strings.Contains(err.Error(), "cycles") {
					t.Fatalf("cycling sequence diagnostic: %+v %v", r, err)
				}
			}
		})
	}
}

func TestReservationLiveCancellation(t *testing.T) {
	for _, driver := range []string{"mysql", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			h := reservationdb.Open(t, driver)
			h.CreateRecords(t)
			h.DB.SetMaxOpenConns(1)
			tx, err := h.DB.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			s, _ := explicitReservationInserter(context.Background(), h.DB, h.Table("records"))
			r, err := s.ReserveSequence(ctx, &liveReservationRow{}, 2, tx)
			if r != nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled allocation: %+v %v", r, err)
			}
			var one int
			if err = tx.QueryRow("SELECT 1").Scan(&one); err != nil {
				t.Fatal("pre-cancellation completed caller transaction:", err)
			}
		})
	}
}

func TestReservationLivePostgresAuthorities(t *testing.T) {
	for _, kind := range []string{"serial", "identity always", "explicit"} {
		t.Run(kind, func(t *testing.T) {
			h := reservationdb.Open(t, "postgres")
			definition := "id BIGSERIAL PRIMARY KEY,name TEXT"
			if kind == "identity always" {
				definition = "id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,name TEXT"
			}
			if kind == "explicit" {
				definition = "id BIGINT PRIMARY KEY,name TEXT"
				h.Exec(t, "CREATE SEQUENCE "+h.Table("numbers"))
			}
			h.Exec(t, "CREATE TABLE "+h.Table("records")+"("+definition+")")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.ExecContext(ctx, "SET LOCAL search_path TO "+h.Quote(h.Schema)); err != nil {
				t.Fatal(err)
			}
			type named struct {
				ID int64 `sqlx:"id,primaryKey,sequence=numbers"`
			}
			var record any = &liveReservationRow{}
			if kind == "explicit" {
				record = &named{}
			}
			service, _ := explicitReservationInserter(ctx, h.DB, h.Table("records"))
			result, err := service.ReserveSequence(ctx, record, 2, tx)
			if err != nil {
				t.Fatal(err)
			}
			if result.Sequence.Schema != h.Schema || len(result.Values) != 2 {
				t.Fatalf("wrong native authority: %+v", result)
			}
			override := ""
			if kind == "identity always" {
				override = " OVERRIDING SYSTEM VALUE"
			}
			for _, id := range result.Values {
				if _, err = tx.ExecContext(ctx, "INSERT INTO "+h.Table("records")+"(id,name)"+override+" VALUES($1,'actual')", id); err != nil {
					t.Fatal(err)
				}
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReservationLiveNoSourceSideEffects(t *testing.T) {
	for _, driver := range []string{"mysql", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			h := reservationdb.Open(t, driver)
			h.CreateRecords(t)
			h.Exec(t, "CREATE TABLE "+h.Table("audit")+"(n INT)")
			if driver == "mysql" {
				h.Exec(t, "CREATE TRIGGER "+h.Table("observe_insert")+" BEFORE INSERT ON "+h.Table("records")+" FOR EACH ROW INSERT INTO "+h.Table("audit")+" VALUES(1)")
			} else {
				h.Exec(t, "CREATE FUNCTION "+h.Table("observe_insert")+"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO "+h.Table("audit")+" VALUES(1); RETURN NEW; END $$", "CREATE TRIGGER observe_insert BEFORE INSERT ON "+h.Table("records")+" FOR EACH ROW EXECUTE FUNCTION "+h.Table("observe_insert")+"()")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			h.DB.SetMaxOpenConns(1)
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var before, after string
			var tableName string
			if driver == "mysql" {
				if err = tx.QueryRowContext(ctx, "SHOW CREATE TABLE "+h.Table("records")).Scan(&tableName, &before); err != nil {
					t.Fatal(err)
				}
			}
			service, _ := explicitReservationInserter(ctx, h.DB, h.Table("records"))
			if _, err = service.ReserveSequence(ctx, &liveReservationRow{}, 4, tx); err != nil {
				t.Fatal(err)
			}
			if driver == "mysql" {
				if err = tx.QueryRowContext(ctx, "SHOW CREATE TABLE "+h.Table("records")).Scan(&tableName, &after); err != nil || before != after {
					t.Fatalf("source schema/AUTO_INCREMENT changed: %v", err)
				}
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if h.Count(t, "records") != 0 || h.Count(t, "audit") != 0 {
				t.Fatal("reservation inserted an entity or executed its trigger")
			}
			h.Exec(t, "INSERT INTO "+h.Table("records")+"(name) VALUES('trigger-control')")
			if h.Count(t, "audit") != 1 {
				t.Fatal("allocator disabled the application trigger")
			}
		})
	}
}

func TestReservationLiveBlockedCancellation(t *testing.T) {
	for _, driver := range []string{"mysql", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			h := reservationdb.Open(t, driver)
			h.CreateRecords(t)
			h.Exec(t, "CREATE TABLE "+h.Table("caller_work")+"(n INT)")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			blocker, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback()
			service, _ := explicitReservationInserter(ctx, h.DB, h.Table("records"))
			if driver == "mysql" {
				if _, err = service.ReserveSequence(ctx, &liveReservationRow{}, 1, blocker); err != nil {
					t.Fatal(err)
				}
			} else {
				var seq string
				if err = blocker.QueryRowContext(ctx, "SELECT pg_get_serial_sequence($1,'id')", h.Table("records")).Scan(&seq); err != nil {
					t.Fatal(err)
				}
				if _, err = blocker.ExecContext(ctx, "ALTER SEQUENCE "+seq+" RESTART WITH 100"); err != nil {
					t.Fatal(err)
				}
			}
			callerDB := h.Other(t)
			callerDB.SetMaxOpenConns(1)
			tx, err := callerDB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.ExecContext(ctx, "INSERT INTO "+h.Table("caller_work")+" VALUES(1)"); err != nil {
				t.Fatal(err)
			}
			callCtx, stop := context.WithTimeout(ctx, 150*time.Millisecond)
			defer stop()
			caller, _ := explicitReservationInserter(ctx, callerDB, h.Table("records"))
			row := &liveReservationRow{}
			result, err := caller.ReserveSequence(callCtx, row, 2, tx)
			if result != nil || err == nil || row.ID != 0 || callCtx.Err() == nil {
				t.Fatalf("blocked cancellation returned IDs: %+v %v", result, err)
			}
			// Drivers/server cancellation may abort their transaction. Native allocation
			// must never commit caller work; only this owner invokes Rollback.
			_ = tx.Rollback()
			if h.Count(t, "caller_work") != 0 {
				t.Fatal("cancelled allocator committed caller work")
			}
		})
	}
}

func TestReservationLiveExplicitAssignmentOwner(t *testing.T) {
	for _, driver := range []string{"mysql", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			h := reservationdb.Open(t, driver)
			if driver == "postgres" {
				h.Exec(t, "CREATE TABLE "+h.Table("records")+"(id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,name TEXT NOT NULL)")
			} else {
				h.CreateRecords(t)
			}
			h.DB.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			service, _ := explicitReservationInserter(ctx, h.DB, h.Table("records"))
			rows := []*liveReservationRow{{Name: "one"}, {Name: "two"}}
			// Explicit strategy configured on the service, no transaction: the insert transaction must
			// own allocation, without trying to borrow a second connection.
			count, _, err := service.Exec(ctx, rows, option.BatchSize(2))
			if err != nil || count != 2 || rows[0].ID == 0 || rows[0].ID == rows[1].ID {
				t.Fatalf("default assignment owner: %d %+v %v", count, rows, err)
			}
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			bound, _ := explicitReservationInserter(ctx, h.DB, h.Table("records"), tx)
			result, err := bound.ReserveSequence(ctx, &liveReservationRow{}, 1)
			if err != nil {
				t.Fatal("constructor transaction lost:", err)
			}
			row := &liveReservationRow{ID: result.Values[0], Name: "preassigned"}
			if _, _, err = bound.Exec(ctx, row); err != nil {
				t.Fatal(err)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal("constructor caller transaction completed:", err)
			}
			if h.Count(t, "records") != 2 {
				t.Fatal("native insertion committed caller transaction")
			}
		})
	}
}

func TestReservationLiveMySQLCurrentRead(t *testing.T) {
	h := reservationdb.Open(t, "mysql")
	h.CreateRecords(t)
	h.Exec(t, h.SeedIDSQL(1))
	h.DB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := h.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM "+h.Table("records")).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if _, err = h.Other(t).ExecContext(ctx, h.SeedIDSQL(50)); err != nil {
		t.Fatal(err)
	}
	s, _ := explicitReservationInserter(ctx, h.DB, h.Table("records"))
	r, err := s.ReserveSequence(ctx, &liveReservationRow{}, 1, tx)
	if err != nil || r.Values[0] <= 50 {
		t.Fatalf("used stale snapshot maximum: %+v %v", r, err)
	}
}

func TestReservationLivePostgresDefaultAndColumnLimits(t *testing.T) {
	h := reservationdb.Open(t, "postgres")
	h.Exec(t, "CREATE SEQUENCE "+h.Table("numbers")+" START 32768",
		"CREATE TABLE "+h.Table("narrow")+"(id SMALLINT DEFAULT nextval('"+h.Table("numbers")+"'::regclass))",
		"CREATE TABLE "+h.Table("transformed")+"(id BIGINT DEFAULT (nextval('"+h.Table("numbers")+"'::regclass)*10))")
	for _, table := range []string{"narrow", "transformed"} {
		s, _ := explicitReservationInserter(context.Background(), h.DB, h.Table(table))
		r, err := s.ReserveSequence(context.Background(), &liveReservationRow{}, 1)
		if r != nil || err == nil {
			t.Fatalf("accepted unsupported column/default shape: %+v %v", r, err)
		}
		want := "mapped column range"
		if table == "transformed" {
			want = "not a direct nextval"
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("imprecise diagnostic: %v", err)
		}
	}
}

func TestReservationLiveMySQLExplicitScalar(t *testing.T) {
	h := reservationdb.Open(t, "mysql")
	h.CreateRecords(t)
	s, _ := explicitReservationInserter(context.Background(), h.DB, h.Table("records"))
	result, err := s.NextSequence(context.Background(), &liveReservationRow{}, 2)
	if err != nil || result == nil || result.Name == "" || result.MinValue(2) != 1 {
		t.Fatalf("default scalar API did not use a real native allocator: %+v %v", result, err)
	}
	if h.Count(t, "records") != 0 {
		t.Fatal("scalar allocation inserted business rows")
	}
}

func TestReservationLiveMySQLUncachedAutoStart(t *testing.T) {
	h := reservationdb.Open(t, "mysql")
	h.CreateRecords(t)
	// Populate cached INFORMATION_SCHEMA statistics before changing the source's
	// configured next value. Allocation must read current engine metadata.
	var cached sql.NullInt64
	if err := h.DB.QueryRow("SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME='records'", h.Schema).Scan(&cached); err != nil {
		t.Fatal(err)
	}
	h.Exec(t, "ALTER TABLE "+h.Table("records")+" AUTO_INCREMENT=1000")
	s, _ := explicitReservationInserter(context.Background(), h.DB, h.Table("records"))
	r, err := s.ReserveSequence(context.Background(), &liveReservationRow{}, 1)
	if err != nil || r.Values[0] != 1000 {
		t.Fatalf("lost configured auto-increment start: %+v %v", r, err)
	}
}

func explicitReservationInserter(ctx context.Context, db *sql.DB, table string, options ...option.Option) (*insert.Service, error) {
	return insert.New(ctx, db, table, append(options, dialect.PresetIDWithReservation)...)
}
