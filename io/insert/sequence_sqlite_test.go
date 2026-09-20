package insert_test

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/option"
)

type sequenceRow struct {
	ID int64 `sqlx:"id,primaryKey,autoincrement,sequence=authored_sequence"`
}

func TestSequenceInfoReadOnlyCanonicalSQLiteIdentity(t *testing.T) {
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	h.Exec(t,
		"CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)",
		"CREATE TEMP TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)",
		"ATTACH ':memory:' AS aux", "CREATE TABLE aux.records(id INTEGER PRIMARY KEY AUTOINCREMENT)",
		`CREATE TABLE "odd.name"(id INTEGER PRIMARY KEY AUTOINCREMENT)`,
		`CREATE TABLE "odd""quote"(id INTEGER PRIMARY KEY AUTOINCREMENT)`,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tc := range []struct{ table, schema, name string }{
		{"records", "temp", "records"}, {"RECORDS", "temp", "records"},
		{`"MAIN"."RECORDS"`, "main", "records"}, {"[main].[records]", "main", "records"},
		{"`main`.`records`", "main", "records"}, {"'records'", "temp", "records"},
		{`"aux"."records"`, "aux", "records"}, {`"odd.name"`, "main", "odd.name"},
		{`"odd""quote"`, "main", `odd"quote`},
	} {
		t.Run(tc.table, func(t *testing.T) {
			s, err := insert.New(ctx, h.DB, tc.table)
			if err != nil {
				t.Fatal(err)
			}
			row := &sequenceRow{ID: 6}
			identity, err := s.SequenceInfo(ctx, []*sequenceRow{row})
			if err != nil || identity.Name != tc.name || identity.Schema != tc.schema || identity.StartValue != 1 || identity.IncrementBy != 1 || row.ID != 6 {
				t.Fatalf("identity=%+v row=%+v err=%v", identity, row, err)
			}
		})
	}
	for _, table := range []string{"absent", "missing.records", "main.absent", "a.b.c"} {
		s, err := insert.New(ctx, h.DB, table)
		if err != nil {
			t.Fatal(err)
		}
		if identity, err := s.SequenceInfo(ctx, &sequenceRow{}); err == nil || identity != nil {
			t.Fatalf("invented authority for %s: %+v %v", table, identity, err)
		}
	}
	for _, schema := range []string{"main", "temp", "aux"} {
		sqlite.AssertRows(t, h, "SELECT COUNT(*) AS n FROM "+schema+".sqlite_sequence", []struct{ N int }{{0}})
	}
	sqlite.AssertRows(t, h, "SELECT total_changes() AS n", []struct{ N int }{{0}})
	h.Exec(t, "DROP TABLE temp.records", "DROP TABLE main.records")
	s, _ := insert.New(ctx, h.DB, "records")
	identity, err := s.SequenceInfo(ctx, &sequenceRow{})
	if err != nil || identity.Schema != "aux" {
		t.Fatalf("attachment search order: %+v %v", identity, err)
	}
}

func TestTransientRangesPersistAcrossIndependentServices(t *testing.T) {
	for _, initial := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty counter", true: "existing counter"}[initial], func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)")
			h.DB.SetMaxOpenConns(1)
			start, rows := int64(1), 0
			if initial {
				h.Exec(t, "INSERT INTO records VALUES(5)")
				start, rows = 6, 1
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for index, table := range []string{"records", "RECORDS", `"MAIN"."records"`, "[records]"} {
				s, err := insert.New(ctx, h.DB, table)
				if err != nil {
					t.Fatal(err)
				}
				rangeInfo, err := s.NextSequence(ctx, &sequenceRow{}, 2, dialect.PresetIDWithTransientTransaction)
				want := start + int64(index)*2
				if err != nil || rangeInfo.MinValue(2) != want || rangeInfo.Value != want+2 || rangeInfo.Name != "records" || rangeInfo.Schema != "main" {
					t.Fatalf("%s range=%+v err=%v want=%d", table, rangeInfo, err, want)
				}
				sqlite.AssertRows(t, h, "SELECT seq FROM sqlite_sequence WHERE name='records'", []struct{ Seq int64 }{{want + 1}})
			}
			sqlite.AssertRows(t, h, "SELECT COUNT(*) AS n FROM records", []struct{ N int }{{rows}})
		})
	}
}

func TestConcurrentNativeTransientReservations(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO records VALUES(5)")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const count = 12
	ids, failures := make([]int64, count), make([]error, count)
	var group sync.WaitGroup
	for index := range ids {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			s, err := insert.New(ctx, h.DB, "records")
			if err != nil {
				failures[index] = err
				return
			}
			rangeInfo, err := s.NextSequence(ctx, &sequenceRow{}, 1, dialect.PresetIDWithTransientTransaction)
			failures[index] = err
			if err == nil {
				ids[index] = rangeInfo.MinValue(1)
			}
		}(index)
	}
	group.Wait()
	for _, err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i, id := range ids {
		if id != int64(i+6) {
			t.Fatalf("overlapping reservations: %v", ids)
		}
	}
	sqlite.AssertRows(t, h, "SELECT seq FROM sqlite_sequence WHERE name='records'", []struct{ Seq int }{{5 + count}})
	sqlite.AssertRows(t, h, "SELECT COUNT(*) AS n FROM records", []struct{ N int }{{1}})
}

func TestSequenceInfoAndReservationsRespectCallerTransaction(t *testing.T) {
	for _, strategy := range []dialect.PresetIDStrategy{dialect.PresetIDWithMax, dialect.PresetIDWithTransientTransaction} {
		t.Run(string(strategy), func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO records VALUES(5)")
			h.DB.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			s, _ := insert.New(ctx, h.DB, "RECORDS")
			identity, err := s.SequenceInfo(ctx, &sequenceRow{ID: 6}, tx)
			if err != nil || identity.Name != "records" {
				t.Fatalf("identity=%+v err=%v", identity, err)
			}
			result, err := s.NextSequence(ctx, &sequenceRow{}, 2, strategy, tx)
			if err != nil || result.MinValue(2) != 6 {
				t.Fatalf("range=%+v err=%v", result, err)
			}
			var value int64
			if err := tx.QueryRowContext(ctx, "SELECT seq FROM sqlite_sequence WHERE name='records'").Scan(&value); err != nil {
				t.Fatal(err)
			}
			want := int64(5)
			if strategy == dialect.PresetIDWithTransientTransaction {
				want = 7
			}
			if value != want {
				t.Fatalf("physical sequence=%d want=%d", value, want)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal("caller transaction completed by allocator:", err)
			}
			sqlite.AssertRows(t, h, "SELECT seq FROM sqlite_sequence WHERE name='records'", []struct{ Seq int }{{5}})
		})
	}
}

func TestNativeReservationFailuresDoNotPublishRanges(t *testing.T) {
	for _, mode := range []string{"cancel", "overflow", "duplicate counter", "write denied", "write ignored", "commit denied", "invalid count"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO records VALUES(5)")
			h.DB.SetMaxOpenConns(1)
			want, rowCount := int64(5), 1
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			count := 2
			switch mode {
			case "cancel":
				cancel()
			case "overflow":
				want = math.MaxInt64 - 1
				h.Exec(t, "UPDATE sqlite_sequence SET seq=9223372036854775806 WHERE name='records'")
			case "duplicate counter":
				rowCount = 2
				h.Exec(t, "INSERT INTO sqlite_sequence VALUES('records',5)")
			case "invalid count":
				count = 0
			case "write denied", "write ignored", "commit denied":
				conn, err := h.DB.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				err = conn.Raw(func(raw interface{}) error {
					raw.(*sqlite3.SQLiteConn).RegisterAuthorizer(func(op int, arg1, arg2, schema string) int {
						if mode == "write ignored" && op == sqlite3.SQLITE_UPDATE && arg1 == "sqlite_sequence" {
							return sqlite3.SQLITE_IGNORE
						}
						if mode == "commit denied" && op == sqlite3.SQLITE_TRANSACTION && arg1 == "COMMIT" || mode == "write denied" && op == sqlite3.SQLITE_UPDATE && arg1 == "sqlite_sequence" {
							return sqlite3.SQLITE_DENY
						}
						return sqlite3.SQLITE_OK
					})
					return nil
				})
				conn.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			s, _ := insert.New(ctx, h.DB, "records")
			result, err := s.NextSequence(ctx, &sequenceRow{}, count, dialect.PresetIDWithTransientTransaction)
			if err == nil || result != nil {
				t.Fatalf("failed reservation published %+v: %v", result, err)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if mode == "commit denied" && !strings.Contains(err.Error(), "commit SQLite sequence reservation") {
				t.Fatal("lost commit failure:", err)
			}
			sqlite.AssertRows(t, h, "SELECT seq FROM sqlite_sequence WHERE name='records' LIMIT 1", []struct{ Seq int64 }{{want}})
			sqlite.AssertRows(t, h, "SELECT COUNT(*) AS n FROM sqlite_sequence WHERE name='records'", []struct{ N int }{{rowCount}})
		})
	}
}

func TestSequenceInfoCannotBeRedirectedByTableHint(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE other(id INTEGER PRIMARY KEY)")
	s, _ := insert.New(context.Background(), h.DB, "records", option.SequenceTable("other"))
	identity, err := s.SequenceInfo(context.Background(), &sequenceRow{}, option.SequenceTable("absent"))
	if err != nil || identity.Name != "records" {
		t.Fatalf("physical authority overridden: %+v %v", identity, err)
	}
}
