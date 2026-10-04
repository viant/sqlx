package sequence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
)

func fixture(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "scoped.db") + "?_busy_timeout=10000"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err = db.Exec("CREATE TABLE messages(id TEXT PRIMARY KEY,turn_id TEXT,sequence INTEGER,UNIQUE(turn_id,sequence));INSERT INTO messages VALUES('a','t1',2),('b','t2',100)"); err != nil {
		t.Fatal(err)
	}
	if err = Provision(context.Background(), db, "sqlite"); err != nil {
		t.Fatal(err)
	}
	return db, dsn
}

func TestScopedSequencePreservesContentionAfterCreateFallback(t *testing.T) {
	db, dsn := fixture(t)
	other, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ctx := context.Background()
	reader, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	var count int
	if err = reader.QueryRowContext(ctx, "SELECT COUNT(*) FROM messages").Scan(&count); err != nil {
		t.Fatal(err)
	}
	writer, err := other.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	if _, err = writer.ExecContext(ctx, "UPDATE messages SET sequence=sequence WHERE id='a'"); err != nil {
		t.Fatal(err)
	}
	_, err = Reserve(ctx, reader, Request{Dialect: "sqlite", Table: "messages", Column: "sequence", Scope: []Scope{{"turn_id", "t1"}}, Count: 1})
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) || (sqliteErr.Code != sqlite3.ErrBusy && sqliteErr.Code != sqlite3.ErrLocked) {
		t.Fatalf("expected original lock error, got %v", err)
	}
}
func TestScopedSequencePartitionsAndPresence(t *testing.T) {
	type useCase struct {
		desc, turn string
		count      int
		supplied   []int64
		expect     []int64
	}
	for _, tc := range []useCase{
		{"first partition", "t1", 1, nil, []int64{3}},
		{"second partition", "t2", 1, nil, []int64{101}},
		{"new partition", "t3", 2, nil, []int64{1, 2}},
		{"supplied low value skipped high value retained", "t1", 2, []int64{3, 100, 0}, []int64{4, 5}},
		{"supplied-only reservation does not allocate", "t1", 0, []int64{99}, []int64{}},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			db, _ := fixture(t)
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			values, err := Reserve(ctx, tx, Request{Dialect: "sqlite", Table: "messages", Column: "sequence", Scope: []Scope{{"turn_id", tc.turn}}, Count: tc.count, Supplied: tc.supplied})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(values, tc.expect) {
				t.Fatalf("values=%v expected=%v", values, tc.expect)
			}
			// Reservation must leave the transaction usable and application rows alone.
			var n int
			if err = tx.QueryRow("SELECT COUNT(*) FROM messages").Scan(&n); err != nil || n != 2 {
				t.Fatalf("source rows=%d error=%v", n, err)
			}
		})
	}
}
func TestScopedSequenceRollbackAndCommittedLedger(t *testing.T) {
	db, _ := fixture(t)
	ctx := context.Background()
	for _, commit := range []bool{false, true} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		values, err := Reserve(ctx, tx, Request{Dialect: "sqlite", Table: "messages", Column: "sequence", Scope: []Scope{{"turn_id", "t1"}}, Count: 1})
		if err != nil {
			t.Fatal(err)
		}
		if values[0] != 3 {
			t.Fatalf("first allocation=%v", values)
		}
		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	values, err := Reserve(ctx, tx, Request{Dialect: "sqlite", Table: "messages", Column: "sequence", Scope: []Scope{{"turn_id", "t1"}}, Count: 1})
	if err != nil || values[0] != 4 {
		t.Fatalf("committed counter=%v error=%v", values, err)
	}
}
func TestScopedSequenceIndependentConnectionRace(t *testing.T) {
	_, dsn := fixture(t)
	ctx := context.Background()
	const count = 8
	var wg sync.WaitGroup
	results := make(chan int64, count)
	failures := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db, err := sql.Open("sqlite3", dsn)
			if err != nil {
				failures <- err
				return
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				failures <- err
				return
			}
			defer tx.Rollback()
			values, err := Reserve(ctx, tx, Request{Dialect: "sqlite", Table: "messages", Column: "sequence", Scope: []Scope{{"turn_id", "t1"}}, Count: 1})
			if err != nil {
				failures <- err
				return
			}
			if _, err = tx.Exec("INSERT INTO messages VALUES(?,'t1',?)", fmt.Sprintf("new-%d", i), values[0]); err != nil {
				failures <- err
				return
			}
			if err = tx.Commit(); err != nil {
				failures <- err
				return
			}
			results <- values[0]
		}(i)
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	seen := map[int64]bool{}
	for n := range results {
		if seen[n] {
			t.Errorf("duplicate %d", n)
		}
		seen[n] = true
	}
	if len(seen) != count {
		t.Fatalf("successful allocations=%d expected=%d", len(seen), count)
	}
}
func TestScopedSequenceRejectsUnsafeRequests(t *testing.T) {
	db, _ := fixture(t)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	type useCase struct {
		desc  string
		input Request
	}
	for _, tc := range []useCase{
		{"table expression", Request{Dialect: "sqlite", Table: "messages;DROP TABLE messages", Column: "sequence", Scope: []Scope{{"turn_id", "t1"}}, Count: 1}},
		{"column expression", Request{Dialect: "sqlite", Table: "messages", Column: "sequence + 1", Scope: []Scope{{"turn_id", "t1"}}, Count: 1}},
		{"missing scope", Request{Dialect: "sqlite", Table: "messages", Column: "sequence", Count: 1}},
		{"null scope", Request{Dialect: "sqlite", Table: "messages", Column: "sequence", Scope: []Scope{{"turn_id", nil}}, Count: 1}},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if _, err := Reserve(ctx, tx, tc.input); err == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
}
