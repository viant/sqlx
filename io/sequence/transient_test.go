package sequence

import (
	"context"
	"database/sql"
	"reflect"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestTransientScopedCountersWithoutSchemaObjects(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("CREATE TABLE messages(id TEXT PRIMARY KEY,turn_id TEXT,sequence INTEGER,UNIQUE(turn_id,sequence));INSERT INTO messages VALUES('seed','a',7)"); err != nil {
		t.Fatal(err)
	}
	var owner Transient
	reserve := func(turn string, count int, supplied []int64, want []int64) {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		got, err := owner.Reserve(ctx, db, tx, Request{Dialect: "sqlite", Table: "messages", Column: "sequence", Scope: []Scope{{"turn_id", turn}}, Count: count, Supplied: supplied})
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("values%v want%v err%v", got, want, err)
		}
	}
	reserve("a", 2, []int64{9}, []int64{8, 10})
	reserve("a", 1, nil, []int64{11})
	reserve("b", 1, nil, []int64{1})
	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", LedgerTable).Scan(&count); err != nil || count != 0 {
		t.Fatal("transient allocator created persisted counter storage", err, count)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	const workers = 16
	var wg sync.WaitGroup
	values := make(chan int64, workers)
	failures := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, e := owner.Reserve(ctx, db, tx, Request{Dialect: "sqlite", Table: "messages", Column: "sequence", Scope: []Scope{{"turn_id", "parallel"}}, Count: 1})
			if e != nil {
				failures <- e
				return
			}
			values <- v[0]
		}()
	}
	wg.Wait()
	close(values)
	close(failures)
	for e := range failures {
		t.Fatal(e)
	}
	seen := map[int64]bool{}
	for v := range values {
		if seen[v] {
			t.Fatal("duplicate transient value", v)
		}
		seen[v] = true
	}
	if len(seen) != workers {
		t.Fatal("missing allocations")
	}
}
