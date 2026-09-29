package sequence

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"reflect"
	"sync"
	"testing"
)

func TestScopedSequenceMySQLLive(t *testing.T) {
	dsn := os.Getenv("SQLX_SCOPED_MYSQL_DSN")
	if dsn == "" {
		t.Skip("SQLX_SCOPED_MYSQL_DSN is not configured")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if err = Provision(ctx, db, "mysql"); err != nil {
		t.Fatal(err)
	}
	table := "scope_fixture_messages"
	defer db.Exec("DROP TABLE IF EXISTS " + table)
	if _, err = db.Exec("CREATE TABLE " + table + "(id VARCHAR(64) PRIMARY KEY,turn_id VARCHAR(64),sequence BIGINT,UNIQUE(turn_id,sequence)) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("INSERT INTO " + table + " VALUES('a','t1',2),('b','t2',100)"); err != nil {
		t.Fatal(err)
	}
	type useCase struct {
		desc, turn string
		supplied   []int64
		expected   []int64
	}
	for _, tc := range []useCase{{"first scope", "t1", nil, []int64{3}}, {"second scope", "t2", nil, []int64{101}}, {"supplied skipped", "t1", []int64{3}, []int64{4}}} {
		t.Run(tc.desc, func(t *testing.T) {
			tx, e := db.BeginTx(ctx, nil)
			if e != nil {
				t.Fatal(e)
			}
			defer tx.Rollback()
			values, e := Reserve(ctx, tx, Request{Dialect: "mysql", Table: table, Column: "sequence", Scope: []Scope{{"turn_id", tc.turn}}, Count: 1, Supplied: tc.supplied})
			if e != nil || !reflect.DeepEqual(values, tc.expected) {
				t.Fatalf("values=%v expected=%v error=%v", values, tc.expected, e)
			}
		})
	}
	const count = 6
	var wg sync.WaitGroup
	failures := make(chan error, count)
	values := make(chan int64, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			connection, e := sql.Open("mysql", dsn)
			if e != nil {
				failures <- e
				return
			}
			defer connection.Close()
			connection.SetMaxOpenConns(1)
			tx, e := connection.BeginTx(ctx, nil)
			if e != nil {
				failures <- e
				return
			}
			defer tx.Rollback()
			allocated, e := Reserve(ctx, tx, Request{Dialect: "mysql", Table: table, Column: "sequence", Scope: []Scope{{"turn_id", "t1"}}, Count: 1})
			if e != nil {
				failures <- e
				return
			}
			if _, e = tx.Exec("INSERT INTO "+table+" VALUES(?,'t1',?)", fmt.Sprintf("new%d", i), allocated[0]); e != nil {
				failures <- e
				return
			}
			if e = tx.Commit(); e != nil {
				failures <- e
				return
			}
			values <- allocated[0]
		}(i)
	}
	wg.Wait()
	close(failures)
	close(values)
	for e := range failures {
		t.Error(e)
	}
	seen := map[int64]bool{}
	for value := range values {
		if seen[value] {
			t.Errorf("duplicate %d", value)
		}
		seen[value] = true
	}
	if len(seen) != count {
		t.Fatalf("allocated=%d", len(seen))
	}
}
