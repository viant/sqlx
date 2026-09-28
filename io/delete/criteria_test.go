package delete_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io/delete"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/option"
)

type criteriaDelete struct {
	ID      int     `sqlx:"id,primaryKey"`
	Owner   *string `sqlx:"owner"`
	Attempt int     `sqlx:"attempt"`
}

func TestDeleteCriteria(t *testing.T) {
	type useCase struct {
		desc   string
		input  *sqlx.Criteria
		expect int64
		fail   bool
	}
	for _, test := range []useCase{
		{desc: "compound predicate", input: &sqlx.Criteria{Expression: "owner = ? AND attempt <= ?", Placeholders: []any{"old", 0}}, expect: 1},
		{desc: "stale predicate", input: &sqlx.Criteria{Expression: "owner = ?", Placeholders: []any{"other"}}},
		{desc: "omitted predicate", expect: 1},
		{desc: "empty predicate", input: &sqlx.Criteria{}, expect: 1},
		{desc: "placeholder mismatch", input: &sqlx.Criteria{Expression: "owner = ?"}, fail: true},
	} {
		t.Run(test.desc, func(t *testing.T) {
			ctx := context.Background()
			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			if _, err = db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY,owner TEXT,attempt INTEGER);INSERT INTO records VALUES(1,'old',0),(2,'old',0)"); err != nil {
				t.Fatal(err)
			}
			service, err := delete.New(ctx, db, "records")
			if err != nil {
				t.Fatal(err)
			}
			affected, err := service.Exec(ctx, &criteriaDelete{ID: 1}, test.input)
			if (err != nil) != test.fail || affected != test.expect {
				t.Fatalf("affected=%d err=%v", affected, err)
			}
			var count int
			if err = db.QueryRow("SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 2-int(test.expect) {
				t.Fatalf("remaining=%d err=%v", count, err)
			}
		})
	}
}
func TestDeleteCriteriaCallerRollbackAndNull(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY,owner TEXT,attempt INTEGER);INSERT INTO records VALUES(1,NULL,0)"); err != nil {
		t.Fatal(err)
	}
	service, err := delete.New(ctx, db, "records")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	affected, err := service.Exec(ctx, &criteriaDelete{ID: 1}, tx, &sqlx.Criteria{Expression: "owner IS NULL"})
	if err != nil || affected != 1 {
		t.Fatalf("affected=%d err=%v", affected, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 1 {
		t.Fatalf("caller rollback: count=%d err=%v", count, err)
	}
}

func TestDeleteCriteriaBatchOverflowAndNoServiceLeak(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY,owner TEXT,attempt INTEGER);INSERT INTO records VALUES(1,'old',0),(2,'other',0),(3,'old',0)"); err != nil {
		t.Fatal(err)
	}
	service, err := delete.New(ctx, db, "records")
	if err != nil {
		t.Fatal(err)
	}
	affected, err := service.Exec(ctx, []*criteriaDelete{{ID: 1}, {ID: 2}, {ID: 3}}, option.BatchSize(2), &sqlx.Criteria{Expression: "owner = ?", Placeholders: []any{"old"}})
	if err != nil || affected != 2 {
		t.Fatalf("batch affected=%d err=%v", affected, err)
	}
	affected, err = service.Exec(ctx, &criteriaDelete{ID: 2})
	if err != nil || affected != 1 {
		t.Fatalf("predicate leaked across reusable service: affected=%d err=%v", affected, err)
	}
}
