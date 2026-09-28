package update_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io/update"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/option"
)

type criteriaRecord struct {
	Title   string             `sqlx:"title"`
	Owner   *string            `sqlx:"owner"`
	Attempt int                `sqlx:"attempt"`
	ID      int                `sqlx:"id,primaryKey"`
	Has     *criteriaRecordHas `setMarker:"true" sqlx:"-"`
}
type criteriaRecordHas struct{ Title, Owner, Attempt, ID bool }

func TestUpdateCriteria(t *testing.T) {
	type useCase struct {
		desc   string
		input  *sqlx.Criteria
		expect int64
		fail   bool
	}
	tests := []useCase{
		{desc: "compound equality and range", input: &sqlx.Criteria{Expression: "owner = ? AND attempt >= ?", Placeholders: []any{"old", 0}}, expect: 1},
		{desc: "stale owner returns zero", input: &sqlx.Criteria{Expression: "owner = ?", Placeholders: []any{"other"}}, expect: 0},
		{desc: "false range returns zero", input: &sqlx.Criteria{Expression: "attempt > ?", Placeholders: []any{0}}, expect: 0},
		{desc: "explicit zero activates predicate", input: &sqlx.Criteria{Expression: "attempt = ?", Placeholders: []any{0}}, expect: 1},
		{desc: "omitted predicate", expect: 1},
		{desc: "empty predicate", input: &sqlx.Criteria{}, expect: 1},
		{desc: "placeholder mismatch", input: &sqlx.Criteria{Expression: "owner = ?"}, fail: true},
		{desc: "SQL-shaped value remains a parameter", input: &sqlx.Criteria{Expression: "owner = ?", Placeholders: []any{"old' OR 1=1 --"}}, expect: 0},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ctx := context.Background()
			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			_, err = db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY,title TEXT,owner TEXT,attempt INTEGER);INSERT INTO records VALUES(1,'keep','old',0)")
			if err != nil {
				t.Fatal(err)
			}
			service, err := update.New(ctx, db, "records")
			if err != nil {
				t.Fatal(err)
			}
			row := &criteriaRecord{ID: 1, Title: "changed", Has: &criteriaRecordHas{ID: true, Title: true}}
			affected, err := service.Exec(ctx, row, test.input)
			if (err != nil) != test.fail {
				t.Fatalf("err=%v", err)
			}
			if affected != test.expect {
				t.Fatalf("affected=%d expected=%d", affected, test.expect)
			}
			var title, owner string
			var attempt int
			if err := db.QueryRow("SELECT title,owner,attempt FROM records WHERE id=1").Scan(&title, &owner, &attempt); err != nil {
				t.Fatal(err)
			}
			expected := "keep"
			if test.expect == 1 {
				expected = "changed"
			}
			if title != expected || owner != "old" || attempt != 0 {
				t.Fatalf("sparse/guard behavior: title=%q owner=%q attempt=%d", title, owner, attempt)
			}
		})
	}
}

func TestUpdateCriteriaCallerTransactionAndIfMatch(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY,title TEXT,owner TEXT,attempt INTEGER);INSERT INTO records VALUES(1,'keep','old',0)"); err != nil {
		t.Fatal(err)
	}
	service, err := update.New(ctx, db, "records")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	row := &criteriaRecord{ID: 1, Title: "pending", Has: &criteriaRecordHas{ID: true, Title: true}}
	affected, err := service.Exec(ctx, row, tx, option.IfMatch{Column: "attempt", Value: 0}, &sqlx.Criteria{Expression: "owner = ?", Placeholders: []any{"old"}})
	if err != nil || affected != 1 {
		t.Fatalf("affected=%d err=%v", affected, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var title string
	if err = db.QueryRow("SELECT title FROM records WHERE id=1").Scan(&title); err != nil || title != "keep" {
		t.Fatalf("caller transaction title=%q err=%v", title, err)
	}
	affected, err = service.Exec(ctx, row, option.IfMatch{Column: "attempt", Value: 1}, &sqlx.Criteria{Expression: "owner = ?", Placeholders: []any{"old"}})
	if affected != 0 || !errors.Is(err, option.ErrNoMatch) {
		t.Fatalf("existing IfMatch contract changed: affected=%d err=%v", affected, err)
	}
}
