package sequence_test

import (
	"context"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/metadata/product/sqlite/sequence"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

func TestDirectReservationRespectsExplicitSchema(t *testing.T) {
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	h.Exec(t, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO records VALUES(50)", "ATTACH ':memory:' AS aux", "CREATE TABLE aux.records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO aux.records VALUES(5)")
	result := &sink.Sequence{}
	_, err := (&sequence.Next{}).Handle(context.Background(), h.DB, result, option.NewArgs("", "AUX", "RECORDS"), option.RecordCount(2))
	if err != nil || result.Schema != "aux" || result.Name != "records" || result.MinValue(2) != 6 {
		t.Fatalf("schema authority lost: %+v %v", result, err)
	}
	sqlite.AssertRows(t, h, "SELECT seq FROM aux.sqlite_sequence WHERE name='records'", []struct{ Seq int }{{7}})
	sqlite.AssertRows(t, h, "SELECT seq FROM main.sqlite_sequence WHERE name='records'", []struct{ Seq int }{{50}})
}

func TestReservationFailurePreservesTarget(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO records VALUES(5)")
	for _, tc := range []struct {
		name  string
		count int64
	}{
		{"missing", 1}, {"records", 0}, {"records", -1}, {"", 1},
	} {
		before := sink.Sequence{Name: "untouched", Value: 99}
		result := before
		_, err := (&sequence.Next{}).Handle(context.Background(), h.DB, &result, option.NewArgs("", "main", tc.name), option.RecordCount(tc.count))
		if err == nil || result != before {
			t.Fatalf("failed reservation modified output: %+v %v", result, err)
		}
	}
	sqlite.AssertRows(t, h, "SELECT seq FROM sqlite_sequence WHERE name='records'", []struct{ Seq int }{{5}})
}

func TestAutoIncrementReservationUsesOnlyEngineCounter(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT NOT NULL)")
	ctx := context.Background()
	for i := int64(0); i < 2; i++ {
		reserved := &sink.Sequence{}
		if _, err := (&sequence.Next{}).Handle(ctx, h.DB, reserved, option.NewArgs("", "main", "records"), option.RecordCount(3)); err != nil {
			t.Fatal(err)
		}
		if reserved.MinValue(3) != 1+i*3 {
			t.Fatalf("unexpected allocation: %+v", reserved)
		}
	}
	sqlite.AssertRows(t, h, "SELECT COUNT(*) AS n FROM sqlite_master WHERE name='sqlx_sequence_reservations'", []struct{ N int }{{0}})
	sqlite.AssertRows(t, h, "SELECT COUNT(*) AS n FROM records", []struct{ N int }{{0}})
	inserted, err := h.DB.ExecContext(ctx, "INSERT INTO records(label) VALUES('native')")
	if err != nil {
		t.Fatal(err)
	}
	id, err := inserted.LastInsertId()
	if err != nil || id != 7 {
		t.Fatalf("native insert collided with reservation: id=%d err=%v", id, err)
	}
}

func TestAutoIncrementReservationKeepsCallerTransactionOwnership(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO records VALUES(10)")
	h.DB.SetMaxOpenConns(1)
	ctx := context.Background()
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	reserved := &sink.Sequence{}
	if _, err = (&sequence.Next{}).Handle(ctx, h.DB, reserved, tx, option.NewArgs("", "main", "records"), option.RecordCount(2)); err != nil {
		t.Fatal(err)
	}
	var next int64
	if err = tx.QueryRowContext(ctx, "SELECT seq FROM sqlite_sequence WHERE name='records'").Scan(&next); err != nil || next != 12 {
		t.Fatalf("pending native counter=%d err=%v", next, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	sqlite.AssertRows(t, h, "SELECT seq FROM sqlite_sequence WHERE name='records'", []struct{ Seq int }{{10}})
	sqlite.AssertRows(t, h, "SELECT COUNT(*) AS n FROM sqlite_master WHERE name='sqlx_sequence_reservations'", []struct{ N int }{{0}})
}
