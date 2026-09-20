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
