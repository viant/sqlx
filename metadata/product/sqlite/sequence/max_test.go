package sequence_test

import (
	"context"
	"testing"
	"time"

	"github.com/viant/sqlx"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/metadata/product/sqlite/sequence"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

func TestMaxUsesSuppliedTransactionAndBoundedSingleConnection(t *testing.T) {
	for _, count := range []int64{1, 3} {
		h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY)")
		h.DB.SetMaxOpenConns(1)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		tx, err := h.DB.BeginTx(ctx, nil)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(41)"); err != nil {
			tx.Rollback()
			cancel()
			t.Fatal(err)
		}
		result := &sink.Sequence{}
		_, err = (&sequence.Max{}).Handle(ctx, h.DB, result, tx, option.RecordCount(count), option.NewArgs("", "main", "records"), func() *sqlx.SQL { return &sqlx.SQL{Query: "SELECT COALESCE(MAX(id),0) FROM records"} })
		if err != nil {
			tx.Rollback()
			cancel()
			t.Fatal(err)
		}
		if result.MinValue(count) != 42 || result.Name != "records" || result.Schema != "main" {
			tx.Rollback()
			cancel()
			t.Fatalf("sequence=%+v", result)
		}
		if err = tx.Rollback(); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		sqlite.AssertRows(t, h, "SELECT COUNT(*) AS count FROM records", []struct{ Count int }{{0}})
	}
}

func TestMaxResolvesCanonicalSQLiteTableNamesAndSchemas(t *testing.T) {
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	h.Exec(t, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "INSERT INTO records VALUES(5)", "CREATE TEMP TABLE records(id INTEGER PRIMARY KEY)", "INSERT INTO temp.records VALUES(50)", `CREATE TABLE "odd.name"(id INTEGER PRIMARY KEY)`, `INSERT INTO "odd.name" VALUES(9)`)
	for _, tc := range []struct {
		table, schema, name string
		want                int64
	}{
		{"records", "temp", "records", 51}, {"RECORDS", "temp", "records", 51},
		{`"MAIN"."RECORDS"`, "main", "records", 6}, {"[main].[records]", "main", "records", 6},
		{`"odd.name"`, "main", "odd.name", 10},
	} {
		t.Run(tc.table, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			result := &sink.Sequence{}
			_, err := (&sequence.Max{}).Handle(ctx, h.DB, result, option.RecordCount(1), option.SequenceTable(tc.table), option.NewArgs("", "main", "custom_sequence"), func() *sqlx.SQL { return &sqlx.SQL{Query: "SELECT COALESCE(MAX(id),0) FROM " + tc.table} })
			if err != nil || result.Schema != tc.schema || result.Name != tc.name || result.MinValue(1) != tc.want {
				t.Fatalf("sequence=%+v err=%v", result, err)
			}
		})
	}
}

func TestMaxPreservesLegacyLogicalSequenceName(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "INSERT INTO records VALUES(7)")
	for _, name := range []string{"custom_sequence", "", "not a table identifier"} {
		t.Run(name, func(t *testing.T) {
			result := &sink.Sequence{}
			_, err := (&sequence.Max{}).Handle(context.Background(), h.DB, result, option.RecordCount(2), option.NewArgs("", "main", name), func() *sqlx.SQL { return &sqlx.SQL{Query: "SELECT COALESCE(MAX(id),0) FROM records"} })
			if err != nil || result.Name != name || result.MinValue(2) != 8 {
				t.Fatalf("sequence=%+v err=%v", result, err)
			}
		})
	}
}
