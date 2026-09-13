package insert_test

import (
	"context"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/option"
	"testing"
)

func TestInsertPreservesExplicitZeroIdentity(t *testing.T) {
	type flags struct{ ID, Name bool }
	type row struct {
		ID   *int64 `sqlx:"id,primaryKey"`
		Name string `sqlx:"name"`
		Has  *flags `sqlx:"-" setMarker:"true"`
	}
	type result struct {
		ID   int64
		Name string
	}
	for _, batch := range []int{1, 3} {
		t.Run(string(rune('0'+batch)), func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(5,'prior')")
			zero := int64(0)
			unmarkedZero := int64(0)
			rows := []*row{{ID: &zero, Name: "explicit", Has: &flags{ID: true}}, {Name: "omitted", Has: &flags{}}, {ID: &unmarkedZero, Name: "unmarked", Has: &flags{}}}
			service, err := insert.New(context.Background(), h.DB, "records")
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = service.Exec(context.Background(), rows, option.BatchSize(batch), dialect.PresetIDWithMax); err != nil {
				t.Fatal(err)
			}
			if *rows[0].ID != 0 || rows[1].ID == nil || *rows[1].ID != 6 || *rows[2].ID != 7 {
				t.Fatalf("wrong identity backfill: %+v %+v %+v", rows[0], rows[1], rows[2])
			}
			sqlite.AssertRows(t, h, "SELECT id,name FROM records ORDER BY id", []result{{0, "explicit"}, {5, "prior"}, {6, "omitted"}, {7, "unmarked"}})
		})
	}
}

func TestInsertCompositeZeroPreservesEachIdentityColumn(t *testing.T) {
	type flags struct{ Tenant, ID, Name bool }
	type row struct {
		Tenant int64  `sqlx:"tenant,primaryKey"`
		ID     int64  `sqlx:"id,primaryKey"`
		Name   string `sqlx:"name"`
		Has    *flags `sqlx:"-" setMarker:"true"`
	}
	type result struct {
		Tenant, ID int64
		Name       string
	}
	for _, batch := range []int{1, 3} {
		t.Run(string(rune('0'+batch)), func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(tenant INTEGER NOT NULL,id INTEGER NOT NULL,name TEXT,PRIMARY KEY(tenant,id))")
			rows := []*row{{Tenant: 0, ID: 3, Name: "first zero", Has: &flags{Tenant: true, ID: true}}, {Tenant: 2, ID: 0, Name: "second zero", Has: &flags{Tenant: true, ID: true}}, {Tenant: 0, ID: 0, Name: "both zero", Has: &flags{Tenant: true, ID: true}}}
			service, err := insert.New(context.Background(), h.DB, "records")
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = service.Exec(context.Background(), rows, option.BatchSize(batch)); err != nil {
				t.Fatal(err)
			}
			if rows[0].Tenant != 0 || rows[0].ID != 3 || rows[1].Tenant != 2 || rows[1].ID != 0 || rows[2].Tenant != 0 || rows[2].ID != 0 {
				t.Fatalf("composite identity changed %+v %+v %+v", rows[0], rows[1], rows[2])
			}
			sqlite.AssertRows(t, h, "SELECT tenant,id,name FROM records ORDER BY tenant,id", []result{{0, 0, "both zero"}, {0, 3, "first zero"}, {2, 0, "second zero"}})
		})
	}
}
