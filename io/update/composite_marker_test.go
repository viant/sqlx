package update_test

import (
	"context"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/update"
	"testing"
)

func TestCompositeIdentityArgumentsIgnoreIdentityMarkerBits(t *testing.T) {
	type flags struct{ Tenant, ID, Name, Score bool }
	type row struct {
		Tenant int64  `sqlx:"tenant,primaryKey"`
		ID     int64  `sqlx:"id,primaryKey"`
		Name   string `sqlx:"name"`
		Score  int64  `sqlx:"score"`
		Has    *flags `sqlx:"-" setMarker:"true"`
	}
	type result struct {
		Tenant, ID int64
		Name       string
		Score      int64
	}
	for _, tc := range []struct {
		name      string
		flags     flags
		nameValue string
		score     int64
	}{{"both identity bits false", flags{Name: true}, "new", 7}, {"first identity false", flags{ID: true, Score: true}, "old", 9}, {"second identity false", flags{Tenant: true, Name: true, Score: true}, "new", 9}} {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(tenant INTEGER,id INTEGER,name TEXT,score INTEGER,PRIMARY KEY(tenant,id))", "INSERT INTO records VALUES(1,0,'old',7),(2,0,'unrelated',8)")
			service, err := update.New(context.Background(), h.DB, "records")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.Exec(context.Background(), &row{Tenant: 1, ID: 0, Name: "new", Score: 9, Has: &tc.flags}); err != nil {
				t.Fatal(err)
			}
			sqlite.AssertRows(t, h, "SELECT tenant,id,name,score FROM records ORDER BY tenant,id", []result{{1, 0, tc.nameValue, tc.score}, {2, 0, "unrelated", 8}})
		})
	}
}
