package insert

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/product/pg"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// This is an assignment-contract unit test, not evidence for a database dialect.
// The real PostgreSQL cache/interleaving acceptance test lives separately.
func TestReservationExactValueAssignment(t *testing.T) {
	for _, values := range [][]int64{{2, 3, 7}, {-2, -3, -7}, {9, 1, 4}} {
		n := &numericSequencer{sequence: &sink.Sequence{Name: "exact"}, reservation: &sink.Reservation{Values: values}, shallPresetIdentities: true, detectedPreset: true}
		supplied := int64(42)
		var cell any = &supplied
		if err := n.updateRecord(context.Background(), nil, nil, &cell, len(values), nil, nil); err != nil || supplied != 42 || n.reservationIndex != 0 {
			t.Fatalf("supplied value was consumed: %d %v", supplied, err)
		}
		actual := make([]int64, len(values))
		for i := range actual {
			cell = &actual[i]
			if err := n.updateRecord(context.Background(), nil, nil, &cell, len(values), nil, nil); err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(actual, values) {
			t.Fatalf("fabricated arithmetic values: %v want %v", actual, values)
		}
		exhausted := int64(0)
		cell = &exhausted
		if err := n.updateRecord(context.Background(), nil, nil, &cell, 1, nil, nil); err == nil || exhausted != 0 {
			t.Fatal("exhausted reservation changed a record")
		}
	}
}

func TestReservationInsertRendering(t *testing.T) {
	for _, tc := range []struct {
		dialect     *info.Dialect
		table, want string
	}{
		{registry.LookupDialect(mysql.MySQL5()), "`schema`.`records`", "INSERT INTO `schema`.`records`(id) VALUES (?)"},
		{registry.LookupDialect(pg.PqSQL9()), `"schema"."records"`, `INSERT INTO "schema"."records"(id) VALUES ($1) RETURNING id`},
		{registry.LookupDialect(&database.Product{Name: "PostgreSQL", Major: 17}), `"schema"."records"`, `INSERT INTO "schema"."records"(id) OVERRIDING SYSTEM VALUE VALUES ($1) RETURNING id`},
	} {
		builder, err := NewBuilder(tc.table, []string{"id"}, tc.dialect, "id", 1)
		if err != nil {
			t.Fatal(err)
		}
		if got := builder.Build(nil, option.BatchSize(1)); got != tc.want {
			t.Fatalf("native insert rendering: %s want %s", got, tc.want)
		}
	}
}
