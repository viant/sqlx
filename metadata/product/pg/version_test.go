package pg_test

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	_ "github.com/viant/sqlx/metadata/product/pg"
	"github.com/viant/sqlx/metadata/registry"
	"strings"
	"testing"
)

func TestSequenceVersionBoundary(t *testing.T) {
	for _, major := range []int{9, 10, 17} {
		p := &database.Product{Name: "PostgreSQL", Major: major, Minor: 6}
		d := registry.LookupDialect(p)
		if got, want := d.DefaultPresetIDStrategy == dialect.PresetIDWithReservation, major >= 10; got != want {
			t.Fatalf("version %d reservation default=%v", major, got)
		}
		for _, kind := range []info.Kind{info.KindSequenceLock, info.KindSequenceReservation, info.KindSequenceNextValue} {
			q := registry.Lookup(p.Name, kind).Match(p)
			if (q != nil) != (major >= 10) {
				t.Fatalf("version %d kind %v query=%v", major, kind, q)
			}
		}
		q := registry.Lookup(p.Name, info.KindSequences).Match(p)
		if q == nil || strings.Contains(q.SQL, "pg_catalog.pg_sequence") != (major >= 10) {
			t.Fatalf("version %d wrong sequence catalog", major)
		}
	}
}
