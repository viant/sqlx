package validator_test

import (
	"context"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
)

type scopedUniqueRow struct {
	ID     int                   `sqlx:"id,primaryKey"`
	Tenant *int                  `sqlx:"tenant_id"`
	Name   string                `sqlx:"name,uniqueDep=tenant_id,table=records"`
	Has    *scopedUniquePresence `sqlx:"presence=true"`
}
type scopedUniquePresence struct{ ID, Tenant, Name bool }

func TestUniquePreviousSparseDependencySQLite(t *testing.T) {
	one, two := 1, 2
	tests := []struct {
		name    string
		current *scopedUniqueRow
		failed  bool
	}{
		{"name changes omitted dependency uses previous", &scopedUniqueRow{ID: 1, Name: "taken", Has: &scopedUniquePresence{Name: true}}, true},
		{"dependency changes omitted name uses previous", &scopedUniqueRow{ID: 1, Tenant: &two, Has: &scopedUniquePresence{Tenant: true}}, true},
		{"both omitted ignores bogus working values", &scopedUniqueRow{ID: 1, Tenant: &two, Name: "taken", Has: &scopedUniquePresence{}}, false},
		{"explicit null dependency no SQL equality", &scopedUniqueRow{ID: 1, Name: "taken", Has: &scopedUniquePresence{Tenant: true, Name: true}}, false},
		{"same name in different tenant allowed", &scopedUniqueRow{ID: 1, Tenant: &two, Name: "taken", Has: &scopedUniquePresence{Tenant: true, Name: true}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(id INTEGER PRIMARY KEY,tenant_id INTEGER,name TEXT,UNIQUE(tenant_id,name))", "INSERT INTO records VALUES(1,1,'old'),(2,1,'taken'),(3,2,'old')")
			previous := &scopedUniqueRow{ID: 1, Tenant: &one, Name: "old"}
			result, err := validator.New().Validate(context.Background(), h.DB, tc.current, validator.WithPrevious(previous), validator.WithSetMarker(), validator.WithShallow(true))
			if err != nil {
				t.Fatal(err)
			}
			if result.Failed != tc.failed {
				t.Fatalf("got %v want failed=%v", result, tc.failed)
			}
			if previous.Tenant != &one || previous.Name != "old" {
				t.Fatal("previous changed")
			}
		})
	}
}
