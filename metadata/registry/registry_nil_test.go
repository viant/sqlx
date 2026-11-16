package registry

import (
	"testing"

	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

func TestLookupDialectNilProduct(t *testing.T) {
	if actual := LookupDialect(nil); actual != nil {
		t.Fatalf("LookupDialect(nil) = %v, want nil", actual)
	}
}

func TestRegistryLookupDialectProduct(t *testing.T) {
	older := &info.Dialect{Product: database.Product{Name: "fixture", Major: 1}}
	newer := &info.Dialect{Product: database.Product{Name: "fixture", Major: 3}}
	r := &registry{dialects: map[string]info.Dialects{"fixture": {older, newer}}}
	tests := []struct {
		name    string
		product *database.Product
		want    *info.Dialect
	}{
		{name: "nil"},
		{name: "unknown", product: &database.Product{Name: "unknown"}},
		{name: "compatible older", product: &database.Product{Name: "fixture", Major: 2}, want: older},
		{name: "compatible newer", product: &database.Product{Name: "fixture", Major: 3}, want: newer},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if actual := r.LookupDialect(test.product); actual != test.want {
				t.Fatalf("LookupDialect(%+v) = %v, want %v", test.product, actual, test.want)
			}
		})
	}
}
