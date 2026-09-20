package registry_test

import (
	"sync"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/registry"
)

func TestMatchProductKeepsRegisteredMetadataImmutable(t *testing.T) {
	h := sqlite.New(t)
	registered := registry.Products()["sqlite"]
	if registered == nil {
		t.Fatal("SQLite product not registered")
	}
	before := *registered
	products := make([]*database.Product, 16)
	var group sync.WaitGroup
	for i := range products {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			products[i] = registry.MatchProduct(h.DB)
			products[i].Major = i + 100
		}(i)
	}
	group.Wait()
	if *registered != before {
		t.Fatal("driver/version discovery mutated registered metadata")
	}
	for i, product := range products {
		if product == registered || product.Major != i+100 || product.Driver == "" {
			t.Fatalf("discovery result is shared: %+v", product)
		}
	}
}
