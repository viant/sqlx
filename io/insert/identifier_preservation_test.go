package insert

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	"testing"
)

func TestBuilderExplicitIdentifiers(t *testing.T) {
	d := &info.Dialect{Product: database.Product{Name: "SQLite"}, Placeholder: "?"}
	b, err := NewBuilder(`"a.b"`, []string{"select", "Mixed Case"}, d, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	actual := b.Build(nil, option.BatchSize(1))
	if expected := `INSERT INTO "a.b"("select","Mixed Case") VALUES (?,?)`; actual != expected {
		t.Fatalf("%s != %s", actual, expected)
	}
}
