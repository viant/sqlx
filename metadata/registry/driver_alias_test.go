package registry

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/viant/sqlx/metadata/database"
	"reflect"
	"testing"
)

type aliasFixtureDriver struct{}

func (aliasFixtureDriver) Open(string) (driver.Conn, error) {
	return nil, fmt.Errorf("metadata identity test never opens a connection")
}

type aliasFixtureConnector struct{ value driver.Driver }

func (c aliasFixtureConnector) Driver() driver.Driver                        { return c.value }
func (c aliasFixtureConnector) Connect(context.Context) (driver.Conn, error) { return c.value.Open("") }
func TestExactDriverAliasSupportsValueAndPointerDrivers(t *testing.T) {
	typeOf := reflect.TypeOf(aliasFixtureDriver{})
	key := driverIdentity{typeOf.PkgPath(), typeOf.Name()}
	product := database.Product{Name: "AliasFixture", Major: 3}
	if err := RegisterDriver(product, key.Package, key.Name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { explicitDrivers.Delete(key) })
	for _, drv := range []driver.Driver{aliasFixtureDriver{}, &aliasFixtureDriver{}} {
		db := sql.OpenDB(aliasFixtureConnector{drv})
		actual := MatchProduct(db)
		db.Close()
		if actual == nil || actual.Name != product.Name || actual.DriverPkg != key.Package || actual.Driver != key.Name {
			t.Fatalf("match=%+v", actual)
		}
		actual.Name = "mutated copy"
	}
	if err := RegisterDriver(database.Product{Name: "Different"}, key.Package, key.Name); err == nil {
		t.Fatal("conflicting driver registration accepted")
	}
	if MatchProduct(nil) != nil {
		t.Fatal("nil DB matched")
	}
}
