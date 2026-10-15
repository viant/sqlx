package registry

import (
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/database"
	"reflect"
	"strings"
	"sync"
)

const defaultProductName = "ansi"

type driverIdentity struct{ Package, Name string }

var explicitDrivers sync.Map

// RegisterDriver binds an exact Go driver identity to a database product.
// Aliases do not require importing the driver or registering another dialect.
func RegisterDriver(product database.Product, packagePath, typeName string) error {
	if product.Name == "" || packagePath == "" || typeName == "" {
		return fmt.Errorf("driver registration requires product, package and type")
	}
	key := driverIdentity{packagePath, typeName}
	if existing, loaded := explicitDrivers.LoadOrStore(key, product); loaded && !reflect.DeepEqual(existing, product) {
		return fmt.Errorf("driver %s.%s already has a different product", packagePath, typeName)
	}
	return nil
}

// MatchProduct matches product with sql driver
func MatchProduct(db *sql.DB) *database.Product {
	if db == nil {
		return nil
	}
	driverType := reflect.TypeOf(db.Driver())
	for driverType != nil && driverType.Kind() == reflect.Pointer {
		driverType = driverType.Elem()
	}
	if driverType == nil {
		return nil
	}
	if explicit, ok := explicitDrivers.Load(driverIdentity{driverType.PkgPath(), driverType.Name()}); ok {
		matched := explicit.(database.Product)
		matched.DriverPkg, matched.Driver = driverType.PkgPath(), driverType.Name()
		return &matched
	}
	driverTypeName := driverType.String()
	driverTypePair := strings.Split(driverTypeName, ".")
	if len(driverTypePair) != 2 {
		return nil
	}
	driverPkg := driverTypePair[0]
	driverName := driverTypePair[1]
	var product, defaultProduct *database.Product
	for name, candidate := range Products() {
		if strings.Contains(driverPkg, name) ||
			(candidate.DriverPkg != "" && strings.Contains(driverPkg, candidate.DriverPkg)) ||
			(candidate.Driver != "" && strings.Contains(candidate.Driver, driverName) && driverName != "Driver") { // CONDITION WAS MET FOR VERTICA AND BIGQUERY WHEN driverName == "Driver"
			// Driver/version discovery belongs to this caller, not the registry.
			matched := *candidate
			product = &matched
			product.DriverPkg = driverPkg
			product.Driver = driverName
		}
		if strings.Contains(candidate.Driver, defaultProductName) {
			defaultProduct = candidate
		}
	}
	if product == nil {
		if defaultProduct == nil {
			return nil
		}
		matched := *defaultProduct
		product = &matched
	}
	return product
}
