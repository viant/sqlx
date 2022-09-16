package registry

import (
	"database/sql"
	"github.com/viant/sqlx/metadata/database"
	"reflect"
	"strings"
)

const defaultProductName = "ansi"

//MatchProduct matches product with sql driver
func MatchProduct(db *sql.DB) *database.Product {
	driverTypeName := reflect.TypeOf(db.Driver()).Elem().String()
	driverTypePair := strings.Split(driverTypeName, ".")
	driverPkg := driverTypePair[0]
	driverName := driverTypePair[1]
	var product, defaultProduct *database.Product
	for name, candidate := range Products() {
		if strings.Contains(driverPkg, name) ||
			(candidate.DriverPkg != "" && strings.Contains(driverPkg, candidate.DriverPkg)) ||
			(candidate.Driver != "" && strings.Contains(candidate.Driver, driverName) && driverName != "Driver") { // CONDITION WAS MET FOR VERTICA AND BIGQUERY WHEN driverName == "Driver"
			product = candidate
			product.DriverPkg = driverPkg
			product.Driver = driverName
		}
		if strings.Contains(candidate.Driver, defaultProductName) {
			defaultProduct = candidate
		}
	}
	if product == nil {
		product = defaultProduct
	}
	return product
}
