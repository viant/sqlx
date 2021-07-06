package ansi

import (
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"log"
)

//Product represents product
const product = "ANSI"

var ANSI = database.Product{
	Name:  product,
	Major: 1,
}

func init() {
	err := metadata.Register(
		info.NewQuery(info.KindVersion, "SELECT version()", ANSI),
	)

	if err != nil {
		log.Printf("failed to register queries: %v", err)
	}
}

//SELECT count(table_name) FROM information_schema.tables;
//SELECT column_name, data_type, column_default, is_nullable FROM information_schema.columns WHERE table_name='alpha'
//SELECT * FROM information_schema.information_schema_catalog_name;
