package ansi

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
)

//Product represents product
const product = "ANSI"

var ANSI = database.Product{
	Name:  product,
	Major: 1,
}

func init() {
	registry.RegisterDialect(&info.Dialect{
		Product:          ANSI,
		Placeholder:      "?",
		Transactional:    true,
		Insert:           dialect.InsertWithSingleValues,
		Upsert:           dialect.UpsertTypeUnsupported,
		Load:             dialect.LoadTypeUnsupported,
		CanAutoincrement: false,
	})
}

//SELECT count(table_name) FROM information_schema.tables;
//SELECT column_name, data_type, column_default, is_nullable FROM information_schema.columns WHERE table_name='alpha'
//SELECT * FROM information_schema.information_schema_catalog_name;