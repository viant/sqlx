package ansi

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/info/placeholder"
	"github.com/viant/sqlx/metadata/registry"
)

//Product represents product
const product = "ANSI"

//ANSI defines default product
var ANSI = database.Product{
	Name:   product,
	Major:  1,
	Driver: "ansi",
}

func init() {
	registry.RegisterDialect(&info.Dialect{
		Product:                 ANSI,
		Placeholder:             "?",
		Transactional:           true,
		Insert:                  dialect.InsertWithSingleValues,
		Upsert:                  dialect.UpsertTypeUnsupported,
		Load:                    dialect.LoadTypeUnsupported,
		PlaceholderResolver:     &placeholder.DefaultGenerator{},
		DefaultPresetIDStrategy: dialect.PresetIDStrategyUndefined,
	})
	registry.Register(
		info.NewQuery(info.KindVersion, "SELECT 1", ANSI).OnPre(info.NopHandler()),
		info.NewQuery(info.KindSession, "SELECT 1", ANSI).OnPre(info.NopHandler()),

		info.NewQuery(info.KindSchemas, "SELECT 1", ANSI,
			info.NewCriterion(info.Catalog, "CATALOG_NAME")).OnPre(info.NopHandler()),

		info.NewQuery(info.KindSchema, "SELECT 1", ANSI,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
			info.NewCriterion(info.Schema, "SCHEMA_NAME")).OnPre(info.NopHandler()),

		info.NewQuery(info.KindCatalogs, "SELECT 1", ANSI).OnPre(info.NopHandler()),
		info.NewQuery(info.KindCatalog, "SELECT 1", ANSI,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
		).OnPre(info.NopHandler()),

		info.NewQuery(info.KindTables, "SELECT 1", ANSI,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA")).OnPre(info.NopHandler()),

		info.NewQuery(info.KindTable, "SELECT 1", ANSI,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME")).OnPre(info.NopHandler()),
	)
}
