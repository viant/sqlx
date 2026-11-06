package clickhouse

import (
	"log"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
)

const productName = "ClickHouse"

var product = database.Product{Name: productName}

// ClickHouse returns the ClickHouse database product.
func ClickHouse() *database.Product {
	return &product
}

func init() {
	if err := registry.RegisterDriver(product, "github.com/ClickHouse/clickhouse-go/v2", "stdDriver"); err != nil {
		panic(err)
	}
	if err := registry.Register(
		info.NewQuery(info.KindVersion, "SELECT concat('ClickHouse ', version())", product),
		info.NewQuery(info.KindCurrentSchema, "SELECT currentDatabase() AS SCHEMA_NAME", product),
		info.NewQuery(info.KindSchemas, `SELECT '' AS CATALOG_NAME, name AS SCHEMA_NAME, '' AS SQL_PATH, 'UTF-8' AS DEFAULT_CHARACTER_SET_NAME, '' AS DEFAULT_COLLATION_NAME FROM system.databases`, product,
			info.NewCriterion(info.Catalog, "")),
		info.NewQuery(info.KindSchema, `SELECT '' AS CATALOG_NAME, name AS SCHEMA_NAME, '' AS SQL_PATH, 'UTF-8' AS DEFAULT_CHARACTER_SET_NAME, '' AS DEFAULT_COLLATION_NAME FROM system.databases`, product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "name")),
		info.NewQuery(info.KindTables, `SELECT '' AS TABLE_CATALOG, database AS TABLE_SCHEMA, name AS TABLE_NAME, if(engine = 'View', 'VIEW', 'TABLE') AS TABLE_TYPE, engine AS ENGINE, create_table_query AS DDL FROM system.tables`, product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "database")),
		info.NewQuery(info.KindTable, `SELECT '' AS TABLE_CATALOG, database AS TABLE_SCHEMA, table AS TABLE_NAME, name AS COLUMN_NAME, position AS ORDINAL_POSITION, comment AS COLUMN_COMMENT, type AS DATA_TYPE, if(startsWith(type, 'Nullable('), 'YES', 'NO') AS IS_NULLABLE, if(default_kind = '', NULL, default_expression) AS COLUMN_DEFAULT, '' AS COLUMN_KEY FROM system.columns`, product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "database"),
			info.NewCriterion(info.Table, "table")),
		info.NewQuery(info.KindSession, `SELECT '' AS PID, currentUser() AS USER_NAME, currentDatabase() AS SCHEMA_NAME, '' AS APP_NAME`, product),
	); err != nil {
		log.Printf("failed to register ClickHouse metadata queries: %v", err)
	}
	registry.RegisterDialect(&info.Dialect{
		Product:                 product,
		Placeholder:             "?",
		Transactional:           false,
		DirectQuery:             true,
		InsertBatchCommit:       true,
		Insert:                  dialect.InsertWithMultiValues,
		Upsert:                  dialect.UpsertTypeUnsupported,
		Load:                    dialect.LoadTypeUnsupported,
		QuoteCharacter:          '\'',
		CanAutoincrement:        false,
		CanLastInsertID:         false,
		DefaultPresetIDStrategy: dialect.PresetIDStrategyUndefined,
	})
}
