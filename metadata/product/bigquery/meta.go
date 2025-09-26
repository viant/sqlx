package bigquery

import (
	"log"

	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
)

const product = "BigQuery"
const driver = "Driver"
const driverPkg = "bigquery"

var bigQuery = database.Product{
	Name:      product,
	DriverPkg: driverPkg,
	Driver:    driver,
	//Major:     int,
	//Minor:     int,
	//Release:   int
}

// BigQuery return BigQuery product
func BigQuery() *database.Product {
	return &bigQuery
}

func init() {
	err := registry.Register(
		info.NewQuery(info.KindVersion, "SELECT 'BigQuery 0.0.0'", bigQuery), // Parsing the version gives an error if the version number doesn't exist
		info.NewQuery(info.KindSchemas, `SELECT
CATALOG_NAME, 
SCHEMA_NAME,
COALESCE(LOCATION,'') AS SQL_PATH,
'utf8' DEFAULT_CHARACTER_SET_NAME,
'' AS  DEFAULT_COLLATION_NAME,
LOCATION AS REGION
FROM $Args[1].INFORMATION_SCHEMA.SCHEMATA
`, bigQuery,
			info.NewCriterion(info.Catalog, ""),
		),
		info.NewQuery(info.KindSchema, `SELECT
CATALOG_NAME, 
SCHEMA_NAME,
COALESCE(LOCATION,'') AS SQL_PATH,
'utf8' DEFAULT_CHARACTER_SET_NAME,
'' AS  DEFAULT_COLLATION_NAME,
LOCATION AS REGION
FROM $Args[1].INFORMATION_SCHEMA.SCHEMATA
`, bigQuery,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "SCHEMA_NAME"),
		),
		info.NewQuery(info.KindSchema, `SELECT
CATALOG_NAME, 
SCHEMA_NAME,
COALESCE(LOCATION,'') AS SQL_PATH,
'utf8' DEFAULT_CHARACTER_SET_NAME,
'' AS  DEFAULT_COLLATION_NAME,
LOCATION AS REGION
FROM $Args[1].INFORMATION_SCHEMA.SCHEMATA
`, bigQuery,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "SCHEMA_NAME"),
		),
		info.NewQuery(info.KindTables, `SELECT
TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_TYPE,
TABLE_NAME,
'' AS AUTO_INCREMENT,
CREATION_TIME AS CREATE_TIME,
CREATION_TIME AS UPDATE_TIME,
0 AS TABLE_ROWS,
'' VERSION,
TABLE_TYPE AS ENGINE,
DDL
FROM $Args[1].INFORMATION_SCHEMA.TABLES`,
			bigQuery,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
		),

		info.NewQuery(info.KindTable, `SELECT 
TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME,
ORDINAL_POSITION,


'' COLUMN_COMMENT,
DATA_TYPE,
CAST(NULL AS INT64) CHARACTER_MAXIMUM_LENGTH,
CAST(NULL AS INT64) NUMERIC_PRECISION,
CAST(NULL AS INT64) NUMERIC_SCALE,
IS_NULLABLE,
'' COLUMN_DEFAULT,
'' COLUMN_KEY
FROM $Args[1].INFORMATION_SCHEMA.COLUMNS
`,
			bigQuery,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
		),

		info.NewQuery(info.KindSequences, `SELECT * FROM (SELECT 
  '' AS SEQUENCE_CATALOG,
  '' AS SEQUENCE_SCHEMA, 
  '' AS SEQUENCE_NAME,
  '' AS DATA_TYPE,
  CAST(NULL AS INT64) AS MAX_VALUE,
  CAST(NULL AS INT64) AS SEQUENCE_VALUE) WHERE 1 = 0
`,
			bigQuery,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Sequence, ""),
		),

		info.NewQuery(info.KindIndexes, `SELECT * FROM (SELECT 
		'' AS TABLE_CATALOG,
		'' AS TABLE_SCHEMA,
		'' AS TABLE_NAME,
		'' AS INDEX_SCHEMA,
		'' AS INDEX_NAME,
		'' AS INDEX_TYPE,
		'' AS INDEX_UNIQUE,
		'' AS INDEX_COLUMNS) WHERE 1=0
		`, bigQuery,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),

		info.NewQuery(info.KindIndex, `SELECT * FROM (SELECT 
		'' AS TABLE_CATALOG,
		'' AS TABLE_SCHEMA,
		'' AS TABLE_NAME,
		'' AS INDEX_NAME,
		'' AS COLUMN_NAME,
		'' AS COLLATION,
		0 AS INDEX_POSITION)
		WHERE 1=0
`, bigQuery,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
			info.NewCriterion(info.Index, ""),
		),

		info.NewQuery(info.KindPrimaryKeys, `SELECT * FROM (
SELECT    
'' AS CONSTRAINT_NAME,  
'' AS CONSTRAINT_TYPE,
'' AS CONSTRAINT_CATALOG,
'' AS CONSTRAINT_SCHEMA,
'' AS TABLE_NAME,
'' AS COLUMN_NAME, 
'' AS  REFERENCED_TABLE_NAME,
'' AS REFERENCED_COLUMN_NAME,
'' AS  REFERENCED_TABLE_SCHEMA ) WHERE 1=0
`, bigQuery,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),

		info.NewQuery(info.KindForeignKeys, `SELECT * FROM (
SELECT 
'' AS CONSTRAINT_NAME,  
'' AS CONSTRAINT_TYPE,
'' AS CONSTRAINT_CATALOG,
'' AS CONSTRAINT_SCHEMA,
'' AS TABLE_NAME,
'' AS COLUMN_NAME, 
'' AS REFERENCED_TABLE_NAME,
'' AS REFERENCED_COLUMN_NAME,
'' AS  REFERENCED_TABLE_SCHEMA ) WHERE 1=0`, bigQuery,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),

		info.NewQuery(info.KindSession, `SELECT /*+ {"ExpandDSN": true} +*/ 
'' AS PID,
SESSION_USER() AS USER_NAME,
'$Location' AS REGION,		
'$ProjectID' AS CATALOG,
'$DatasetID' as SCHEMA_NAME,
'' AS APP_NAME 
`, bigQuery),
	)
	if err != nil {
		log.Printf("failed to register queries: %v", err)
	}
	registry.RegisterDialect(&info.Dialect{
		Product:                 bigQuery,
		Placeholder:             "?",
		Transactional:           false, //only script is transactional
		Insert:                  dialect.InsertWithMultiValues,
		Upsert:                  dialect.UpsertTypeMerge,
		Load:                    dialect.LoadTypeLocalData,
		QuoteCharacter:          '\'',
		CanAutoincrement:        false,
		CanLastInsertID:         false,
		AutoincrementFunc:       "",
		DefaultPresetIDStrategy: dialect.PresetIDStrategyUndefined,
	})
}
