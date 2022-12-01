package vertica

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
	"log"
)

const product = "Vertica Analytic Database"
const driver = "Driver"
const driverPkg = "vertigo"

var vertica = database.Product{
	Name:      product,
	DriverPkg: driverPkg,
	Driver:    driver,
	//Major:     int,
	//Minor:     int,
	//Release:   int
}

//Vertica return Vertica product
func Vertica() *database.Product {
	return &vertica
}

func init() {
	err := registry.Register(
		info.NewQuery(info.KindVersion, "SELECT VERSION()", vertica),

		info.NewQuery(info.KindCatalogs, "SELECT '' AS CATALOG_NAME FROM DUAL", vertica),

		info.NewQuery(info.KindCatalog, "SELECT '' AS CATALOG_NAME FROM DUAL",
			vertica,
			info.NewCriterion(info.Catalog, "CATALOG_NAME")),

		// Impossible to use "SELECT CURRENT_SCHEMA" with regular SQL
		info.NewQuery(info.KindCurrentSchema, `SELECT CURRENT_SCHEMA AS SCHEMA_NAME`, vertica),

		info.NewQuery(info.KindSchemas, `SELECT DISTINCT
'' CATALOG_NAME,
SCHEMA_NAME,
'utf8' DEFAULT_CHARACTER_SET_NAME,
'en_US@collation=binary' DEFAULT_COLLATION_NAME,
'' SQL_PATH,
0 SCHEMA_POS,
'' REGION
FROM V_CATALOG.SCHEMATA`,
			vertica,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
		),

		info.NewQuery(info.KindSchema, `SELECT
'' CATALOG_NAME,
SCHEMA_NAME,
'utf8' DEFAULT_CHARACTER_SET_NAME,
'en_US@collation=binary' DEFAULT_COLLATION_NAME,
'' SQL_PATH,
0 SCHEMA_POS,
'' REGION
FROM V_CATALOG.SCHEMATA`,
			vertica,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
			info.NewCriterion(info.Schema, "SCHEMA_NAME"),
		),

		info.NewQuery(info.KindTables, `SELECT
'' TABLE_CATALOG,
T.TABLE_SCHEMA,
T.TABLE_NAME,
COALESCE(CS.COMMENT, '') TABLE_COMMENT,
'' TABLE_TYPE,
COALESCE(CL.IS_IDENTITY, false) AUTO_INCREMENT,
T.CREATE_TIME,
'' UPDATE_TIME,
0 AS TABLE_ROWS,
'' VERSION,
'' ENGINE,
'' DDL
FROM V_CATALOG.TABLES T
LEFT JOIN V_CATALOG.COMMENTS CS ON T.TABLE_ID = CS.OBJECT_ID AND CS.OBJECT_TYPE = 'TABLE'
LEFT JOIN V_CATALOG.COLUMNS CL ON T.TABLE_ID = CL.TABLE_ID AND CL.IS_IDENTITY = true`,
			vertica,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "T.TABLE_SCHEMA"),
		),

		info.NewQuery(info.KindTable, `SELECT
'' AS TABLE_CATALOG,
CL.TABLE_SCHEMA,
CL.TABLE_NAME,
CL.COLUMN_NAME,
CL.ORDINAL_POSITION,
COALESCE(CS.COMMENT, '') COLUMN_COMMENT,
CL.DATA_TYPE,
CL.CHARACTER_MAXIMUM_LENGTH,
CL.NUMERIC_PRECISION,
CL.NUMERIC_SCALE,
CL.IS_NULLABLE,
CL.COLUMN_DEFAULT,
CASE WHEN COALESCE(CC.CONSTRAINT_NAME, '~') = 'C_PRIMARY' THEN 'PRI' ELSE '' END COLUMN_KEY,
'' DESCENDING,
'' INDEX_NAME,
0 INDEX_POSITION,
NULL COLLATION
FROM V_CATALOG.COLUMNS CL
LEFT JOIN V_CATALOG.COMMENTS CS ON CL.TABLE_ID = CS.OBJECT_ID AND CL.COLUMN_NAME = CS.CHILD_OBJECT
LEFT JOIN V_CATALOG.CONSTRAINT_COLUMNS CC ON CL.TABLE_ID = CC.TABLE_ID AND CL.COLUMN_NAME = CC.COLUMN_NAME AND CC.CONSTRAINT_NAME = 'C_PRIMARY'`,
			vertica,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "CL.TABLE_SCHEMA"),
			info.NewCriterion(info.Table, "CL.TABLE_NAME"),
		),

		info.NewQuery(info.KindViews, `SELECT
'' TABLE_CATALOG,
T.TABLE_SCHEMA,
T.TABLE_NAME,
COALESCE(CS.COMMENT, '') TABLE_COMMENT,
'' TABLE_TYPE,
'' AUTO_INCREMENT,
T.CREATE_TIME,
'' UPDATE_TIME,
0 AS TABLE_ROWS,
'' VERSION,
'' ENGINE,
'' DDL
FROM V_CATALOG.VIEWS T
LEFT JOIN V_CATALOG.COMMENTS CS ON T.TABLE_ID = CS.OBJECT_ID AND CS.OBJECT_TYPE = 'VIEW'`,
			vertica,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "T.TABLE_SCHEMA"),
		),

		info.NewQuery(info.KindView, `SELECT
'' AS TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME,
ORDINAL_POSITION,
'' COLUMN_COMMENT,
DATA_TYPE,
CHARACTER_MAXIMUM_LENGTH,
NUMERIC_PRECISION,
NUMERIC_SCALE,
'' IS_NULLABLE,
'' COLUMN_DEFAULT,
'' COLUMN_KEY,
'' DESCENDING,
'' INDEX_NAME,
0 INDEX_POSITION,
NULL COLLATION
FROM V_CATALOG.VIEW_COLUMNS`,
			vertica,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
			info.NewCriterion(info.View, "TABLE_NAME"),
		),

		info.NewQuery(info.KindPrimaryKeys, `SELECT 
CONSTRAINT_NAME,  
CONSTRAINT_TYPE,
'' CONSTRAINT_CATALOG,
TABLE_SCHEMA CONSTRAINT_SCHEMA,
TABLE_NAME,
ORDINAL_POSITION,
COLUMN_NAME,
'' REFERENCED_TABLE_NAME,
'' REFERENCED_COLUMN_NAME,
'' REFERENCED_TABLE_SCHEMA,
0 POSITION_IN_UNIQUE_CONSTRAINT,
'' ON_UPDATE,
'' ON_DELETE,
'' ON_MATCH
FROM V_CATALOG.PRIMARY_KEYS`,
			vertica,
			info.NewCriterion(info.Catalog, "CONSTRAINT_CATALOG"),
			info.NewCriterion(info.Schema, "CONSTRAINT_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
		),

		info.NewQuery(info.KindForeignKeys, `SELECT 
CONSTRAINT_NAME,  
CONSTRAINT_TYPE,
'' CONSTRAINT_CATALOG,
TABLE_SCHEMA AS CONSTRAINT_SCHEMA,
TABLE_NAME,
ORDINAL_POSITION,
COLUMN_NAME, 
REFERENCE_TABLE_NAME REFERENCED_TABLE_NAME,
REFERENCE_COLUMN_NAME REFERENCED_COLUMN_NAME,
REFERENCE_TABLE_SCHEMA REFERENCED_TABLE_SCHEMA,
0 POSITION_IN_UNIQUE_CONSTRAINT,
'' ON_UPDATE,
'' ON_DELETE,
'' ON_MATCH
FROM V_CATALOG.FOREIGN_KEYS`,
			vertica,
			info.NewCriterion(info.Catalog, "CONSTRAINT_CATALOG"),
			info.NewCriterion(info.Schema, "CONSTRAINT_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
		),

		info.NewQuery(info.KindConstraints, `SELECT 
CONSTRAINT_NAME,  
CONSTRAINT_TYPE,
'' CONSTRAINT_CATALOG,
TABLE_SCHEMA CONSTRAINT_SCHEMA,
TABLE_NAME,
0 ORDINAL_POSITION,
COLUMN_NAME, 
COALESCE(REFERENCE_TABLE_NAME, '') REFERENCED_TABLE_NAME,
COALESCE(REFERENCE_COLUMN_NAME, '') REFERENCED_COLUMN_NAME,
COALESCE(REFERENCE_TABLE_SCHEMA, '') REFERENCED_TABLE_SCHEMA,
0 POSITION_IN_UNIQUE_CONSTRAINT,
'' ON_UPDATE,
'' ON_DELETE,
'' ON_MATCH
FROM  V_CATALOG.CONSTRAINT_COLUMNS`,
			vertica,
			info.NewCriterion(info.Catalog, "CONSTRAINT_CATALOG"),
			info.NewCriterion(info.Schema, "CONSTRAINT_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
		),

		// Indexes don't exist in Vertica
		info.NewQuery(info.KindIndexes, `SELECT
'' TABLE_CATALOG,
'' TABLE_NAME,
'' INDEX_TYPE,
'' TABLE_SCHEMA,
'' INDEX_SCHEMA,
0 INDEX_POSITION,
'' INDEX_NAME,
'' INDEX_UNIQUE,
'' INDEX_COLUMNS,
'' INDEX_ORIGIN,
'' INDEX_PARTIAL
FROM DUAL
WHERE 1=0`,
			vertica,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "INDEX_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
		),

		// Indexes don't exist in Vertica
		info.NewQuery(info.KindIndex, `SELECT
'' TABLE_CATALOG,
'' TABLE_NAME,
'' INDEX_TYPE,
'' TABLE_SCHEMA,
'' INDEX_SCHEMA,
0 INDEX_POSITION,
'' INDEX_NAME,
'' INDEX_UNIQUE,
'' INDEX_COLUMNS,
'' INDEX_ORIGIN,
'' INDEX_PARTIAL
FROM DUAL
WHERE 1=0`,
			vertica,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "INDEX_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
			info.NewCriterion(info.Index, "INDEX_NAME"),
		),

		info.NewQuery(info.KindSequences, `SELECT
'' SEQUENCE_CATALOG,
S.SEQUENCE_SCHEMA,
S.SEQUENCE_NAME,
S.CURRENT_VALUE SEQUENCE_VALUE,
CASE WHEN S.IDENTITY_TABLE_ID = 0 THEN 'int' ELSE C.DATA_TYPE END DATA_TYPE,
S.MINIMUM START_VALUE,
S.MAXIMUM MAX_VALUE
FROM V_CATALOG.SEQUENCES S
LEFT JOIN V_CATALOG.COLUMNS C 
ON S.IDENTITY_TABLE_ID = C.TABLE_ID AND C.IS_IDENTITY = 't'`,
			vertica,
			info.NewCriterion(info.Catalog, "SEQUENCE_CATALOG"),
			info.NewCriterion(info.Schema, "SEQUENCE_SCHEMA"),
			info.NewCriterion(info.Sequence, "SEQUENCE_NAME"),
		),

		info.NewQuery(info.KindFunctions, `SELECT
'' ROUTINE_CATALOG,
SCHEMA_NAME ROUTINE_SCHEMA,
FUNCTION_NAME ROUTINE_NAME,
FUNCTION_DEFINITION ROUTINE_BODY,
FUNCTION_RETURN_TYPE DATA_TYPE,
'' ROUTINE_TYPE, 
'utf8' CHARACTER_SET_NAME,
CASE WHEN VOLATILITY = 'immutable' THEN 'YES' ELSE 'NO' END IS_DETERMINISTIC
FROM V_CATALOG.USER_FUNCTIONS`,
			vertica,
			info.NewCriterion(info.Catalog, "ROUTINE_CATALOG"),
			info.NewCriterion(info.Schema, "ROUTINE_SCHEMA"),
			info.NewCriterion(info.Function, "ROUTINE_NAME"),
		),

		info.NewQuery(info.KindSession,
			`SELECT
SESSION_ID PID,
USER_NAME USER_NAME,	
'' REGION,
'' CATALOG_NAME,
'' SCHEMA_NAME,
COALESCE(CLIENT_LABEL,'') || CASE WHEN LENGTH(COALESCE(CLIENT_LABEL,'')) > 0 AND LENGTH(COALESCE(CLIENT_TYPE,'')) > 0 THEN ' - ' ELSE '' END || COALESCE(CLIENT_TYPE, '') APP_NAME
FROM V_MONITOR.SESSIONS`,
			vertica),

		//It's impossible to disable/enable all FK on Vertica with one command
		//KindForeignKeysCheckOn
		//KindForeignKeysCheckOff

	)
	if err != nil {
		log.Printf("failed to register queries: %v", err)
	}

	registry.RegisterDialect(&info.Dialect{
		Product:                 vertica,
		Placeholder:             ":", // "@" or ":" for backward compatibility
		Transactional:           true,
		Insert:                  dialect.InsertWithMultiValues,
		Upsert:                  dialect.UpsertTypeMergeInto,
		Load:                    dialect.LoadTypeLocalData,
		QuoteCharacter:          '\'',
		CanAutoincrement:        true,
		CanLastInsertID:         true, // LAST_INSERT_ID works only with AUTO_INCREMENT and IDENTITY columns
		AutoincrementFunc:       "nextval",
		DefaultPresetIDStrategy: dialect.PresetIDStrategyUndefined,
	})
}
