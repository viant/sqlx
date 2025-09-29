package oracle

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
	"log"
)

const product = "Oracle"

var oracleProduct = database.Product{
	Name: product,
}

// Oracle returns Oracle product
func Oracle() *database.Product { return &oracleProduct }

func init() {
	err := registry.Register(
		// Version
		info.NewQuery(info.KindVersion, "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%' AND ROWNUM = 1", oracleProduct),

		// Catalogs (not applicable in Oracle)
		info.NewQuery(info.KindCatalogs, "SELECT '' AS CATALOG_NAME FROM DUAL", oracleProduct),
		info.NewQuery(info.KindCatalog, "SELECT '' AS CATALOG_NAME FROM DUAL", oracleProduct,
			info.NewCriterion(info.Catalog, "")),

		// Current schema
		info.NewQuery(info.KindCurrentSchema, `SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') AS SCHEMA_NAME`, oracleProduct),

		// Schemas
		info.NewQuery(info.KindSchemas, `SELECT 
'' AS CATALOG_NAME,
USERNAME AS SCHEMA_NAME,
'' AS SQL_PATH,
'' AS DEFAULT_CHARACTER_SET_NAME,
'' AS DEFAULT_COLLATION_NAME,
ROWNUM AS SCHEMA_POS,
'' AS REGION
FROM ALL_USERS`, oracleProduct,
			info.NewCriterion(info.Catalog, ""),
		),

		info.NewQuery(info.KindSchema, `SELECT 
'' AS CATALOG_NAME,
USERNAME AS SCHEMA_NAME,
'' AS SQL_PATH,
'' AS DEFAULT_CHARACTER_SET_NAME,
'' AS DEFAULT_COLLATION_NAME,
ROWNUM AS SCHEMA_POS,
'' AS REGION
FROM ALL_USERS`, oracleProduct,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "SCHEMA_NAME"),
		),

		// Tables
		info.NewQuery(info.KindTables, `SELECT 
'' AS TABLE_CATALOG,
T.OWNER AS TABLE_SCHEMA,
T.TABLE_NAME,
COALESCE(C.COMMENTS,'') AS TABLE_COMMENT,
'BASE TABLE' AS TABLE_TYPE,
'' AS AUTO_INCREMENT,
TO_CHAR(O.CREATED, 'YYYY-MM-DD HH24:MI:SS') AS CREATE_TIME,
TO_CHAR(O.LAST_DDL_TIME, 'YYYY-MM-DD HH24:MI:SS') AS UPDATE_TIME,
COALESCE(T.NUM_ROWS, 0) AS TABLE_ROWS,
'' AS VERSION,
'' AS ENGINE,
'' AS DDL
FROM ALL_TABLES T
JOIN ALL_OBJECTS O ON O.OWNER = T.OWNER AND O.OBJECT_NAME = T.TABLE_NAME AND O.OBJECT_TYPE = 'TABLE'
LEFT JOIN ALL_TAB_COMMENTS C ON C.OWNER = T.OWNER AND C.TABLE_NAME = T.TABLE_NAME`,
			oracleProduct,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "T.OWNER"),
		),

		// Table columns
		info.NewQuery(info.KindTable, `SELECT 
'' AS TABLE_CATALOG,
C.OWNER AS TABLE_SCHEMA,
C.TABLE_NAME,
C.COLUMN_NAME,
C.COLUMN_ID AS ORDINAL_POSITION,
COALESCE(CC.COMMENTS, '') AS COLUMN_COMMENT,
C.DATA_TYPE,
C.DATA_LENGTH AS CHARACTER_MAXIMUM_LENGTH,
C.DATA_PRECISION AS NUMERIC_PRECISION,
C.DATA_SCALE AS NUMERIC_SCALE,
C.NULLABLE AS IS_NULLABLE,
C.DATA_DEFAULT AS COLUMN_DEFAULT,
CASE WHEN PKC.COLUMN_NAME IS NOT NULL THEN 'PRI' ELSE '' END AS COLUMN_KEY,
'' AS DESCENDING,
COALESCE(PKC.CONSTRAINT_NAME, '') AS INDEX_NAME,
COALESCE(PKC.POSITION, 0) AS INDEX_POSITION,
CAST(NULL AS VARCHAR2(1)) AS COLLATION
FROM ALL_TAB_COLUMNS C
LEFT JOIN ALL_COL_COMMENTS CC ON CC.OWNER = C.OWNER AND CC.TABLE_NAME = C.TABLE_NAME AND CC.COLUMN_NAME = C.COLUMN_NAME
LEFT JOIN (
    SELECT AC.OWNER, ACC.TABLE_NAME, ACC.COLUMN_NAME, ACC.POSITION, AC.CONSTRAINT_NAME
    FROM ALL_CONSTRAINTS AC
    JOIN ALL_CONS_COLUMNS ACC ON ACC.OWNER = AC.OWNER AND ACC.CONSTRAINT_NAME = AC.CONSTRAINT_NAME
    WHERE AC.CONSTRAINT_TYPE = 'P'
) PKC ON PKC.OWNER = C.OWNER AND PKC.TABLE_NAME = C.TABLE_NAME AND PKC.COLUMN_NAME = C.COLUMN_NAME`,
			oracleProduct,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "C.OWNER"),
			info.NewCriterion(info.Table, "C.TABLE_NAME"),
		),

		// Sequences
		info.NewQuery(info.KindSequences, `SELECT 
'' AS SEQUENCE_CATALOG,
S.SEQUENCE_OWNER AS SEQUENCE_SCHEMA,
S.SEQUENCE_NAME,
S.LAST_NUMBER AS SEQUENCE_VALUE,
S.INCREMENT_BY,
'NUMBER' AS DATA_TYPE,
S.MIN_VALUE AS START_VALUE,
S.MAX_VALUE
FROM ALL_SEQUENCES S`,
			oracleProduct,
			info.NewCriterion(info.Catalog, "SEQUENCE_CATALOG"),
			info.NewCriterion(info.Schema, "S.SEQUENCE_OWNER"),
			info.NewCriterion(info.Sequence, "S.SEQUENCE_NAME"),
		),

		// Indexes (aggregated)
		info.NewQuery(info.KindIndexes, `SELECT 
'' AS TABLE_CATALOG,
I.TABLE_OWNER AS TABLE_SCHEMA,
I.TABLE_NAME,
I.OWNER AS INDEX_SCHEMA,
I.INDEX_NAME,
I.INDEX_TYPE,
CASE WHEN I.UNIQUENESS = 'UNIQUE' THEN 1 ELSE 0 END AS INDEX_UNIQUE,
LISTAGG(IC.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY IC.COLUMN_POSITION) AS INDEX_COLUMNS
FROM ALL_INDEXES I
JOIN ALL_IND_COLUMNS IC ON IC.INDEX_OWNER = I.OWNER AND IC.INDEX_NAME = I.INDEX_NAME
$WHERE
GROUP BY 1,2,3,4,5,6,7`,
			oracleProduct,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "I.TABLE_OWNER"),
			info.NewCriterion(info.Table, "I.TABLE_NAME"),
		),

		// Index details
		info.NewQuery(info.KindIndex, `SELECT 
'' AS TABLE_CATALOG,
I.TABLE_OWNER AS TABLE_SCHEMA,
I.TABLE_NAME,
I.INDEX_NAME,
IC.COLUMN_NAME,
IC.DESCEND AS COLLATION,
IC.COLUMN_POSITION AS INDEX_POSITION
FROM ALL_INDEXES I
JOIN ALL_IND_COLUMNS IC ON IC.INDEX_OWNER = I.OWNER AND IC.INDEX_NAME = I.INDEX_NAME`,
			oracleProduct,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "I.TABLE_OWNER"),
			info.NewCriterion(info.Table, "I.TABLE_NAME"),
			info.NewCriterion(info.Index, "I.INDEX_NAME"),
		),

		// Primary keys
		info.NewQuery(info.KindPrimaryKeys, `SELECT 
AC.CONSTRAINT_NAME,
AC.CONSTRAINT_TYPE,
'' AS CONSTRAINT_CATALOG,
AC.OWNER AS CONSTRAINT_SCHEMA,
ACC.TABLE_NAME,
ACC.POSITION AS ORDINAL_POSITION,
ACC.COLUMN_NAME,
'' AS REFERENCED_TABLE_NAME,
'' AS REFERENCED_COLUMN_NAME,
'' AS REFERENCED_TABLE_SCHEMA,
0 AS POSITION_IN_UNIQUE_CONSTRAINT,
'' AS ON_UPDATE,
'' AS ON_DELETE,
'' AS ON_MATCH
FROM ALL_CONSTRAINTS AC
JOIN ALL_CONS_COLUMNS ACC ON ACC.OWNER = AC.OWNER AND ACC.CONSTRAINT_NAME = AC.CONSTRAINT_NAME
WHERE AC.CONSTRAINT_TYPE = 'P'`,
			oracleProduct,
			info.NewCriterion(info.Catalog, "CONSTRAINT_CATALOG"),
			info.NewCriterion(info.Schema, "AC.OWNER"),
			info.NewCriterion(info.Table, "ACC.TABLE_NAME"),
		),

		// Foreign keys
		info.NewQuery(info.KindForeignKeys, `SELECT 
FK.CONSTRAINT_NAME,
FK.CONSTRAINT_TYPE,
'' AS CONSTRAINT_CATALOG,
FK.OWNER AS CONSTRAINT_SCHEMA,
FK.TABLE_NAME,
FKC.POSITION AS ORDINAL_POSITION,
FKC.COLUMN_NAME,
PK.TABLE_NAME AS REFERENCED_TABLE_NAME,
PKC.COLUMN_NAME AS REFERENCED_COLUMN_NAME,
PK.OWNER AS REFERENCED_TABLE_SCHEMA,
0 AS POSITION_IN_UNIQUE_CONSTRAINT,
'' AS ON_UPDATE,
FK.DELETE_RULE AS ON_DELETE,
FK.DEFERRABLE AS ON_MATCH
FROM ALL_CONSTRAINTS FK
JOIN ALL_CONS_COLUMNS FKC ON FKC.OWNER = FK.OWNER AND FKC.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
JOIN ALL_CONSTRAINTS PK ON PK.CONSTRAINT_NAME = FK.R_CONSTRAINT_NAME AND PK.OWNER = FK.R_OWNER
JOIN ALL_CONS_COLUMNS PKC ON PKC.OWNER = PK.OWNER AND PKC.CONSTRAINT_NAME = PK.CONSTRAINT_NAME AND PKC.POSITION = FKC.POSITION
WHERE FK.CONSTRAINT_TYPE = 'R'`,
			oracleProduct,
			info.NewCriterion(info.Catalog, "CONSTRAINT_CATALOG"),
			info.NewCriterion(info.Schema, "FK.OWNER"),
			info.NewCriterion(info.Table, "FK.TABLE_NAME"),
		),

		// Session info
		info.NewQuery(info.KindSession, `SELECT 
TO_CHAR(SYS_CONTEXT('USERENV','SID')) AS PID,
USER AS USER_NAME,
'' AS CATALOG,
SYS_CONTEXT('USERENV','CURRENT_SCHEMA') AS SCHEMA_NAME,
NVL(SYS_CONTEXT('USERENV','MODULE'), '') AS APP_NAME
FROM DUAL`, oracleProduct),

		// Foreign key checks not supported globally in Oracle; no-ops
		info.NewQuery(info.KindForeignKeysCheckOn, `SELECT 1 FROM DUAL`,
			oracleProduct,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),
		info.NewQuery(info.KindForeignKeysCheckOff, `SELECT 1 FROM DUAL`,
			oracleProduct,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),
	)

	if err != nil {
		log.Printf("failed to register queries: %v", err)
	}

	registry.RegisterDialect(&info.Dialect{
		Product:                 oracleProduct,
		Placeholder:             "?",
		Transactional:           true,
		Insert:                  dialect.InsertWithSingleValues,
		Upsert:                  dialect.UpsertTypeMergeInto,
		Load:                    dialect.LoadTypeUnsupported,
		QuoteCharacter:          '\'',
		CanAutoincrement:        false,
		CanLastInsertID:         false,
		DefaultPresetIDStrategy: dialect.PresetIDStrategyUndefined,
	})
}
