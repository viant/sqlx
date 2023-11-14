package mysql

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/product/mysql/sequence"
	"github.com/viant/sqlx/metadata/registry"
	"log"
)

const product = "MySQL"

var mySQL5 = database.Product{
	Name:  product,
	Major: 5,
}

var mySQL57 = database.Product{
	Name:  product,
	Major: 5,
	Minor: 7,
}

// MySQL5 return MySQL 5.x product
func MySQL5() *database.Product {
	return &mySQL5
}

func init() {
	err := registry.Register(
		info.NewQuery(info.KindVersion, "SELECT CONCAT('MySQL - ', VERSION())", mySQL5),

		info.NewQuery(info.KindSchemas, `SELECT 
'' CATALOG_NAME, 
SCHEMA_NAME,
COALESCE(SQL_PATH,'') AS SQL_PATH,
DEFAULT_CHARACTER_SET_NAME,
DEFAULT_COLLATION_NAME AS DEFAULT_COLLATION_NAME
FROM information_schema.schemata
`, mySQL5,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
		),
		info.NewQuery(info.KindSchema, `SELECT 
'' CATALOG_NAME, 
SCHEMA_NAME,
COALESCE(SQL_PATH,'') AS SQL_PATH,
DEFAULT_CHARACTER_SET_NAME,
DEFAULT_COLLATION_NAME AS DEFAULT_COLLATION_NAME
FROM information_schema.schemata
`, mySQL5,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
			info.NewCriterion(info.Schema, "SCHEMA_NAME"),
		),
		info.NewQuery(info.KindTables, `SELECT 
'' TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_NAME,
TABLE_TYPE,
COALESCE(AUTO_INCREMENT, '') AS AUTO_INCREMENT,
CREATE_TIME,
UPDATE_TIME,
TABLE_ROWS,
VERSION,
ENGINE
FROM INFORMATION_SCHEMA.TABLES`,
			mySQL5,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
		),

		info.NewQuery(info.KindTable, `SELECT 
'' TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME,
ORDINAL_POSITION,
COLUMN_COMMENT,
DATA_TYPE,
CHARACTER_MAXIMUM_LENGTH,
NUMERIC_PRECISION,
NUMERIC_SCALE,
IS_NULLABLE,
COLUMN_DEFAULT,
COLUMN_KEY,
CASE WHEN COALESCE(EXTRA, "") LIKE '%auto_increment%' THEN 1 ELSE NULL END IS_AUTOINCREMENT
FROM INFORMATION_SCHEMA.COLUMNS`,
			mySQL5,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
		),

		info.NewQuery(info.KindSequences, `SELECT 
  '' SEQUENCE_CATALOG,
  '$Args[1]' AS SEQUENCE_SCHEMA, 
  '$Args[2]'  AS SEQUENCE_NAME,
  0 AS SEQUENCE_VALUE,
  COALESCE(@@SESSION.auto_increment_increment, 0) INCREMENT_BY,
  'int' AS DATA_TYPE,
  COALESCE(@@SESSION.auto_increment_offset, 0) START_VALUE,
  9223372036854775807 AS MAX_VALUE
`,
			mySQL5,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Sequence, ""),
		).OnPost(info.NewHandler(sequence.UpdateMySQLSequence)),

		info.NewQuery(info.KindIndexes, `SELECT 
		'' TABLE_CATALOG,
		TABLE_SCHEMA,
		TABLE_NAME,
		INDEX_SCHEMA,
		INDEX_NAME,
		INDEX_TYPE,
		CASE WHEN NON_UNIQUE = 1 THEN 1 ELSE 0 END AS INDEX_UNIQUE,
		GROUP_CONCAT(COLUMN_NAME) AS INDEX_COLUMNS
FROM INFORMATION_SCHEMA.STATISTICS
$WHERE
GROUP BY 1, 2, 3, 4, 5, 6, 7
`, mySQL5,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
		),

		info.NewQuery(info.KindIndex, `SELECT 
		'' TABLE_CATALOG,
		TABLE_SCHEMA,
		TABLE_NAME,
		INDEX_NAME,
		COLUMN_NAME,
		COLLATION,
		SEQ_IN_INDEX INDEX_POSITION
FROM INFORMATION_SCHEMA.STATISTICS
`, mySQL5,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
			info.NewCriterion(info.Index, "INDEX_NAME"),
		),

		info.NewQuery(info.KindPrimaryKeys, `SELECT 
c.CONSTRAINT_NAME,  
s.CONSTRAINT_TYPE,
'' CONSTRAINT_CATALOG,
s.CONSTRAINT_SCHEMA,
c.TABLE_NAME,
c.COLUMN_NAME, 
COALESCE(c.REFERENCED_TABLE_NAME, '') AS REFERENCED_TABLE_NAME,
COALESCE(c.REFERENCED_COLUMN_NAME, '')REFERENCED_COLUMN_NAME,
CASE WHEN c.REFERENCED_TABLE_NAME IS NOT NULL THEN s.CONSTRAINT_SCHEMA ELSE '' END AS REFERENCED_TABLE_SCHEMA
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS s
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON c.CONSTRAINT_NAME = s.CONSTRAINT_NAME
	 AND c.CONSTRAINT_CATALOG = s.CONSTRAINT_CATALOG
	 AND c.CONSTRAINT_SCHEMA = s.CONSTRAINT_SCHEMA	 
	 AND c.TABLE_NAME = s.TABLE_NAME
WHERE  s.CONSTRAINT_TYPE = 'PRIMARY KEY'
`, mySQL5,
			info.NewCriterion(info.Catalog, "s.CONSTRAINT_CATALOG"),
			info.NewCriterion(info.Schema, "s.CONSTRAINT_SCHEMA"),
			info.NewCriterion(info.Table, "c.TABLE_NAME"),
		),

		// TODO A BUG IN LINE s.CONSTRAINT_SCHEMA AS REFERENCED_TABLE_SCHEMA
		info.NewQuery(info.KindForeignKeys, `SELECT 
c.CONSTRAINT_NAME,  
s.CONSTRAINT_TYPE,
'' CONSTRAINT_CATALOG,
s.CONSTRAINT_SCHEMA,
c.TABLE_NAME,
c.COLUMN_NAME, 
c.REFERENCED_TABLE_NAME,
c.REFERENCED_COLUMN_NAME,
s.CONSTRAINT_SCHEMA AS REFERENCED_TABLE_SCHEMA
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS s
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON c.CONSTRAINT_NAME = s.CONSTRAINT_NAME
	 AND c.CONSTRAINT_CATALOG = s.CONSTRAINT_CATALOG
	 AND c.CONSTRAINT_SCHEMA = s.CONSTRAINT_SCHEMA	 
	 AND c.TABLE_NAME = s.TABLE_NAME
WHERE s.CONSTRAINT_TYPE = 'FOREIGN KEY'
`, mySQL5,
			info.NewCriterion(info.Catalog, "s.CONSTRAINT_CATALOG"),
			info.NewCriterion(info.Schema, "s.CONSTRAINT_SCHEMA"),
			info.NewCriterion(info.Table, "c.TABLE_NAME"),
		),

		info.NewQuery(info.KindSession, `SELECT 
CAST(ID AS CHAR) AS PID,
CAST(USER AS CHAR) AS USER_NAME,
"" AS CATALOG,
CAST(DB as CHAR) as SCHEMA_NAME,
"" AS APP_NAME 
from information_schema.processlist 
where ID=CONNECTION_ID() LIMIT 1;
`, mySQL5),
		info.NewQuery(info.KindForeignKeysCheckOn, `SET FOREIGN_KEY_CHECKS=1`,
			mySQL5,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),

		info.NewQuery(info.KindForeignKeysCheckOff, `SET FOREIGN_KEY_CHECKS=0`,
			mySQL5,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),

		info.NewQuery(info.KindSequenceNextValue, `SELECT 1`,
			mySQL5,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Object, ""),
			info.NewCriterion(info.SequenceNewCurrentValue, ""),
		).OnPre(&sequence.Transient{}, &sequence.Udf{}),

		info.NewQuery(info.KindLockGet, `SELECT '$Args[0]' AS LOCK_CATALOG,
'$Args[1]' AS LOCK_SCHEMA,
'$Args[2]' AS LOCK_TABLE,
'$Args[0].$Args[1].$Args[2]' AS LOCK_NAME,
GET_LOCK('$Args[0].$Args[1].$Args[2]',10) AS SUCCESS`,
			mySQL5,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),

		info.NewQuery(info.KindLockRelease, `SELECT '$Args[0]' AS LOCK_CATALOG,
'$Args[1]' AS LOCK_SCHEMA,
'$Args[2]' AS LOCK_TABLE,
'$Args[0].$Args[1].$Args[2]' AS LOCK_NAME,
RELEASE_LOCK('$Args[0].$Args[1].$Args[2]')  AS SUCCESS`,
			mySQL5,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),
	)

	if err != nil {
		log.Printf("failed to register queries: %v", err)
	}

	registry.RegisterDialect(&info.Dialect{
		Product:                   mySQL5,
		Placeholder:               "?",
		Transactional:             true,
		Insert:                    dialect.InsertWithMultiValues,
		Upsert:                    dialect.UpsertTypeInsertOrUpdate,
		Load:                      dialect.LoadTypeLocalData,
		SpecialKeywordEscapeQuote: '`',
		QuoteCharacter:            '\'',
		CanAutoincrement:          true,
		CanLastInsertID:           true, // in reality true but multi-insert gives us the id from the first row, not the last one
		// TODO: provide real autoincrement function
		AutoincrementFunc:       "autoincrement",
		DefaultPresetIDStrategy: dialect.PresetIDWithTransientTransaction,
	})

}
