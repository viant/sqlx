package pg

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
	"log"
	"strconv"
)

const product = "PostgreSQL"

var pgSQL9 = database.Product{
	Name:      product,
	DriverPkg: "pq",
	Major:     9,
}

//PqSQL9 return PostgreSQL 9.x product
func PqSQL9() *database.Product {
	return &pgSQL9
}

func init() {
	err := registry.Register(
		info.NewQuery(info.KindVersion, "SELECT version()", pgSQL9),

		info.NewQuery(info.KindSchemas, `SELECT 
CATALOG_NAME, 
SCHEMA_NAME,
COALESCE(SQL_PATH,'') AS SQL_PATH,
DEFAULT_CHARACTER_SET_NAME,
DEFAULT_COLLATION_NAME AS DEFAULT_COLLATION_NAME
FROM information_schema.schemata
`, pgSQL9,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
		),
		info.NewQuery(info.KindSchema, `SELECT 
CATALOG_NAME, 
SCHEMA_NAME,
COALESCE(SQL_PATH,'') AS SQL_PATH,
DEFAULT_CHARACTER_SET_NAME,
DEFAULT_COLLATION_NAME AS DEFAULT_COLLATION_NAME
FROM information_schema.schemata
`, pgSQL9,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
			info.NewCriterion(info.Schema, "SCHEMA_NAME"),
		),

		info.NewQuery(info.KindSchema, `SELECT 
CATALOG_NAME, 
SCHEMA_NAME,
COALESCE(SQL_PATH,'') AS SQL_PATH,
DEFAULT_CHARACTER_SET_NAME,
DEFAULT_COLLATION_NAME AS DEFAULT_COLLATION_NAME
FROM information_schema.schemata
`, pgSQL9,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
			info.NewCriterion(info.Schema, "SCHEMA_NAME"),
		),
		info.NewQuery(info.KindTables, `SELECT 
TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_TYPE,
TABLE_NAME,
AUTO_INCREMENT,
CREATE_TIME,
UPDATE_TIME,
TABLE_ROWS,
VERSION,
ENGINE
FROM INFORMATION_SCHEMA.TABLES`,
			pgSQL9,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
			info.NewCriterion(info.Schema, "SCHEMA_NAME"),
		),

		info.NewQuery(info.KindTables, `SELECT 
TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_NAME,
TABLE_TYPE,
AUTO_INCREMENT,
CREATE_TIME,
UPDATE_TIME,
TABLE_ROWS,
VERSION,
ENGINE
FROM INFORMATION_SCHEMA.TABLES`,
			pgSQL9,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
		),

		info.NewQuery(info.KindTable, `SELECT 
TABLE_CATALOG AS TABLE_CATALOG,
TABLE_SCHEMA AS TABLE_SCHEMA,
TABLE_NAME AS TABLE_NAME,
COLUMN_NAME AS COLUMN_NAME,
ORDINAL_POSITION AS ORDINAL_POSITION,
DATA_TYPE AS DATA_TYPE,
CHARACTER_MAXIMUM_LENGTH AS CHARACTER_MAXIMUM_LENGTH,
NUMERIC_PRECISION AS NUMERIC_PRECISION,
NUMERIC_SCALE AS "NUMERIC_SCALE",
IS_NULLABLE AS IS_NULLABLE,
COLUMN_DEFAULT AS COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS`,
			pgSQL9,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
		),

		info.NewQuery(info.KindSequences, `SELECT 
  t.TABLE_CATALOG AS SEQUENCE_CATALOG,
  t.TABLE_SCHEMA AS SEQUENCE_SCHEMA, 
  c.TABLE_NAME AS SEQUENCE_NAME,
  c.COLUMN_TYPE AS DATA_TYPE,
  c.MAX_VALUE,
  t.AUTO_INCREMENT AS "SEQUENCE_VALUE"
FROM 
  (SELECT 
     TABLE_SCHEMA,
     TABLE_NAME,
     COLUMN_TYPE,
     CASE 
        WHEN COLUMN_TYPE LIKE 'tinyint(1)' THEN 127
        WHEN COLUMN_TYPE LIKE 'tinyint(1) unsigned' THEN 255
        WHEN COLUMN_TYPE LIKE 'smallint(%)' THEN 32767
        WHEN COLUMN_TYPE LIKE 'smallint(%) unsigned' THEN 65535
        WHEN COLUMN_TYPE LIKE 'mediumint(%)' THEN 8388607
        WHEN COLUMN_TYPE LIKE 'mediumint(%) unsigned' THEN 16777215
        WHEN COLUMN_TYPE LIKE 'int(%)' THEN 2147483647
        WHEN COLUMN_TYPE LIKE 'int(%) unsigned' THEN 4294967295
        WHEN COLUMN_TYPE LIKE 'bigint(%)' THEN 9223372036854775807
        WHEN COLUMN_TYPE LIKE 'bigint(%) unsigned' THEN 0
        ELSE 0
     END AS "MAX_VALUE" 
   FROM 
     INFORMATION_SCHEMA.COLUMNS
     WHERE EXTRA LIKE '%auto_increment%'
   ) c
   JOIN INFORMATION_SCHEMA.TABLES t ON (t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME)
`,
			pgSQL9,
			info.NewCriterion(info.Catalog, "t.TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "t.TABLE_SCHEMA"),
			info.NewCriterion(info.Sequence, "t.TABLE_NAME"),
		),

		info.NewQuery(info.KindIndexes, `SELECT 
		TABLE_CATALOG,
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
`, pgSQL9,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
		),

		info.NewQuery(info.KindIndex, `SELECT 
		TABLE_CATALOG,
		TABLE_SCHEMA,
		TABLE_NAME,
		INDEX_NAME,
		COLUMN_NAME,
		COLLATION,
		SEQ_IN_INDEX INDEX_POSITION
FROM INFORMATION_SCHEMA.STATISTICS
`, pgSQL9,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
			info.NewCriterion(info.Table, "TABLE_NAME"),
			info.NewCriterion(info.Index, "INDEX_NAME"),
		),

		info.NewQuery(info.KindPrimaryKeys, `SELECT 
c.CONSTRAINT_NAME,  
s.CONSTRAINT_TYPE,
s.CONSTRAINT_CATALOG,
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
`, pgSQL9,
			info.NewCriterion(info.Catalog, "s.CONSTRAINT_CATALOG"),
			info.NewCriterion(info.Schema, "s.CONSTRAINT_SCHEMA"),
			info.NewCriterion(info.Table, "c.TABLE_NAME"),
		),

		info.NewQuery(info.KindForeignKeys, `SELECT 
c.CONSTRAINT_NAME,  
s.CONSTRAINT_TYPE,
s.CONSTRAINT_CATALOG,
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
`, pgSQL9,
			info.NewCriterion(info.Catalog, "s.CONSTRAINT_CATALOG"),
			info.NewCriterion(info.Schema, "s.CONSTRAINT_SCHEMA"),
			info.NewCriterion(info.Table, "c.TABLE_NAME"),
		),

		info.NewQuery(info.KindSession, `SELECT 
    CAST(pid AS varchar) AS PID,
	datname AS CATALOG_NAME,
	usename AS USER_NAME, 
	application_name AS APP_NAME,
	'' AS SCHEMA_NAME
FROM pg_stat_activity
WHERE pid=pg_backend_pid() LIMIT 1;
`, pgSQL9),

		info.NewQuery(info.KindForeignKeysCheckOn, `SET FOREIGN_KEY_CHECKS=1`,
			pgSQL9,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),

		info.NewQuery(info.KindForeignKeysCheckOff, `SET FOREIGN_KEY_CHECKS=1`,
			pgSQL9,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),
	)
	if err != nil {
		log.Printf("failed to register queries: %v", err)
	}

	registry.RegisterDialect(&info.Dialect{
		Product:          pgSQL9,
		Placeholder:      "$",
		Transactional:    true,
		Insert:           dialect.InsertWithMultiValues,
		Upsert:           dialect.UpsertTypeMergeInto,
		Load:             dialect.LoadTypeUnsupported,
		CanAutoincrement: true,
		CanLastInsertID:  false,
		CanReturning:     true,
		QuoteCharacter:   '\'', // 39 is single quote '
		PlaceholderResolver: func() func() string {
			counter := 0
			return func() string {
				counter++
				return "$" + strconv.Itoa(counter)
			}
		},
		AutoincrementFunc: "nextval",
	})

}
