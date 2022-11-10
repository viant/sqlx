package sqlite

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/product/sqlite/sequence"
	"github.com/viant/sqlx/metadata/registry"
	"log"
)

const product = "SQLite"

var sqLite3 = database.Product{
	Name:      product,
	Major:     3,
	DriverPkg: "sqlite3",
	Driver:    "SQLiteDriver",
}

var sqLite333 = database.Product{
	Name:      product,
	Major:     3,
	Minor:     33,
	DriverPkg: "sqlite3",
	Driver:    "SQLiteDriver",
}

//SQLite3 return SQLite3 product
func SQLite3() *database.Product {
	return &sqLite3
}

func SQLiteMaj3Min33() *database.Product {
	return &sqLite333
}

func init() {
	registerProduct(sqLite333, "sqlite_master")
	registerProduct(sqLite3, "sqlite_schema")
}

func registerProduct(product database.Product, schemaTable string) {
	err := registry.Register(
		info.NewQuery(info.KindVersion, "SELECT 'SQLite - ' || sqlite_version()", product),
		info.NewQuery(info.KindSchemas, `SELECT 
	name AS SCHEMA_NAME,
	seq AS SCHEMA_POS,
	file AS SCHEMA_FILE
FROM pragma_database_list`, product,
			info.NewCriterion(info.Catalog, ""),
		),
		info.NewQuery(info.KindSchema, "SELECT name FROM pragma_database_list", product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, "name"),
		),
		info.NewQuery(info.KindTables, `SELECT 
type AS TABLE_TYPE,
name AS TABLE_NAME,
sql 
FROM `+schemaTable+` WHERE type='table' AND name NOT IN('sqlite_sequence')`, product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
		),
		info.NewQuery(info.KindTable, "SELECT CASE WHEN t.`notnull` = 0 THEN '1' ELSE '0' END AS IS_NULLABLE,\n"+`
  m.name AS TABLE_NAME,
  t.name AS COLUMN_NAME,	
  t.cid AS ORDINAL_POSITION, 
  t.type AS DATA_TYPE, 
  COALESCE(t.dflt_value,'') AS COLUMN_DEFAULT, 
  CASE WHEN t.pk = 1 THEN 'PRI' ELSE '' END AS COLUMN_KEY
FROM `+schemaTable+` AS m,
pragma_table_info(m.name) AS t
`, product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, "m.name"),
		),
		info.NewQuery(info.KindIndexes, "SELECT t.`unique` AS INDEX_UNIQUE,\n"+`
m.name AS TABLE_NAME,
t.seq AS INDEX_POSITION,
t.name AS INDEX_NAME,
t.origin AS INDEX_ORIGIN,
t.partial AS INDEX_PARTIAL,
group_concat(i.NAME) AS INDEX_COLUMNS
FROM `+schemaTable+` AS m,
pragma_index_list(m.name) AS t,
pragma_index_info(t.name) i
$WHERE
GROUP BY 1, 2, 3, 4, 5, 6

`, product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, "m.name"),
		),
		info.NewQuery(info.KindIndex, `SELECT 
m.name AS TABLE_NAME,
t.name AS INDEX_NAME,
i.seqno AS INDEX_POSITION,
i.desc AS DESCENDING,
i.cid AS ORDINAL_POSITION,
i.name AS COLUMN_NAME,
i.coll AS COLLATION,
i.key AS COLUMN_KEY
FROM `+schemaTable+` AS m,
pragma_index_list(m.name) AS t,
pragma_index_xinfo(t.name) i
WHERE i.name IS NOT NULL
`, product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, "m.name"),
			info.NewCriterion(info.Index, "t.name"),
		),
		info.NewQuery(info.KindSequences, `SELECT name AS SEQUENCE_NAME,  seq AS SEQUENCE_VALUE  
FROM SQLITE_SEQUENCE`,
			product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Sequence, "name"),
		),

		info.NewQuery(info.KindPrimaryKeys, `SELECT
		m.name || '_pk' CONSTRAINT_NAME,
		'PRIMARY KEY' AS CONSTRAINT_TYPE,
		m.name AS TABLE_NAME,
		t.name AS COLUMN_NAME,
		t.cid AS ORDINAL_POSITION
	FROM `+schemaTable+` AS m,
	pragma_table_info(m.name) AS t
	WHERE t.pk = 1 `,
			product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, "m.name		"),
		),

		info.NewQuery(info.KindForeignKeys, `SELECT 
	m.name AS TABLE_NAME, 
	'FOREIGN KEY' AS CONSTRAINT_TYPE,
	t.seq AS ORDINAL_POSITION,`+
			"t.`table` AS REFERENCED_TABLE_NAME, t.`id` AS POSITION_IN_UNIQUE_CONSTRAINT, "+
			"t.`from` AS COLUMN_NAME, "+
			"t.`to` AS REFERENCED_COLUMN_NAME, "+
			"m.name || '_' || t.`table` || '_fk' AS CONSTRAINT_NAME,"+
			"t.`on_update` AS ON_UPDATE, "+
			"t.`on_delete` AS ON_DELETE, "+
			"t.`match` AS ON_MATCH\n"+
			` FROM  `+schemaTable+` AS m,
pragma_foreign_key_list(m.name) t
`,
			product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, "m.name"),
		),

		info.NewQuery(info.KindFunctions, `SELECT 
t.name AS ROUTINE_NAME,
CASE WHEN t.type = 'w' THEN 'NUMERIC' 
	 WHEN t.type = 's' THEN 'TEXT'  
	 ELSE '' END AS DATA_TYPE ,
t.enc AS CHARACTER_SET_NAME,
CASE WHEN t.builtin = 1 THEN 'NATIVE' ELSE '' END AS ROUTINE_TYPE,
CASE WHEN t.flags & 0x800 !=0 THEN 'YES' ELSE 'NO' END AS IS_DETERMINISTIC
FROM pragma_function_list t`,
			product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Function, "t.name"),
		),

		info.NewQuery(info.KindSession, `SELECT
		'' AS PID,
		'' AS USER_NAME,
		'' AS CATALOG,
	    name AS SCHEMA_NAME,
		'' AS APP_NAME
FROM pragma_database_list
`, product),

		info.NewQuery(info.KindForeignKeysCheckOn, `PRAGMA foreign_keys = true`,
			product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),
		info.NewQuery(info.KindForeignKeysCheckOff, `PRAGMA foreign_keys = false`,
			product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Table, ""),
		),

		info.NewQuery(info.KindSequenceNextValue, `SELECT 1`,
			product,
			info.NewCriterion(info.Catalog, ""),
			info.NewCriterion(info.Schema, ""),
			info.NewCriterion(info.Object, ""),
			info.NewCriterion(info.SequenceNewCurrentValue, ""),
		).OnPre(&sequence.Max{}),
	)

	if err != nil {
		log.Printf("failed to register queries: %v", err)
	}

	registry.RegisterDialect(&info.Dialect{
		Product:          product,
		Placeholder:      "?",
		Transactional:    true,
		QuoteCharacter:   '\'',
		Insert:           dialect.InsertWithMultiValues,
		Upsert:           dialect.UpsertTypeInsertOrReplace,
		Load:             dialect.LoadTypeUnsupported,
		CanAutoincrement: true,
		CanLastInsertID:  true,
	})
}
