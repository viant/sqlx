package aerospike

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
	"log"
)

const product = "Aerospike"
const driver = "Driver"
const driverPkg = "aerospike"

var aerospike = database.Product{
	Name:      product,
	DriverPkg: driverPkg,
	Driver:    driver,
	//Major:     int,
	//Minor:     int,
	//Release:   int
}

// Aerospike return Aerospike product
func Aerospike() *database.Product {
	return &aerospike
}

func init() {
	err := registry.Register(

		//info.NewQuery(info.KindVersion, "SELECT CONCAT('MySQL - ', VERSION())", aerospike),

		info.NewQuery(info.KindSchemas, `select
'' catalog_name,
schema_name,
'' sql_path,
'utf8' default_character_set_name,
'' as default_collation_name
from information_schema.schemata
`, aerospike,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
		),

		info.NewQuery(info.KindSchema, `select
'' catalog_name,
schema_name,
'' sql_path,
'utf8' default_character_set_name,
'' as default_collation_name
from information_schema.schemata
		`, aerospike,
			info.NewCriterion(info.Catalog, "CATALOG_NAME"),
			info.NewCriterion(info.Schema, "pk"),
		),

		info.NewQuery(info.KindTables, `select
'' table_catalog,
table_schema,
table_name,
'' table_comment,
'' table_type,
'' as auto_increment,
'' create_time,
'' update_time,
0 table_rows,
'' version,
'' engine,
'' ddl
from information_schema.tables`,
			aerospike,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "pk"),
		),

		info.NewQuery(info.KindTable, `select
'' table_catalog,
table_schema,
table_name,
column_name,
ordinal_position,
column_comment,
data_type,
character_maximum_length,
numeric_precision,
numeric_scale,
is_nullable,
column_default,
column_key,
is_autoincrement
from information_schema.columns`,
			aerospike,
			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
			info.NewCriterion(info.Table, "pk"),
		),
		//		// please leave 0 values inside coalesce statements
		//		info.NewQuery(info.KindSequences, `SELECT
		//  '' SEQUENCE_CATALOG,
		//  '$Args[1]' AS SEQUENCE_SCHEMA,
		//  '$Args[2]'  AS SEQUENCE_NAME,
		//  0 AS SEQUENCE_VALUE,
		//  COALESCE(@@SESSION.auto_increment_increment, 0) INCREMENT_BY,
		//  'int' AS DATA_TYPE,
		//  COALESCE(@@SESSION.auto_increment_offset, 0) START_VALUE,
		//  `+strconv.Itoa(sequence.MaxSeqValue)+` AS MAX_VALUE
		//`,
		//			mySQL5,
		//			info.NewCriterion(info.Catalog, ""),
		//			info.NewCriterion(info.Schema, ""),
		//			info.NewCriterion(info.Sequence, ""),
		//		).OnPost(info.NewHandler(sequence.UpdateMySQLSequence)),
		//
		//		info.NewQuery(info.KindIndexes, `SELECT
		//		'' TABLE_CATALOG,
		//		TABLE_SCHEMA,
		//		TABLE_NAME,
		//		INDEX_SCHEMA,
		//		INDEX_NAME,
		//		INDEX_TYPE,
		//		CASE WHEN NON_UNIQUE = 1 THEN 1 ELSE 0 END AS INDEX_UNIQUE,
		//		GROUP_CONCAT(COLUMN_NAME) AS INDEX_COLUMNS
		//FROM INFORMATION_SCHEMA.STATISTICS
		//$WHERE
		//GROUP BY 1, 2, 3, 4, 5, 6, 7
		//`, mySQL5,
		//			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
		//			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
		//			info.NewCriterion(info.Table, "TABLE_NAME"),
		//		),
		//
		//		info.NewQuery(info.KindIndex, `SELECT
		//		'' TABLE_CATALOG,
		//		TABLE_SCHEMA,
		//		TABLE_NAME,
		//		INDEX_NAME,
		//		COLUMN_NAME,
		//		COLLATION,
		//		SEQ_IN_INDEX INDEX_POSITION
		//FROM INFORMATION_SCHEMA.STATISTICS
		//`, mySQL5,
		//			info.NewCriterion(info.Catalog, "TABLE_CATALOG"),
		//			info.NewCriterion(info.Schema, "TABLE_SCHEMA"),
		//			info.NewCriterion(info.Table, "TABLE_NAME"),
		//			info.NewCriterion(info.Index, "INDEX_NAME"),
		//		),
		//
		//		info.NewQuery(info.KindPrimaryKeys, `SELECT
		//c.CONSTRAINT_NAME,
		//s.CONSTRAINT_TYPE,
		//'' CONSTRAINT_CATALOG,
		//s.CONSTRAINT_SCHEMA,
		//c.TABLE_NAME,
		//c.COLUMN_NAME,
		//COALESCE(c.REFERENCED_TABLE_NAME, '') AS REFERENCED_TABLE_NAME,
		//COALESCE(c.REFERENCED_COLUMN_NAME, '')REFERENCED_COLUMN_NAME,
		//CASE WHEN c.REFERENCED_TABLE_NAME IS NOT NULL THEN s.CONSTRAINT_SCHEMA ELSE '' END AS REFERENCED_TABLE_SCHEMA
		//FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS s
		//JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON c.CONSTRAINT_NAME = s.CONSTRAINT_NAME
		//	 AND c.CONSTRAINT_CATALOG = s.CONSTRAINT_CATALOG
		//	 AND c.CONSTRAINT_SCHEMA = s.CONSTRAINT_SCHEMA
		//	 AND c.TABLE_NAME = s.TABLE_NAME
		//WHERE  s.CONSTRAINT_TYPE = 'PRIMARY KEY'
		//`, mySQL5,
		//			info.NewCriterion(info.Catalog, "s.CONSTRAINT_CATALOG"),
		//			info.NewCriterion(info.Schema, "s.CONSTRAINT_SCHEMA"),
		//			info.NewCriterion(info.Table, "c.TABLE_NAME"),
		//		),
		//
		//		// TODO A BUG IN LINE s.CONSTRAINT_SCHEMA AS REFERENCED_TABLE_SCHEMA
		//		info.NewQuery(info.KindForeignKeys, `SELECT
		//c.CONSTRAINT_NAME,
		//s.CONSTRAINT_TYPE,
		//'' CONSTRAINT_CATALOG,
		//s.CONSTRAINT_SCHEMA,
		//c.TABLE_NAME,
		//c.COLUMN_NAME,
		//c.REFERENCED_TABLE_NAME,
		//c.REFERENCED_COLUMN_NAME,
		//s.CONSTRAINT_SCHEMA AS REFERENCED_TABLE_SCHEMA
		//FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS s
		//JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON c.CONSTRAINT_NAME = s.CONSTRAINT_NAME
		//	 AND c.CONSTRAINT_CATALOG = s.CONSTRAINT_CATALOG
		//	 AND c.CONSTRAINT_SCHEMA = s.CONSTRAINT_SCHEMA
		//	 AND c.TABLE_NAME = s.TABLE_NAME
		//WHERE s.CONSTRAINT_TYPE = 'FOREIGN KEY'
		//`, mySQL5,
		//			info.NewCriterion(info.Catalog, "s.CONSTRAINT_CATALOG"),
		//			info.NewCriterion(info.Schema, "s.CONSTRAINT_SCHEMA"),
		//			info.NewCriterion(info.Table, "c.TABLE_NAME"),
		//		),
		//
		//		info.NewQuery(info.KindSession, `SELECT
		//CAST(ID AS CHAR) AS PID,
		//CAST(USER AS CHAR) AS USER_NAME,
		//"" AS CATALOG,
		//CAST(DB as CHAR) as SCHEMA_NAME,
		//"" AS APP_NAME
		//from information_schema.processlist
		//where ID=CONNECTION_ID() LIMIT 1;
		//`, mySQL5),
		//		info.NewQuery(info.KindForeignKeysCheckOn, `SET FOREIGN_KEY_CHECKS=1`,
		//			mySQL5,
		//			info.NewCriterion(info.Catalog, ""),
		//			info.NewCriterion(info.Schema, ""),
		//			info.NewCriterion(info.Table, ""),
		//		),
		//
		//		info.NewQuery(info.KindForeignKeysCheckOff, `SET FOREIGN_KEY_CHECKS=0`,
		//			mySQL5,
		//			info.NewCriterion(info.Catalog, ""),
		//			info.NewCriterion(info.Schema, ""),
		//			info.NewCriterion(info.Table, ""),
		//		),
		//
		//		info.NewQuery(info.KindSequenceNextValue, `SELECT 1`,
		//			mySQL5,
		//			info.NewCriterion(info.Catalog, ""),
		//			info.NewCriterion(info.Schema, ""),
		//			info.NewCriterion(info.Object, ""),
		//			info.NewCriterion(info.SequenceNewCurrentValue, ""),
		//		).OnPre(&sequence.Transient{}, &sequence.Udf{}),
		//
		//		info.NewQuery(info.KindLockGet, `SELECT '$Args[0]' AS LOCK_CATALOG,
		//'$Args[1]' AS LOCK_SCHEMA,
		//'$Args[2]' AS LOCK_TABLE,
		//'$Args[0].$Args[1].$Args[2]' AS LOCK_NAME,
		//GET_LOCK('$Args[0].$Args[1].$Args[2]',10) AS SUCCESS`,
		//			mySQL5,
		//			info.NewCriterion(info.Catalog, ""),
		//			info.NewCriterion(info.Schema, ""),
		//			info.NewCriterion(info.Table, ""),
		//		),
		//
		//		info.NewQuery(info.KindLockRelease, `SELECT '$Args[0]' AS LOCK_CATALOG,
		//'$Args[1]' AS LOCK_SCHEMA,
		//'$Args[2]' AS LOCK_TABLE,
		//'$Args[0].$Args[1].$Args[2]' AS LOCK_NAME,
		//RELEASE_LOCK('$Args[0].$Args[1].$Args[2]')  AS SUCCESS`,
		//			mySQL5,
		//			info.NewCriterion(info.Catalog, ""),
		//			info.NewCriterion(info.Schema, ""),
		//			info.NewCriterion(info.Table, ""),
		//		),
	)

	if err != nil {
		log.Printf("failed to register queries: %v", err)
	}

	registry.RegisterDialect(&info.Dialect{
		Product:                   aerospike,
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
