package dialect

//UpsertFeatures represents dialect supported merge type bitset
type UpsertFeatures int

const (
	//UpsertTypeUnsupported defines uppsert types
	UpsertTypeUnsupported = UpsertFeatures(iota)
	//UpsertTypeMerge defines merge upsert type
	UpsertTypeMerge //i.e PostgreSQL, BigQuery
	//UpsertTypeMergeInto //defined merge into upsert
	UpsertTypeMergeInto //i.e Oracle, Vertica, MS SQL, PostgreSQL dialect
	//UpsertTypeInsertOrReplace defined insert or replace upsert
	UpsertTypeInsertOrReplace //i.e. SQLLite
	//UpsertTypeInsertOrUpdate defined insert or update upsert
	UpsertTypeInsertOrUpdate //i.e MySQL
	//UpsertTypeUpdateOrInsert defined update or insert upsert
	UpsertTypeUpdateOrInsert //i.e Firebird
)
