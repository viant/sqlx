package dialect

//UpsertFeatures represents dialect supported merge type bitset
type UpsertFeatures int

const (
	UpsertTypeUnsupported     = UpsertFeatures(iota)
	UpsertTypeMerge           //i.e PostgreSQL, BigQuery
	UpsertTypeMergeInto       //i.e Oracle dialect
	UpsertTypeInsertOrReplace //i.e. SQLLite
	UpsertTypeInsertOrUpdate  //i.e MySQL
	UpsertTypeUpdateOrInsert  //i.e Firebird
)
