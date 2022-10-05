package sink

//Sequence represents information schema sequence
type Sequence struct {
	Catalog     string `sqlx:"SEQUENCE_CATALOG"`
	Schema      string `sqlx:"SEQUENCE_SCHEMA"`
	Name        string `sqlx:"SEQUENCE_NAME"`
	Value       int64  `sqlx:"SEQUENCE_VALUE"`
	IncrementBy int64  `sqlx:"-"`
	DataType    string `sqlx:"DATA_TYPE"`
	StartValue  string `sqlx:"START_VALUE"`
	MaxValue    string `sqlx:"MAX_VALUE"`
}
