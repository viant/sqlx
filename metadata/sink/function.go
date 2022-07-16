package sink

//Function represents information schema function
type Function struct {
	Catalog       string `sqlx:"ROUTINE_CATALOG"`
	Schema        string `sqlx:"ROUTINE_SCHEMA"`
	Name          string `sqlx:"ROUTINE_NAME"`
	Body          string `sqlx:"ROUTINE_BODY"`
	DataType      string `sqlx:"DATA_TYPE"`    // The data type name that the SQL function returns.
	Type          string `sqlx:"ROUTINE_TYPE"` // Native or user defined ("NATIVE" or "")
	Charset       string `sqlx:"CHARACTER_SET_NAME"`
	Deterministic string `sqlx:"IS_DETERMINISTIC"`
}
