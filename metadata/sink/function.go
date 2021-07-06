package sink

//Function represents information schema function
type Function struct {
	Name          string `sqlx:"ROUTINE_NAME"`
	Body          string `sqlx:"ROUTINE_BODY"`
	DataType      string `sqlx:"DATA_TYPE"`
	Type          string `sqlx:"ROUTINE_TYPE"`
	Charset       string `sqlx:"CHARACTER_SET_NAME"`
	Deterministic string `sqlx:"IS_DETERMINISTIC"`
}
