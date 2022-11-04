package sink

//Sequence represents information schema sequence
type Sequence struct {
	Catalog     string `sqlx:"SEQUENCE_CATALOG"`
	Schema      string `sqlx:"SEQUENCE_SCHEMA"`
	Name        string `sqlx:"SEQUENCE_NAME"`
	Value       int64  `sqlx:"SEQUENCE_VALUE"`
	IncrementBy int64  `sqlx:"INCREMENT_BY"`
	DataType    string `sqlx:"DATA_TYPE"`
	StartValue  int64  `sqlx:"START_VALUE"`
	MaxValue    int64  `sqlx:"MAX_VALUE"`
}

func (s *Sequence) MinValue(recordCount int64) int64 {

	modValue := (s.Value - s.StartValue) % s.IncrementBy

	if modValue == 0 && s.Value > s.StartValue {
		return s.Value - recordCount*s.IncrementBy
	}

	if modValue == 0 && s.Value <= s.StartValue {
		return s.StartValue
	}

	if modValue < 0 {
		return s.StartValue
	}

	return s.Value - modValue - recordCount*s.IncrementBy
}

func (s *Sequence) NextValue(recordCount int64) int64 {
	modValue := (s.Value - s.StartValue) % s.IncrementBy

	if modValue == 0 && s.Value > s.StartValue {
		return s.Value + recordCount*s.IncrementBy
	}

	if modValue == 0 && s.Value <= s.StartValue {
		return s.StartValue + recordCount*s.IncrementBy
	}

	if modValue < 0 {
		return s.StartValue + recordCount*s.IncrementBy
	}

	return s.Value - modValue + s.IncrementBy + recordCount*s.IncrementBy
}
