package sink

import "fmt"

// Sequence represents information schema sequence
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

// MinValue returns previous sequence's next value for given record count,
func (s *Sequence) MinValue(recordCount int64) int64 {
	// For any recordCount, if current value is at or below start, clamp to StartValue.
	if s.Value <= s.StartValue {
		return s.StartValue
	}

	// Hot path: single record, step of 1.
	if recordCount == 1 && s.IncrementBy == 1 {
		return s.Value - 1
	}

	// From here on we know: s.Value > s.StartValue, so (s.Value - s.StartValue) > 0
	// In Go, (positive % anything non-zero) is >= 0, so modValue < 0 is impossible.
	modValue := (s.Value - s.StartValue) % s.IncrementBy

	if modValue == 0 {
		return s.Value - recordCount*s.IncrementBy
	}

	return s.Value - modValue - recordCount*s.IncrementBy
}

// NextValue returns sequence's next value for given record count
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

func (s *Sequence) ComputeNextForTransient(recordCount int64) (int64, error) {
	if recordCount <= 0 {
		return 0, fmt.Errorf("recordCount must be > 0, got %d", recordCount)
	}

	oldValue := s.Value

	// Advance sequence by recordCount
	s.Value = s.NextValue(recordCount)

	// Sanity check: new value must be at least recordCount ahead
	if diff := s.Value - oldValue; diff < recordCount {
		return 0, fmt.Errorf(
			"new next value for sequence %s (%d) is too small, expected >= %d but had %d",
			s.Name, s.Value, oldValue+recordCount, s.Value,
		)
	}

	// decreasing is required for transient insert approach
	passedValue := s.Value - s.IncrementBy
	return passedValue, nil
}
