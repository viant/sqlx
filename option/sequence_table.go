package option

// SequenceTable identifies the physical table whose numeric IDs are allocated.
// It is distinct from an authored sequence name, which need not name a table.
type SequenceTable string

// SequenceIdentityOnly requests read-only identity/arithmetic metadata rather
// than a current counter. It never authorizes a sequence reservation.
type SequenceIdentityOnly bool

func (o Options) SequenceIdentityOnly() bool {
	for _, candidate := range o {
		if value, ok := candidate.(SequenceIdentityOnly); ok {
			return bool(value)
		}
	}
	return false
}

// SequenceTable returns the physical allocation table, if supplied.
func (o Options) SequenceTable() string {
	for _, candidate := range o {
		if actual, ok := candidate.(SequenceTable); ok {
			return string(actual)
		}
	}
	return ""
}

// SequenceColumn is the numeric column selected by the native insert mapper.
type SequenceColumn string

func (o Options) SequenceColumn() string {
	for _, candidate := range o {
		if value, ok := candidate.(SequenceColumn); ok {
			return string(value)
		}
	}
	return ""
}

// SequenceField selects a Go field through the native insert mapper, without
// asking an application to parse sqlx tags or infer a physical column name.
type SequenceField string

func (o Options) SequenceField() string {
	for _, candidate := range o {
		if value, ok := candidate.(SequenceField); ok {
			return string(value)
		}
	}
	return ""
}

// SequenceReservationIntent requests the product's allocation preparation before
// reading identity metadata, without selecting or overriding its preset strategy.
type SequenceReservationIntent bool

func (o Options) SequenceReservationIntent() bool {
	for _, candidate := range o {
		if value, ok := candidate.(SequenceReservationIntent); ok {
			return bool(value)
		}
	}
	return false
}
