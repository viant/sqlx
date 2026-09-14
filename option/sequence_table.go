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
