package option

// SequenceTable identifies the physical table whose numeric IDs are allocated.
// It is distinct from an authored sequence name, which need not name a table.
type SequenceTable string

// SequenceTable returns the physical allocation table, if supplied.
func (o Options) SequenceTable() string {
	for _, candidate := range o {
		if actual, ok := candidate.(SequenceTable); ok {
			return string(actual)
		}
	}
	return ""
}
