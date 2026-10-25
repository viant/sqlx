package option

// InsertIgnore selects MySQL's fixed INSERT IGNORE mode when constructing an
// insert service or builder. It does not change identity allocation policy.
type InsertIgnore bool

// InsertIgnore returns the fixed insert mode, which defaults to ordinary INSERT.
func (o Options) InsertIgnore() bool {
	for _, candidate := range o {
		if value, ok := candidate.(InsertIgnore); ok {
			return bool(value)
		}
	}
	return false
}
