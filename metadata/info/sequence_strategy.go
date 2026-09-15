package info

import "github.com/viant/sqlx/metadata/info/dialect"

// SequenceStrategy resolves explicit allocation policy. A dialect may use
// database-generated IDs for ordinary INSERT while reserving IDs explicitly
// through a different native owner (SQLite is one such dialect).
func (d *Dialect) SequenceStrategy(requested dialect.PresetIDStrategy) dialect.PresetIDStrategy {
	if requested != "" && requested != dialect.PresetIDStrategyUndefined {
		return requested
	}
	if d.DefaultSequenceStrategy != "" && d.DefaultSequenceStrategy != dialect.PresetIDStrategyUndefined {
		return d.DefaultSequenceStrategy
	}
	return d.DefaultPresetIDStrategy
}
