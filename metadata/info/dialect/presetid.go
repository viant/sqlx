package dialect

// PresetIDStrategy represents strategy of presetting identities
type PresetIDStrategy string

// PresetIDStrategyUndefined and others, represent presetting identities strategies
const (
	PresetIDStrategyUndefined        = PresetIDStrategy("undefined")
	PresetIDStrategyIgnore           = PresetIDStrategy("ignore")
	PresetIDWithTransientTransaction = PresetIDStrategy("transient")
	PresetIDWithUDFSequence          = PresetIDStrategy("udf")
	PresetIDWithMax                  = PresetIDStrategy("maxid")
)
