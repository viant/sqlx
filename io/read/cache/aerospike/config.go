package aerospike

const (
	kb                   = 1024
	mb                   = 1024 * kb
	compressionThreshold = 860
)

var (
	maxSize       = mb
	availableSize = maxSize / 8 * 6
	maxBins       = 32
)
