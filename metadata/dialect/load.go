package dialect

//LoadFeature represents dialect supported load type type bitset
type LoadFeature int

const (
	LoadTypeUnsupported = LoadFeature(iota)
	LoadTypeLocalData
)
