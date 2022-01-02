package dialect

//LoadFeature represents dialect supported load type type bitset
type LoadFeature int

const (
	//LoadTypeUnsupported defines unsupported type
	LoadTypeUnsupported = LoadFeature(iota)
	//LoadTypeLocalData defined local data load support
	LoadTypeLocalData
)
