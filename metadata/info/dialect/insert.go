package dialect

//InsertFeatures represents dialect supported insert type bitset
type InsertFeatures int

//MultiValues returns true if dialect support multi values insert batch DML
func (t InsertFeatures) MultiValues() bool {
	target := int(InsertWithMultiValues)
	return int(t)&target == target
}

const (

	//InsertWithSingleValues single values insert
	InsertWithSingleValues = InsertFeatures(iota)
	//InsertWithMultiValues multi values insert
	InsertWithMultiValues
)
