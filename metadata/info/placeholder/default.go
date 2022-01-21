package placeholder

//Default default placeholder
const Default = "?"

//DefaultGenerator represents DefaultPlaceholderGenerator
type DefaultGenerator struct {
}

//Resolver returns function that returns Default placeholder
func (p *DefaultGenerator) Resolver() func() string {
	return func() string {
		return Default
	}
}

//Len calculates length of placeholders
//might be used to alocate in advance required slice length
func (p *DefaultGenerator) Len(start, numOfPlaceholders int) int {
	return numOfPlaceholders - start
}
