package sqlserver

import (
	"github.com/viant/sqlx/metadata/registry"
	"strconv"
)

//PlaceholderGenerator represents placeholder
type PlaceHolderGenerator struct {
}

//Resolver returns function that returns Default placeholder
func (p *PlaceHolderGenerator) Resolver() func() string {
	counter := 0
	return func() string {
		counter++
		return registry.LookupDialect(&sqlServer).Placeholder + strconv.Itoa(counter)
	}
}

//Len calculates length of placeholders
//might be used to allocate in advance required slice length
func (p *PlaceHolderGenerator) Len(start, numOfPlaceholders int) int {
	return numOfPlaceholders - start
}
