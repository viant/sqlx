package placeholder

//Generator represents placeholder generator
type Generator interface {
	Resolver() func() string
	Len(start, numOfPlaceholders int) int
}
