package pg

import "strconv"

//PlaceholderGenerator represents placeholder
type PlaceholderGenerator struct {
}

//Resolver returns Postgres placeholder
func (p *PlaceholderGenerator) Resolver() func() string {
	counter := 0
	return func() string {
		counter++
		return "$" + strconv.Itoa(counter)
	}
}

//Len calculates length of placeholders
//might be used to alocate in advance required slice length
func (p *PlaceholderGenerator) Len(start, numOfPlaceholders int) int {
	start++

	placeholderLen := numOfPlaceholders
	multiplier := 1
	i := 0

	for multiplier < start+numOfPlaceholders {
		i += numOfPlaceholders % multiplier
		multiplier *= 10
	}

	multiplier = 1
	for numOfPlaceholders != 0 {
		placeholderLen += (numOfPlaceholders - start) * multiplier
		numOfPlaceholders = numOfPlaceholders / 10
		multiplier *= 10
		i++
	}
	return placeholderLen + i
}
