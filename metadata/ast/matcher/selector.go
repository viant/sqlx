package matcher

import (
	"github.com/viant/parsly"
)

type selector struct{}

//Match matches a string
func (n *selector) Match(cursor *parsly.Cursor) (matched int) {
	input := cursor.Input
	pos := cursor.Pos
	if startsWithCharacter := IsLetter(input[pos]); startsWithCharacter {
		pos++
		matched++
	} else if input[pos] == '?' || input[pos] == ':' {
		pos++
		matched++
	} else {
		return 0
	}
	size := len(input)
	for i := pos; i < size; i++ {
		switch input[i] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '.', ':':
			matched++
			continue
		case '*':
			if i > 0 && input[i-1] == '.' {
				matched++
				return matched
			}
			return matched
		default:
			if IsLetter(input[i]) {
				matched++
				continue
			}
			return matched
		}
	}

	return matched
}

//NewSelector creates a selector matcher
func NewSelector() *selector {
	return &selector{}
}
