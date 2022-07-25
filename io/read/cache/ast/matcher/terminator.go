package matcher

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

type whitespaceTerminator struct {
}

func (w *whitespaceTerminator) Match(cursor *parsly.Cursor) (matched int) {
	for i := cursor.Pos; i < cursor.InputSize && !matcher.IsWhiteSpace(cursor.Input[i]); i++ {
		matched++
	}

	return matched
}

func NewWhitespaceTerminator() parsly.Matcher {
	return &whitespaceTerminator{}
}
