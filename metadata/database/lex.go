package database

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

//number represents a version digit token
var digits = parsly.NewToken(1, "digits", matcher.NewDigits())

//separator represents a version separator token
var separator = parsly.NewToken(2, "separator", matcher.NewCharset(".:-"))
