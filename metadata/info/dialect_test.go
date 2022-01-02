package info

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestDialect_EnsurePlaceholders(t *testing.T) {

	var testCases = []struct {
		dialect     Dialect
		description string
		sQL         string
		expect      string
	}{
		{
			description: "transformed placeholder",
			dialect: Dialect{
				Placeholder: "$",
				PlaceholderResolver: func() func() string {
					counter := 1
					return func() string {
						result := "$" + strconv.Itoa(counter)
						counter++
						return result
					}
				},
			},
			sQL:    "SELECT COUNT(1) FROM foo WHERE Kind=? AND Active=? AND year > ? ",
			expect: "SELECT COUNT(1) FROM foo WHERE Kind=$1 AND Active=$2 AND year > $3 ",
		},
		{
			description: "original placeholders",
			dialect: Dialect{
				Placeholder: "?",
			},
			sQL:    "SELECT COUNT(1) FROM foo WHERE Kind=? AND Active=? AND year > ? ",
			expect: "SELECT COUNT(1) FROM foo WHERE Kind=? AND Active=? AND year > ? ",
		},
	}

	for _, testCase := range testCases {
		actual := testCase.dialect.EnsurePlaceholders(testCase.sQL)
		assert.Equal(t, testCase.expect, actual, testCase.description)
	}

}
