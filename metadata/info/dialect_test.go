package info

import (
	"github.com/stretchr/testify/assert"
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
