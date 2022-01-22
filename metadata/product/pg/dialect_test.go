package pg

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/metadata/info"
	"testing"
)

func TestDialect_EnsurePlaceholders(t *testing.T) {
	var testCases = []struct {
		description string
		SQL         string
		expect      string
	}{
		{
			description: "transformed placeholder",
			SQL:         "SELECT COUNT(1) FROM foo WHERE Kind=? AND Active=? AND year > ? ",
			expect:      "SELECT COUNT(1) FROM foo WHERE Kind=$1 AND Active=$2 AND year > $3 ",
		},
	}

	for _, testCase := range testCases {
		dialect := info.Dialect{
			PlaceholderResolver: &PlaceholderGenerator{},
		}
		actual := dialect.EnsurePlaceholders(testCase.SQL)
		assert.Equal(t, testCase.expect, actual, testCase.description)
	}

}
