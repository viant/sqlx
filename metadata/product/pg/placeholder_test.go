package pg

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"
)

func TestPlaceholderResolver_Len(t *testing.T) {
	testCases := []struct {
		description       string
		start             int
		numOfPlaceholders int
		expected          int
	}{
		{
			description:       "test case $1 - $5",
			start:             0,
			numOfPlaceholders: 5,
			expected:          10,
		},
		{
			description:       "test case $1 - $10",
			start:             0,
			numOfPlaceholders: 10,
			expected:          21,
		},
		{
			description:       "test case $1 - $100",
			start:             0,
			numOfPlaceholders: 100,
			expected:          292,
		},
		{
			description:       "test case $1 - $6000",
			start:             0,
			numOfPlaceholders: 60000,
			expected:          348894,
		},
		{
			description:       "test case $1 - $450",
			start:             0,
			numOfPlaceholders: 450,
			expected:          1692,
		},
		{
			description:       "test case $1 - $1232",
			start:             0,
			numOfPlaceholders: 1232,
			expected:          5053,
		},
	}

	for _, testCase := range testCases {
		actualLen := (&PlaceholderGenerator{}).Len(testCase.start, testCase.numOfPlaceholders)
		assert.Equal(t, testCase.expected, actualLen, testCase.description)
		sb := strings.Builder{}
		for i := testCase.start; i < testCase.numOfPlaceholders; i++ {
			sb.WriteString("$" + strconv.Itoa(i+1))
		}
		assert.Equal(t, sb.Len(), actualLen, testCase.description)
	}
}

func TestPlaceholderResolver_Placeholder(t *testing.T) {
	testCases := []struct {
		description string
		callTimes   int
		expected    []string
	}{
		{
			description: "test case $1 - $5",
			callTimes:   5,
			expected:    []string{"$1", "$2", "$3", "$4", "$5"},
		},
	}

	for _, testCase := range testCases {
		placeholderGetter := (&PlaceholderGenerator{}).Resolver()
		for i := 0; i < testCase.callTimes; i++ {
			assert.Equal(t, testCase.expected[i], placeholderGetter(), testCase.description)
		}
	}
}
