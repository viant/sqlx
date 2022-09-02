package io

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestTypeStringifier(t *testing.T) {
	type Boo struct {
		ID      int
		Name    string
		Comment string
	}

	type Foo struct {
		ID      int
		Name    string `sqlx:"nullifyEmpty=true"`
		Comment string
	}

	testCases := []struct {
		description   string
		rType         reflect.Type
		exampleObject interface{}
		nullValue     string
		results       []string
		wasStrings    []bool
	}{
		{
			description: "without nullifyEmpty tag",
			rType:       reflect.TypeOf(Boo{}),
			exampleObject: &Boo{
				ID:      25,
				Name:    "",
				Comment: "some comment",
			},
			nullValue:  "null",
			results:    []string{"25", "", "some comment"},
			wasStrings: []bool{false, true, true},
		},
		{
			description: "with nullifyEmpty tag",
			rType:       reflect.TypeOf(Foo{}),
			exampleObject: &Foo{
				ID:      25,
				Name:    "",
				Comment: "some comment",
			},
			nullValue:  "null",
			results:    []string{"25", "null", "some comment"},
			wasStrings: []bool{false, false, true},
		},
	}

	for _, testCase := range testCases {
		stringify := TypeStringifier(testCase.rType, testCase.nullValue, true)
		strings, bools := stringify(testCase.exampleObject)
		for i := 0; i < len(strings); i++ {
			assert.Equal(t, testCase.results[i], strings[i], testCase.description)
			assert.Equal(t, testCase.wasStrings[i], bools[i], testCase.description)
		}
	}
}
