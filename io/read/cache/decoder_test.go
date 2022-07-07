package cache

import (
	"github.com/francoispqt/gojay"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestDecoder(t *testing.T) {
	testCases := []struct {
		description string
		scanTypes   []reflect.Type
		marshaled   string
		expected    []interface{}
	}{
		{
			scanTypes: []reflect.Type{
				reflect.TypeOf(0),
				reflect.TypeOf(""),
				reflect.TypeOf(false),
			},
			marshaled: `[0,"abcdef",true]`,
			expected:  []interface{}{intPtr(0), stringPtr("abcdef"), boolPtr(true)},
		},
		{
			scanTypes: []reflect.Type{
				reflect.TypeOf(0),
				reflect.TypeOf(""),
				reflect.SliceOf(reflect.TypeOf(false)),
			},
			marshaled: `[0,"abcdef",[true, false, false, true]]`,
			expected:  []interface{}{intPtr(0), stringPtr("abcdef"), &[]interface{}{true, false, false, true}},
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
	for _, testCase := range testCases {
		decoder := NewDecoder(testCase.scanTypes)
		assert.Nil(t, gojay.UnmarshalJSONArray([]byte(testCase.marshaled), decoder), testCase.description)
		for i, value := range decoder.values {
			assert.EqualValuesf(t, testCase.expected[i], value, testCase.description)
		}
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func stringPtr(s string) *string {
	return &s
}

func intPtr(value int) *int {
	return &value
}
