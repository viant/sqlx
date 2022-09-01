package cache

import (
	"github.com/francoispqt/gojay"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
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
		{
			scanTypes: []reflect.Type{
				reflect.TypeOf(time.Time{}),
			},
			marshaled: `["2022-07-08T23:25:26.721357+02:00"]`,
			expected:  []interface{}{asTimePtr("2022-07-08T23:25:26.721357+02:00")},
		},
		{
			scanTypes: []reflect.Type{
				reflect.TypeOf(&time.Time{}),
			},
			marshaled: `["2022-07-08T23:25:26.721357+02:00"]`,
			expected:  []interface{}{asTimeDoublePtr("2022-07-08T23:25:26.721357+02:00")},
		},
		{
			scanTypes: []reflect.Type{
				reflect.TypeOf(time.Time{}),
			},
			marshaled: `["0000-01-01T15:06:00Z"]`,
			expected:  []interface{}{asTimePtrWithLayout(time.Kitchen, "3:06PM")},
		},
		{
			scanTypes: []reflect.Type{
				reflect.PtrTo(reflect.TypeOf(0)),
			},
			marshaled: `[null]`,
			expected:  []interface{}{nil},
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
	for _, testCase := range testCases {
		decoder := NewDecoder(testCase.scanTypes, []byte(testCase.marshaled))
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

func asTimePtr(value string) *time.Time {
	parse, _ := time.Parse(time.RFC3339Nano, value)
	return &parse
}

func asTimeDoublePtr(value string) **time.Time {
	parse := asTimePtr(value)
	return &parse
}

func asTimePtrWithLayout(layout string, value string) *time.Time {
	aTime, _ := time.Parse(layout, value)
	return &aTime
}
