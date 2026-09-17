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
		{
			scanTypes: []reflect.Type{
				reflect.TypeOf(false),
			},
			marshaled: `[1]`,
			expected:  []interface{}{boolPtr(true)},
		},
		{
			scanTypes: []reflect.Type{
				reflect.TypeOf(&falseValue),
			},
			marshaled: `[0]`,
			expected:  []interface{}{boolDoublePtr(false)},
		},
		{
			scanTypes: []reflect.Type{
				reflect.TypeOf(&falseValue),
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

func TestDecoder_BoolNullToNonPointerFails(t *testing.T) {
	decoder := NewDecoder([]reflect.Type{reflect.TypeOf(false)}, []byte(`[null]`))
	err := gojay.UnmarshalJSONArray([]byte(`[null]`), decoder)
	assert.EqualError(t, err, "Cannot unmarshal JSON to type 'bool'")
}

func TestDecoder_EscapedStringPreservesLaterNulls(t *testing.T) {
	marshaled := `["FoxNews:US\u0026WorldHeadlines",null,null,null,null]`
	scanTypes := []reflect.Type{
		reflect.TypeOf(""),
		reflect.TypeOf((*int)(nil)),
		reflect.TypeOf((*float64)(nil)),
		reflect.TypeOf((*string)(nil)),
		reflect.TypeOf((*bool)(nil)),
	}

	decoder := NewDecoder(scanTypes, []byte(marshaled))
	err := gojay.UnmarshalJSONArray([]byte(marshaled), decoder)
	assert.NoError(t, err)
	assert.EqualValues(t, stringPtr("FoxNews:US&WorldHeadlines"), decoder.values[0])
	assert.Nil(t, decoder.values[1])
	assert.Nil(t, decoder.values[2])
	assert.Nil(t, decoder.values[3])
	assert.Nil(t, decoder.values[4])
}

func TestDecoder_EscapedQuotePreservesLaterNulls(t *testing.T) {
	marshaled := `["prefix\"quoted\"",null,null]`
	scanTypes := []reflect.Type{
		reflect.TypeOf(""),
		reflect.TypeOf((*int)(nil)),
		reflect.TypeOf((*string)(nil)),
	}

	decoder := NewDecoder(scanTypes, []byte(marshaled))
	err := gojay.UnmarshalJSONArray([]byte(marshaled), decoder)
	assert.NoError(t, err)
	assert.EqualValues(t, stringPtr(`prefix"quoted"`), decoder.values[0])
	assert.Nil(t, decoder.values[1])
	assert.Nil(t, decoder.values[2])
}

func TestDecoder_EscapedStringKeepsLegitimateZeroValues(t *testing.T) {
	marshaled := `["prefix\\suffix",0,0.0,"",false]`
	scanTypes := []reflect.Type{
		reflect.TypeOf(""),
		reflect.TypeOf((*int)(nil)),
		reflect.TypeOf((*float64)(nil)),
		reflect.TypeOf((*string)(nil)),
		reflect.TypeOf((*bool)(nil)),
	}

	decoder := NewDecoder(scanTypes, []byte(marshaled))
	err := gojay.UnmarshalJSONArray([]byte(marshaled), decoder)
	assert.NoError(t, err)
	assert.EqualValues(t, stringPtr(`prefix\suffix`), decoder.values[0])
	assert.EqualValues(t, intDoublePtr(0), decoder.values[1])
	assert.EqualValues(t, float64DoublePtr(0), decoder.values[2])
	assert.EqualValues(t, stringDoublePtr(""), decoder.values[3])
	assert.EqualValues(t, boolDoublePtr(false), decoder.values[4])
}

func boolPtr(b bool) *bool {
	return &b
}

var falseValue bool

func boolDoublePtr(b bool) **bool {
	value := boolPtr(b)
	return &value
}

func stringPtr(s string) *string {
	return &s
}

func intPtr(value int) *int {
	return &value
}

func intDoublePtr(value int) **int {
	ptr := intPtr(value)
	return &ptr
}

func float64Ptr(value float64) *float64 {
	return &value
}

func float64DoublePtr(value float64) **float64 {
	ptr := float64Ptr(value)
	return &ptr
}

func stringDoublePtr(value string) **string {
	ptr := stringPtr(value)
	return &ptr
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
