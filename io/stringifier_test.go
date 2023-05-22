package io

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"reflect"
	"testing"
)

func TestTypeStringifier(t *testing.T) {
	p := fmt.Sprintf

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
		{
			description:   "null pointer as interface",
			rType:         reflect.TypeOf(Interfaces{}),
			exampleObject: nullPointersAsInterfaces(),
			nullValue:     "null",
			results:       []string{"null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null"},
			wasStrings:    []bool{false, false, false, false, false, false, false, false, false, false, false, false, false, false},
		},
		{
			description:   "null as interface",
			rType:         reflect.TypeOf(Interfaces{}),
			exampleObject: &Interfaces{},
			nullValue:     "null",
			results:       []string{"null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null"},
			wasStrings:    []bool{false, false, false, false, false, false, false, false, false, false, false, false, false, false},
		},
		{
			description:   "primitive pointers as interfaces",
			rType:         reflect.TypeOf(Interfaces{}),
			exampleObject: primitivePointersAsInterfaces(),
			nullValue:     "null",
			results:       []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "string", "5.5", "11.5", "true"},
			wasStrings:    []bool{false, false, false, false, false, false, false, false, false, false, true, false, false, false},
		},
		{
			description:   "primitives as interfaces",
			rType:         reflect.TypeOf(Interfaces{}),
			exampleObject: primitivesAsInterfaces(),
			nullValue:     "null",
			results:       []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "string", "5.5", "11.5", "true"},
			wasStrings:    []bool{false, false, false, false, false, false, false, false, false, false, true, false, false, false},
		},
		{
			description:   "primitives min max",
			rType:         reflect.TypeOf(IntegersMinMax{}),
			exampleObject: fnIntegersMinMax(),
			nullValue:     "null",
			results:       []string{p("%d", math.MinInt), p("%d", math.MinInt8), p("%d", math.MinInt16), p("%d", math.MinInt32), p("%d", math.MinInt64), "0", "0", "0", "0", "0", p("%d", math.MaxInt), p("%d", math.MaxInt8), p("%d", math.MaxInt16), p("%d", math.MaxInt32), p("%d", math.MaxInt64), p("%d", uint(math.MaxUint)), p("%d", math.MaxUint8), p("%d", math.MaxUint16), p("%d", math.MaxUint32), p("%d", uint64(math.MaxUint64))},
			wasStrings:    []bool{false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false},
		},
		{
			description:   "primitives pointers min max",
			rType:         reflect.TypeOf(IntegersPtrMinMax{}),
			exampleObject: fnIntegersPtrMinMax(),
			nullValue:     "null",
			results:       []string{p("%d", math.MinInt), p("%d", math.MinInt8), p("%d", math.MinInt16), p("%d", math.MinInt32), p("%d", math.MinInt64), "0", "0", "0", "0", "0", p("%d", math.MaxInt), p("%d", math.MaxInt8), p("%d", math.MaxInt16), p("%d", math.MaxInt32), p("%d", math.MaxInt64), p("%d", uint(math.MaxUint)), p("%d", math.MaxUint8), p("%d", math.MaxUint16), p("%d", math.MaxUint32), p("%d", uint64(math.MaxUint64))},
			wasStrings:    []bool{false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false},
		},
	}

	for _, testCase := range testCases {
		stringify, err := TypeStringifier(testCase.rType, testCase.nullValue, true).Stringifier()
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		strings, bools := stringify(testCase.exampleObject)
		for i := 0; i < len(strings); i++ {
			assert.Equal(t, testCase.results[i], strings[i], testCase.description)
			assert.Equal(t, testCase.wasStrings[i], bools[i], testCase.description)
		}
	}
}

type Interfaces struct {
	IfcInt     interface{} `sqlx:"nullifyEmpty=true"`
	IfcInt8    interface{} `sqlx:"nullifyEmpty=true"`
	IfcUint8   interface{} `sqlx:"nullifyEmpty=true"`
	IfcInt16   interface{} `sqlx:"nullifyEmpty=true"`
	IfcUint16  interface{} `sqlx:"nullifyEmpty=true"`
	IfcInt32   interface{} `sqlx:"nullifyEmpty=true"`
	IfcUint32  interface{} `sqlx:"nullifyEmpty=true"`
	IfcInt64   interface{} `sqlx:"nullifyEmpty=true"`
	IfcUint64  interface{} `sqlx:"nullifyEmpty=true"`
	IfcByte    interface{} `sqlx:"nullifyEmpty=true"`
	IfcString  interface{} `sqlx:"nullifyEmpty=true"`
	IfcFloat32 interface{} `sqlx:"nullifyEmpty=true"`
	IfcFloat64 interface{} `sqlx:"nullifyEmpty=true"`
	IfcBool    interface{} `sqlx:"nullifyEmpty=true"`
}

type primitives struct {
	Int     int
	Int8    int8
	Uint8   uint8
	Int16   int16
	Uint16  uint16
	Int32   int32
	Uint32  uint32
	Int64   int64
	Uint64  uint64
	Byte    byte
	String  string
	Float32 float32
	Float64 float64
	Bool    bool
}

func primitivesAsInterfaces() *Interfaces {
	p := primitives{
		Int:     1,
		Int8:    2,
		Uint8:   3,
		Int16:   4,
		Uint16:  5,
		Int32:   6,
		Uint32:  7,
		Int64:   8,
		Uint64:  9,
		Byte:    10,
		String:  "string",
		Float32: 5.5,
		Float64: 11.5,
		Bool:    true,
	}

	return &Interfaces{
		IfcInt:     p.Int,
		IfcInt8:    p.Int8,
		IfcUint8:   p.Uint8,
		IfcInt16:   p.Int16,
		IfcUint16:  p.Uint16,
		IfcInt32:   p.Int32,
		IfcUint32:  p.Uint32,
		IfcInt64:   p.Int64,
		IfcUint64:  p.Uint64,
		IfcByte:    p.Byte,
		IfcString:  p.String,
		IfcFloat32: p.Float32,
		IfcFloat64: p.Float64,
		IfcBool:    p.Bool,
	}
}

func primitivePointersAsInterfaces() *Interfaces {
	p := primitives{
		Int:     1,
		Int8:    2,
		Uint8:   3,
		Int16:   4,
		Uint16:  5,
		Int32:   6,
		Uint32:  7,
		Int64:   8,
		Uint64:  9,
		Byte:    10,
		String:  "string",
		Float32: 5.5,
		Float64: 11.5,
		Bool:    true,
	}

	return &Interfaces{
		IfcInt:     &p.Int,
		IfcInt8:    &p.Int8,
		IfcUint8:   &p.Uint8,
		IfcInt16:   &p.Int16,
		IfcUint16:  &p.Uint16,
		IfcInt32:   &p.Int32,
		IfcUint32:  &p.Uint32,
		IfcInt64:   &p.Int64,
		IfcUint64:  &p.Uint64,
		IfcByte:    &p.Byte,
		IfcString:  &p.String,
		IfcFloat32: &p.Float32,
		IfcFloat64: &p.Float64,
		IfcBool:    &p.Bool,
	}
}

func nullPointersAsInterfaces() *Interfaces {
	type primitivesPtr struct {
		Int     *int
		Int8    *int8
		Uint8   *uint8
		Int16   *int16
		Uint16  *uint16
		Int32   *int32
		Uint32  *uint32
		Int64   *int64
		Uint64  *uint64
		Byte    *byte
		String  *string
		Float32 *float32
		Float64 *float64
		Bool    *bool
	}

	p := primitivesPtr{}

	return &Interfaces{
		IfcInt:     p.Int,
		IfcInt8:    p.Int8,
		IfcUint8:   p.Uint8,
		IfcInt16:   p.Int16,
		IfcUint16:  p.Uint16,
		IfcInt32:   p.Int32,
		IfcUint32:  p.Uint32,
		IfcInt64:   p.Int64,
		IfcUint64:  p.Uint64,
		IfcByte:    p.Byte,
		IfcString:  p.String,
		IfcFloat32: p.Float32,
		IfcFloat64: p.Float64,
		IfcBool:    p.Bool,
	}
}

type IntegersMinMax struct {
	MinInt    int
	MinInt8   int8
	MinInt16  int16
	MinInt32  int32
	MinInt64  int64
	MinUint   uint
	MinUint8  uint8
	MinUint16 uint16
	MinUint32 uint32
	MinUint64 uint64
	MaxInt    int
	MaxInt8   int8
	MaxInt16  int16
	MaxInt32  int32
	MaxInt64  int64
	MaxUint   uint
	MaxUint8  uint8
	MaxUint16 uint16
	MaxUint32 uint32
	MaxUint64 uint64
}

type IntegersPtrMinMax struct {
	MinInt    *int
	MinInt8   *int8
	MinInt16  *int16
	MinInt32  *int32
	MinInt64  *int64
	MinUint   *uint
	MinUint8  *uint8
	MinUint16 *uint16
	MinUint32 *uint32
	MinUint64 *uint64
	MaxInt    *int
	MaxInt8   *int8
	MaxInt16  *int16
	MaxInt32  *int32
	MaxInt64  *int64
	MaxUint   *uint
	MaxUint8  *uint8
	MaxUint16 *uint16
	MaxUint32 *uint32
	MaxUint64 *uint64
}

func fnIntegersMinMax() *IntegersMinMax {
	return &IntegersMinMax{
		MinInt:    math.MinInt,
		MinInt8:   math.MinInt8,
		MinInt16:  math.MinInt16,
		MinInt32:  math.MinInt32,
		MinInt64:  math.MinInt64,
		MinUint:   uint(0),
		MinUint8:  0,
		MinUint16: 0,
		MinUint32: 0,
		MinUint64: 0,
		MaxInt:    math.MaxInt,
		MaxInt8:   math.MaxInt8,
		MaxInt16:  math.MaxInt16,
		MaxInt32:  math.MaxInt32,
		MaxInt64:  math.MaxInt64,
		MaxUint:   uint(math.MaxUint),
		MaxUint8:  math.MaxUint8,
		MaxUint16: math.MaxUint16,
		MaxUint32: math.MaxUint32,
		MaxUint64: uint64(math.MaxUint64),
	}
}

func fnIntegersPtrMinMax() *IntegersPtrMinMax {
	tmp := IntegersMinMax{
		MinInt:    math.MinInt,
		MinInt8:   math.MinInt8,
		MinInt16:  math.MinInt16,
		MinInt32:  math.MinInt32,
		MinInt64:  math.MinInt64,
		MinUint:   uint(0),
		MinUint8:  0,
		MinUint16: 0,
		MinUint32: 0,
		MinUint64: 0,
		MaxInt:    math.MaxInt,
		MaxInt8:   math.MaxInt8,
		MaxInt16:  math.MaxInt16,
		MaxInt32:  math.MaxInt32,
		MaxInt64:  math.MaxInt64,
		MaxUint:   uint(math.MaxUint),
		MaxUint8:  math.MaxUint8,
		MaxUint16: math.MaxUint16,
		MaxUint32: math.MaxUint32,
		MaxUint64: uint64(math.MaxUint64),
	}

	return &IntegersPtrMinMax{
		MinInt:    &tmp.MinInt,
		MinInt8:   &tmp.MinInt8,
		MinInt16:  &tmp.MinInt16,
		MinInt32:  &tmp.MinInt32,
		MinInt64:  &tmp.MinInt64,
		MinUint:   &tmp.MinUint,
		MinUint8:  &tmp.MinUint8,
		MinUint16: &tmp.MinUint16,
		MinUint32: &tmp.MinUint32,
		MinUint64: &tmp.MinUint64,
		MaxInt:    &tmp.MaxInt,
		MaxInt8:   &tmp.MaxInt8,
		MaxInt16:  &tmp.MaxInt16,
		MaxInt32:  &tmp.MaxInt32,
		MaxInt64:  &tmp.MaxInt64,
		MaxUint:   &tmp.MaxUint,
		MaxUint8:  &tmp.MaxUint8,
		MaxUint16: &tmp.MaxUint16,
		MaxUint32: &tmp.MaxUint32,
		MaxUint64: &tmp.MaxUint64,
	}

}
