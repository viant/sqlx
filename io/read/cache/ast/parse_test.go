package ast

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestParseType(t *testing.T) {
	fooType := reflect.StructOf([]reflect.StructField{
		{
			Name: "Name",
			Type: StringType,
		},
		{
			Name: "Price",
			Type: Float64Type,
		},
	})

	ifaceStruct := reflect.StructOf([]reflect.StructField{
		{
			Name: "Boolean",
			Type: reflect.TypeOf(false),
		},
		{
			Name: "Iface",
			Type: InterfaceType,
		},
	})

	type Boo struct {
		BooName  string
		BooPrice float32
	}

	barType := reflect.StructOf([]reflect.StructField{
		{
			Name: "Name",
			Type: StringType,
		},
		{
			Name: "Price",
			Type: reflect.TypeOf(Boo{}),
		},
	})

	typeWithTags := reflect.StructOf([]reflect.StructField{
		{
			Name: "Name",
			Type: StringType,
			Tag:  `json:"Name" sqlx:"autoincrement=true"`,
		},
		{
			Name: "Price",
			Type: Float64Type,
		},
	})

	testCases := []struct {
		description string
		rType       reflect.Type
		asPtr       bool
		extraTypes  []reflect.Type
	}{
		{
			description: "int",
			rType:       IntType,
		},
		{
			description: "int8",
			rType:       Int8Type,
		},
		{
			description: "int16",
			rType:       Int16Type,
		},
		{
			description: "int32",
			rType:       Int32Type,
		},
		{
			description: "int64",
			rType:       Int64Type,
		},

		{
			description: "uint",
			rType:       UintType,
		},
		{
			description: "uint8",
			rType:       Uint8Type,
		},
		{
			description: "uint16",
			rType:       Uint16Type,
		},
		{
			description: "uint32",
			rType:       Uint32Type,
		},
		{
			description: "uint64",
			rType:       Uint64Type,
		},

		{
			description: "string",
			rType:       StringType,
		},
		{
			description: "float32",
			rType:       Float32Type,
		},
		{
			description: "float64",
			rType:       Float32Type,
		},

		{
			description: "int",
			rType:       IntType,
			asPtr:       true,
		},
		{
			description: "int8",
			rType:       Int8Type,
			asPtr:       true,
		},
		{
			description: "int16",
			rType:       Int16Type,
			asPtr:       true,
		},
		{
			description: "int32",
			rType:       Int32Type,
			asPtr:       true,
		},
		{
			description: "int64",
			rType:       Int64Type,
			asPtr:       true,
		},

		{
			description: "uint",
			rType:       UintType,
			asPtr:       true,
		},
		{
			description: "uint8",
			rType:       Uint8Type,
			asPtr:       true,
		},
		{
			description: "uint16",
			rType:       Uint16Type,
			asPtr:       true,
		},
		{
			description: "uint32",
			rType:       Uint32Type,
			asPtr:       true,
		},
		{
			description: "uint64",
			rType:       Uint64Type,
			asPtr:       true,
		},

		{
			description: "string",
			rType:       StringType,
			asPtr:       true,
		},
		{
			description: "float32",
			rType:       Float32Type,
			asPtr:       true,
		},
		{
			description: "float64",
			rType:       Float32Type,
			asPtr:       true,
		},
		{
			description: "struct",
			rType:       fooType,
			asPtr:       true,
		},
		{
			description: "time",
			rType:       TimeType,
			asPtr:       true,
		},
		{
			description: "slice of ptr of slice of struct",
			rType:       reflect.SliceOf(reflect.PtrTo(reflect.SliceOf(fooType))),
			asPtr:       true,
		},
		{
			description: "nested regular type",
			rType:       barType,
			extraTypes:  []reflect.Type{reflect.TypeOf(Boo{})},
		},
		{
			description: "regular type",
			rType:       reflect.TypeOf(Boo{}),
			extraTypes:  []reflect.Type{reflect.TypeOf(Boo{})},
		},
		{
			description: "struct with tags",
			rType:       typeWithTags,
			extraTypes:  []reflect.Type{reflect.TypeOf(Boo{})},
		},
		{
			description: "interface",
			rType:       ifaceStruct,
		},
	}

	//for i, testCase := range testCases[len(testCases)-1:] {
	for i, testCase := range testCases {
		fmt.Printf("Running testcase %v\n", i)

		rType := testCase.rType
		if testCase.asPtr {
			rType = reflect.PtrTo(rType)
		}

		parse, err := Parse(rType.String(), testCase.extraTypes...)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.Equal(t, rType.String(), parse.String(), testCase.description)
	}
}

func Benchmark_Parse_sqlx(b *testing.B) {
	typeWithTags := reflect.StructOf([]reflect.StructField{
		{
			Name: "Name",
			Type: StringType,
			Tag:  `json:"Name" sqlx:"autoincrement=true"`,
		},
		{
			Name: "Price",
			Type: Float64Type,
			Tag:  `json:"Price" sqlx:"autoincrement=true"`,
		},
		{
			Name: "Date",
			Type: TimeType,
			Tag:  `json:"Date" sqlx:"autoincrement=true"`,
		},
	})

	typeStr := typeWithTags.String()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Parse(typeStr)
	}
}
