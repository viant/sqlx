package io

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/xunsafe"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func TestMatcher_Match(t *testing.T) {
	now := time.Now()
	unmappedName := "xxx"
	unmappedActive := true
	var testCases = []struct {
		description string
		resolve     Resolve
		targetType  reflect.Type
		columns     []Column
		init        func(v interface{})
		expect      []interface{}
		hasError    bool
	}{
		{
			description: "simple struct",
			columns:     NamesToColumns([]string{"A1", "a2", "a_3", "A_4", "A5"}),
			targetType: reflect.StructOf([]reflect.StructField{
				{
					Name: "A1",
					Type: reflect.TypeOf(0),
				},
				{
					Name: "A2",
					Type: reflect.TypeOf(""),
				}, {
					Name: "A3",
					Type: reflect.TypeOf(0.0),
				}, {
					Name: "A4",
					Type: reflect.TypeOf(false),
				}, {
					Name: "A5",
					Type: reflect.TypeOf(time.Time{}),
				},
			}),
			init: func(v interface{}) {
				type Case1 struct {
					A1 int
					A2 string
					A3 float64
					A4 bool
					A5 time.Time
				}
				ptr := xunsafe.AsPointer(v)
				s := (*Case1)(ptr)
				s.A1 = 101
				s.A2 = "test"
				s.A3 = 1.44
				s.A4 = true
				s.A5 = now
			},
			expect: []interface{}{101, "test", 1.44, true, now},
		},
		{
			description: "simple struct - tag mapping",
			columns:     NamesToColumns([]string{"A1", "a2", "a_3", "A_4", "A5"}),
			targetType: reflect.StructOf([]reflect.StructField{
				{
					Name: "A11111",
					Tag:  reflect.StructTag("sqlx:\"A1\""),
					Type: reflect.TypeOf(0),
				},
				{
					Name: "A2",
					Type: reflect.TypeOf(""),
				}, {
					Name: "A3",
					Type: reflect.TypeOf(0.0),
				}, {
					Name: "A4",
					Type: reflect.TypeOf(false),
				}, {
					Name: "A5",
					Type: reflect.TypeOf(time.Time{}),
				},
			}),
			init: func(v interface{}) {
				type Case1 struct {
					A1 int
					A2 string
					A3 float64
					A4 bool
					A5 time.Time
				}
				ptr := xunsafe.AsPointer(v)
				s := (*Case1)(ptr)
				s.A1 = 101
				s.A2 = "test"
				s.A3 = 1.44
				s.A4 = true
				s.A5 = now
			},
			expect: []interface{}{101, "test", 1.44, true, now},
		},
		{
			description: "simple struct - error mapping",
			columns:     NamesToColumns([]string{"A1", "a2", "a_3", "A_4", "A5"}),
			hasError:    true,
			targetType: reflect.StructOf([]reflect.StructField{
				{
					Name: "A11111",
					Type: reflect.TypeOf(0),
				},
				{
					Name: "A2",
					Type: reflect.TypeOf(""),
				}, {
					Name: "A3",
					Type: reflect.TypeOf(0.0),
				}, {
					Name: "A4",
					Type: reflect.TypeOf(false),
				}, {
					Name: "A5",
					Type: reflect.TypeOf(time.Time{}),
				},
			}),
			init: func(v interface{}) {
				type T struct {
					A1 int
					A2 string
					A3 float64
					A4 bool
					A5 time.Time
				}
				ptr := xunsafe.AsPointer(v)
				s := (*T)(ptr)
				s.A1 = 101
				s.A2 = "test"
				s.A3 = 1.44
				s.A4 = true
				s.A5 = now
			},
		},
		{
			description: "nested struct mapping",
			columns:     NamesToColumns([]string{"ID", "Z_ID"}),
			targetType: reflect.StructOf([]reflect.StructField{
				{
					Name: "ID",
					Type: reflect.TypeOf(0),
				},
				{
					Name: "Z",
					Tag:  "sqlx:\"ns=z\"",
					Type: reflect.StructOf([]reflect.StructField{
						{
							Name: "ID",
							Type: reflect.TypeOf(0),
						},
					}),
				},
			}),
			init: func(v interface{}) {
				type Z struct {
					ID int
				}
				type T struct {
					ID int
					Z  Z
				}
				ptr := xunsafe.AsPointer(v)
				s := (*T)(ptr)
				s.ID = 101
				s.Z.ID = 303
			},
			expect: []interface{}{101, 303},
		},
		{
			description: "incomplete struct mapping with resolve",
			columns: []Column{
				NewColumn("ID", "", reflect.TypeOf(0)),
				NewColumn("Name", "", reflect.TypeOf("")),
				NewColumn("Active", "", reflect.TypeOf(false)),
			},
			targetType: reflect.StructOf([]reflect.StructField{
				{
					Name: "ID",
					Type: reflect.TypeOf(0),
				},
			}),
			resolve: func(column Column) func(pointer unsafe.Pointer) interface{} {
				return func(pointer unsafe.Pointer) interface{} {
					switch column.Name() {
					case "Name":
						return &unmappedName
					case "Active":
						return &unmappedActive
					}
					return nil
				}
			},
			init: func(v interface{}) {
				type T struct {
					ID int
				}
				ptr := xunsafe.AsPointer(v)
				s := (*T)(ptr)
				s.ID = 101
			},
			expect: []interface{}{101, "xxx", true},
		},
	}

	for _, testCase := range testCases {
		v := reflect.New(testCase.targetType).Interface()
		testCase.init(v)
		matcher := NewMatcher(testCase.resolve)
		matched, err := matcher.Match(testCase.targetType, testCase.columns)

		if testCase.hasError {
			assert.NotNil(t, err, testCase.description)
			continue
		}
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		for i, column := range testCase.columns {
			assert.EqualValues(t, column, matched[i].Column, testCase.description+" "+column.Name())
		}
		ptr := xunsafe.AsPointer(v)
		for i, expect := range testCase.expect {
			field := matched[i]
			actual := field.Addr(ptr)

			if field.Type == nil {
				field.Type = reflect.TypeOf(actual).Elem()
			}
			xt := xunsafe.NewType(field.Type)
			assert.EqualValues(t, xt.Ref(expect), actual, testCase.description+" "+field.Field.Name)
		}
	}

}
