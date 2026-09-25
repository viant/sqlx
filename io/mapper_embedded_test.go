package io_test

import (
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
)

type EmbeddedTupleKey struct {
	Tenant int `sqlx:"tenant_id"`
	ID     int `sqlx:"id,primaryKey=true"`
}
type EmbeddedPointerKey struct {
	Ignored int `sqlx:"-"`
	*EmbeddedTupleKey
}
type EmbeddedValueKey struct {
	Ignored int `sqlx:"-"`
	EmbeddedTupleKey
}
type NestedPointerKey struct {
	Ignored int `sqlx:"-"`
	*EmbeddedPointerKey
}
type PointerValueKey struct {
	Ignored int `sqlx:"-"`
	*EmbeddedValueKey
}
type EmbeddedEncoded struct {
	JSON []int    `sqlx:"json_value,enc=JSON"`
	CSV  []string `sqlx:"csv_value,enc=CSV"`
}

type rawJSONScalar struct {
	Failure *string `sqlx:"failure_json,enc=RAW"`
}

func TestStructColumnMapperEmbeddedHolderValues(t *testing.T) {
	for _, test := range []struct {
		name    string
		record  any
		columns []string
		args    []driver.Value
	}{
		{"flat", &EmbeddedTupleKey{2, 7}, []string{"tenant_id", "id"}, []driver.Value{int64(2), int64(7)}},
		{"pointer", &EmbeddedPointerKey{99, &EmbeddedTupleKey{2, 7}}, []string{"tenant_id", "id"}, []driver.Value{int64(2), int64(7)}},
		{"nil pointer", &EmbeddedPointerKey{Ignored: 99}, []string{"tenant_id", "id"}, []driver.Value{nil, nil}},
		{"value", &EmbeddedValueKey{99, EmbeddedTupleKey{2, 7}}, []string{"tenant_id", "id"}, []driver.Value{int64(2), int64(7)}},
		{"nested pointer", &NestedPointerKey{98, &EmbeddedPointerKey{99, &EmbeddedTupleKey{2, 7}}}, []string{"tenant_id", "id"}, []driver.Value{int64(2), int64(7)}},
		{"nested inner nil", &NestedPointerKey{98, &EmbeddedPointerKey{Ignored: 99}}, []string{"tenant_id", "id"}, []driver.Value{nil, nil}},
		{"nested outer nil", &NestedPointerKey{Ignored: 98}, []string{"tenant_id", "id"}, []driver.Value{nil, nil}},
		{"pointer then value", &PointerValueKey{98, &EmbeddedValueKey{99, EmbeddedTupleKey{2, 7}}}, []string{"tenant_id", "id"}, []driver.Value{int64(2), int64(7)}},
		{"value then pointer", &struct {
			Ignored int `sqlx:"-"`
			EmbeddedPointerKey
		}{98, EmbeddedPointerKey{99, &EmbeddedTupleKey{2, 7}}}, []string{"tenant_id", "id"}, []driver.Value{int64(2), int64(7)}},
		{"nested values", &struct {
			Ignored int `sqlx:"-"`
			EmbeddedValueKey
		}{98, EmbeddedValueKey{99, EmbeddedTupleKey{2, 7}}}, []string{"tenant_id", "id"}, []driver.Value{int64(2), int64(7)}},
		{"encoders", &struct {
			Ignored int `sqlx:"-"`
			*EmbeddedEncoded
		}{99, &EmbeddedEncoded{[]int{2, 7}, []string{"a", "b"}}}, []string{"json_value", "csv_value"}, []driver.Value{"[2,7]", "a,b"}},
		{"raw nil", &rawJSONScalar{}, []string{"failure_json"}, []driver.Value{nil}},
		{"raw scalar", &rawJSONScalar{Failure: func() *string { value := `[]`; return &value }()}, []string{"failure_json"}, []driver.Value{`[]`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			columns, binder, err := io.StructColumnMapper(reflect.TypeOf(test.record), option.StructOrderedColumns(true))
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, column := range columns {
				names = append(names, column.Name())
			}
			if !reflect.DeepEqual(names, test.columns) {
				t.Fatalf("columns=%v", names)
			}
			values := make([]interface{}, len(columns))
			binder(test.record, values, 0, len(columns))
			actual := make([]driver.Value, len(values))
			for index, value := range values {
				actual[index], err = driver.DefaultParameterConverter.ConvertValue(value)
				if err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(actual, test.args) {
				t.Fatalf("args=%#v want=%#v", actual, test.args)
			}
			if test.name == "nil pointer" && test.record.(*EmbeddedPointerKey).EmbeddedTupleKey != nil {
				t.Fatal("mapper allocated nil holder")
			}
		})
	}
}

func TestStructColumnMapperEmbeddedIdentityOrderAndWindow(t *testing.T) {
	record := &struct {
		Ignored int `sqlx:"-"`
		*EmbeddedTupleKey
		Name string `sqlx:"name"`
	}{99, &EmbeddedTupleKey{2, 7}, "value"}
	columns, binder, err := io.StructColumnMapper(record)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, column := range columns {
		names = append(names, column.Name())
	}
	if !reflect.DeepEqual(names, []string{"tenant_id", "name", "id"}) {
		t.Fatalf("identity order changed: %v", names)
	}
	values := make([]interface{}, 1)
	binder(record, values, 2, 1)
	actual, err := driver.DefaultParameterConverter.ConvertValue(values[0])
	if err != nil || actual != int64(7) {
		t.Fatalf("window=%v error=%v", actual, err)
	}
}
