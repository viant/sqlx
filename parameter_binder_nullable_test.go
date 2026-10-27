package sqlx

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestParameterBinderPreservesNullableScalarTypes(t *testing.T) {
	for _, value := range []any{(*time.Time)(nil), (*int)(nil), (*string)(nil), (*bool)(nil)} {
		for _, named := range []bool{false, true} {
			var binder *ParameterBinder
			query := "SELECT ?"
			if named {
				query = "SELECT :value"
				binder = NewParameterBinder(func(string) (any, bool, error) { return value, true, nil })
			} else {
				binder = NewParameterBinder(nil, value)
			}
			_, args, err := binder.Bind(query)
			if err != nil || len(args) != 1 || reflect.TypeOf(args[0]) != reflect.TypeOf(value) {
				t.Fatalf("declared nullable type lost: %T", value)
			}
			if err := binder.Complete(); err != nil {
				t.Fatal(err)
			}
			converted, err := driver.DefaultParameterConverter.ConvertValue(args[0])
			if err != nil || converted != nil {
				t.Fatal("classic driver SQL NULL behavior changed")
			}
		}
	}
}

func TestParameterBinderNullableCollectionAndByteCompatibility(t *testing.T) {
	var list *[]int
	var array *[2]int
	var mapping *map[string]int
	bytes := []byte{1, 2}
	listValue := []int{2, 3}
	for _, tc := range []struct {
		name  string
		value any
		args  []any
		query string
	}{
		{"untyped null", nil, []any{nil}, "SELECT ?"},
		{"null list pointer", list, []any{nil}, "SELECT ?"},
		{"null array pointer", array, []any{nil}, "SELECT ?"},
		{"null map pointer", mapping, []any{nil}, "SELECT ?"},
		{"empty list", []int{}, nil, "SELECT NULL"},
		{"list pointer", &listValue, []any{2, 3}, "SELECT ?,?"},
		{"bytes", bytes, []any{bytes}, "SELECT ?"},
		{"null bytes", []byte(nil), []any{[]byte(nil)}, "SELECT ?"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binder := NewParameterBinder(nil, tc.value)
			query, args, err := binder.Bind("SELECT ?")
			if err != nil || query != tc.query || !reflect.DeepEqual(args, tc.args) {
				t.Fatal("collection/byte compatibility changed")
			}
			if err := binder.Complete(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type nullableCollectionValuer []int

func (*nullableCollectionValuer) Value() (driver.Value, error) {
	panic("binder must not execute driver valuer")
}

func TestParameterBinderNullableKeepsValuerAndResolverAuthority(t *testing.T) {
	var value *nullableCollectionValuer
	_, args, err := NewParameterBinder(nil, value).Bind("SELECT ?")
	if err != nil || len(args) != 1 || reflect.TypeOf(args[0]) != reflect.TypeOf(value) {
		t.Fatal("driver valuer priority changed")
	}
	denied := errors.New("denied fixture selector")
	if _, _, err := NewParameterBinder(func(string) (any, bool, error) { return nil, false, denied }).Bind("SELECT :value"); !errors.Is(err, denied) {
		t.Fatal("resolver denial changed")
	}
	if _, _, err := NewParameterBinder(func(string) (any, bool, error) { return nil, false, nil }).Bind("SELECT :missing"); err == nil {
		t.Fatal("missing binding became available")
	}
	number := 7
	_, args, err = NewParameterBinder(nil, &number).Bind("SELECT ?")
	if err != nil || len(args) != 1 || args[0] != &number {
		t.Fatal("populated pointer binding changed")
	}
}
