package io

import (
	"reflect"
	"testing"
)

func TestCSVEncodedValue(t *testing.T) {
	for _, tc := range []struct {
		name   string
		raw    any
		target any
		want   any
		fail   bool
	}{
		{"quoted_strings", `"a,b",c`, new([]string), []string{"a,b", "c"}, false},
		{"integer", []byte("1,2"), new([]int), []int{1, 2}, false},
		{"boolean", "true,false", new([]bool), []bool{true, false}, false},
		{"overflow", "128", new([]int8), nil, true},
		{"multiline", "a\nb", new([]string), nil, true},
		{"null", nil, &[]string{"old"}, []string(nil), false},
		{"empty", "", new([]string), []string{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := &CSVEncodedValue{Val: tc.target}
			err := value.Scan(tc.raw)
			if tc.fail {
				if err == nil {
					t.Fatal("invalid CSV accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(reflect.ValueOf(tc.target).Elem().Interface(), tc.want) {
				t.Fatalf("value=%v,want=%v", tc.target, tc.want)
			}
			if _, err := value.Value(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
