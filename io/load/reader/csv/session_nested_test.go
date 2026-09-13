package csv

import (
	"reflect"
	"strings"
	"testing"
)

func TestMarshallerNestedObjectSession(t *testing.T) {
	type profile struct{ Name string }
	type valueRow struct {
		ID      int
		Profile profile
	}
	type pointerRow struct {
		ID      int
		Profile *profile
	}
	for _, tc := range []struct {
		name string
		row  any
		rows any
	}{{"value", valueRow{}, []valueRow{{ID: 1, Profile: profile{Name: "Ada"}}}}, {"pointer", pointerRow{}, []pointerRow{{ID: 1, Profile: &profile{Name: "Ada"}}}}} {
		t.Run(tc.name, func(t *testing.T) {
			marshaller, err := NewMarshaller(reflect.TypeOf(tc.row), nil)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := marshaller.Marshal(tc.rows)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(encoded), "Ada") {
				t.Fatalf("encoded=%s", encoded)
			}
		})
	}
}
