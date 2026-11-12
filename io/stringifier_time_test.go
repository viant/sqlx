package io

import (
	"reflect"
	"testing"
	"time"
)

func TestStringifierTimeLayout(t *testing.T) {
	type row struct {
		Value           time.Time
		Pointer         *time.Time
		Nil             *time.Time
		Empty           time.Time  `sqlx:"nullifyEmpty=true"`
		Explicit        time.Time  `timeLayout:"2006-01-02 15:04:05.000000"`
		ExplicitPointer *time.Time `timeLayout:"2006-01-02 15:04:05.000000"`
		Dynamic         interface{}
	}
	for _, zone := range []*time.Location{time.UTC, time.FixedZone("east", 5*3600+30*60), time.FixedZone("west", -7*3600)} {
		for _, layout := range []StringifierTimeLayout{"", "2006-01-02T15:04:05-07:00"} {
			t.Run(zone.String()+"/"+string(layout), func(t *testing.T) {
				value := time.Date(2026, 11, 9, 12, 34, 56, 123456000, zone)
				input := &row{Value: value, Pointer: &value, Explicit: value, ExplicitPointer: &value, Dynamic: value}
				fn, err := TypeStringifier(reflect.TypeOf(row{}), "NULL", true, layout).Stringifier()
				if err != nil {
					t.Fatal(err)
				}
				actual, strings := fn(input)
				defaultLayout := string(layout)
				if defaultLayout == "" {
					defaultLayout = time.RFC3339
				}
				formatted := value.Format(defaultLayout)
				explicit := "2026-11-09 12:34:56.123456"
				want := []string{formatted, formatted, "NULL", "NULL", explicit, explicit, formatted}
				if !reflect.DeepEqual(actual, want) {
					t.Fatalf("got %v, want %v", actual, want)
				}
				if !reflect.DeepEqual(strings, []bool{true, true, false, false, true, true, true}) {
					t.Fatalf("string flags: %v", strings)
				}
			})
		}
	}
}
