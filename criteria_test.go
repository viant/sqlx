package sqlx

import (
	"fmt"
	"reflect"
	"testing"
)

func TestCriteriaSQL(t *testing.T) {
	type useCase struct {
		desc   string
		input  Criteria
		expect string
		fail   bool
	}
	for _, test := range []useCase{
		{desc: "ordered positional parameters", input: Criteria{Expression: "owner = ? AND attempt >= ?", Placeholders: []any{"worker", 0}}, expect: "owner = $4 AND attempt >= $5"},
		{desc: "literal and comment question marks remain unchanged", input: Criteria{Expression: "name = '?' AND id = ? /* ? */", Placeholders: []any{7}}, expect: "name = '?' AND id = $4 /* ? */"},
		{desc: "empty criteria", expect: ""},
		{desc: "missing value", input: Criteria{Expression: "id = ?"}, fail: true},
		{desc: "extra value", input: Criteria{Expression: "id IS NULL", Placeholders: []any{1}}, fail: true},
		{desc: "named placeholders cannot bypass binding", input: Criteria{Expression: "id = :ID", Placeholders: []any{1}}, fail: true},
	} {
		t.Run(test.desc, func(t *testing.T) {
			original := test.input.Clone()
			index := 3
			actual, err := test.input.SQL(func() string { index++; return fmt.Sprintf("$%d", index) })
			if (err != nil) != test.fail {
				t.Fatalf("err=%v", err)
			}
			if !test.fail && actual != test.expect {
				t.Fatalf("sql=%q expected=%q", actual, test.expect)
			}
			if !reflect.DeepEqual(original, &test.input) {
				t.Fatal("source criteria was mutated")
			}
		})
	}
}
