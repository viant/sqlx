package sqlx

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseParameters(t *testing.T) {
	for _, tc := range []struct {
		name, SQL         string
		positional, named int
		expanded          string
	}{
		{"mixed", "SELECT ?, :Name, :nested.Value", 1, 2, "SELECT ?,?,?, :Name, :nested.Value"},
		{"protected", "SELECT '?', ':Name', \"?\", `:name`, [?], $q$:name ?$q$, ? -- :ignored ?\n/* :ignored ? */", 1, 0, ""},
		{"cast", "SELECT :Value::int, 1::integer", 0, 1, ""},
		{"escaped", "SELECT 'it''s :name ?', ? /* nested /* ? */ :ignored */", 1, 0, ""},
		{"multiple", "SELECT ?,?", 2, 0, "SELECT ?,?"},
		{"json_operators", "SELECT data ? ?, data ?| array['a'], data ?& array['b'] FROM records WHERE id=:id", 1, 1, ""},
		{"json_path_operator", "SELECT data @? '$.a' FROM records WHERE id=?", 1, 0, ""},
		{"json_literal", "SELECT '{}' /* comment */ ? ?, ? ? 'field'", 2, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed := ParseParameters(tc.SQL)
			if parsed.PositionalCount() != tc.positional || parsed.NamedCount() != tc.named || parsed.Count() != tc.positional+tc.named || parsed.HasPositional() != (tc.positional > 0) {
				t.Fatalf("unexpected counts: %+v", parsed)
			}
			if tc.expanded != "" && parsed.ExpandSinglePositional(3) != tc.expanded {
				t.Fatalf("expanded=%s", parsed.ExpandSinglePositional(3))
			}
		})
	}
}

func TestParameterBinder(t *testing.T) {
	for _, tc := range []struct {
		name, SQL, want      string
		values               map[string]any
		positional, wantArgs []any
		fail                 bool
	}{
		{"mixed", "SELECT ?, :value, ?", "SELECT ?, ?, ?", map[string]any{"value": 2}, []any{1, 3}, []any{1, 2, 3}, false},
		{"repeated", "SELECT :value, :value", "SELECT ?, ?", map[string]any{"value": "x"}, nil, []any{"x", "x"}, false},
		{"slice", "SELECT * FROM t WHERE id IN (:ids)", "SELECT * FROM t WHERE id IN (?,?)", map[string]any{"ids": []int{1, 2}}, nil, []any{1, 2}, false},
		{"empty_slice", "SELECT * FROM t WHERE id IN (:ids)", "SELECT * FROM t WHERE id IN (NULL)", map[string]any{"ids": []int{}}, nil, nil, false},
		{"bytes", "SELECT :data", "SELECT ?", map[string]any{"data": []byte{1, 2}}, nil, []any{[]byte{1, 2}}, false},
		{"missing_named", "SELECT :missing", "", nil, nil, nil, true},
		{"missing_positional", "SELECT ?", "", nil, nil, nil, true},
		{"extra_positional", "SELECT 1", "", nil, []any{1}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binder := NewParameterBinder(func(name string) (any, bool, error) { value, ok := tc.values[name]; return value, ok, nil }, tc.positional...)
			SQL, args, err := binder.Bind(tc.SQL)
			if err == nil {
				err = binder.Complete()
			}
			if tc.fail {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil || SQL != tc.want || !reflect.DeepEqual(args, tc.wantArgs) {
				t.Fatalf("Bind=%q,%#v,%v", SQL, args, err)
			}
		})
	}
}

func TestParameterBinderSegments(t *testing.T) {
	binder := NewParameterBinder(nil, 1, 2)
	for _, want := range []int{1, 2} {
		SQL, args, err := binder.Bind("SELECT ?")
		if err != nil || SQL != "SELECT ?" || !reflect.DeepEqual(args, []any{want}) {
			t.Fatalf("Bind=%s,%v,%v", SQL, args, err)
		}
	}
	if err := binder.Complete(); err != nil {
		t.Fatal(err)
	}
	_, _, err := NewParameterBinder(func(string) (any, bool, error) { return nil, false, errors.New("resolver failed") }).Bind("SELECT :value")
	if err == nil {
		t.Fatal("resolver error lost")
	}
}
