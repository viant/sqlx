package sqlx

import (
	"strconv"
	"testing"
)

func TestParametersRewritePositional(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{"SELECT ?, :named, ?", "SELECT :p0, :named, :p1"},
		{"SELECT '?', \"?\", `?`, [?], $q$?$q$, ? -- ?\n/* ? */", "SELECT '?', \"?\", `?`, [?], $q$?$q$, :p0 -- ?\n/* ? */"},
		{"SELECT data ? ?, data ?| array['a'], data ?& array['b'], data @? '$.a', ? ? 'field'", "SELECT data ? :p0, data ?| array['a'], data ?& array['b'], data @? '$.a', :p1 ? 'field'"},
		{"SELECT :value::int", "SELECT :value::int"},
		{"SELECT AS STRUCT ?, ? UNION ALL SELECT AS STRUCT ?, ?", "SELECT AS STRUCT :p0, :p1 UNION ALL SELECT AS STRUCT :p2, :p3"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			parsed := ParseParameters(tc.sql)
			for i := 0; i < 2; i++ {
				got := parsed.RewritePositional(func(index int) string { return ":p" + strconv.Itoa(index) })
				if got != tc.want {
					t.Fatalf("got %q, want %q", got, tc.want)
				}
			}
			if parsed.RewritePositional(nil) != tc.sql || parsed.SQL != tc.sql {
				t.Fatal("parsed source mutated")
			}
		})
	}
	if (*Parameters)(nil).RewritePositional(nil) != "" {
		t.Fatal("nil parameters")
	}
}
