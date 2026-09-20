package sqlx

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func TestParameterScanContext(t *testing.T) {
	for _, tc := range []struct {
		SQL               string
		positional, named int
	}{
		{"? ? ? ?", 2, 0},
		{"SELECT /* a /* ? */ b */ ? -- ?\n, ?", 2, 0},
		{"SELECT data /* ? */ ? /* ? */ ?, data ?| ?, data ?& ?, data @? ?", 4, 0},
		{"SELECT :doc /* ? */ ? :key, ? /* x */ ? 'key', :nested.Value::int", 1, 3},
		{"SELECT '?' ? ?, \"?\" ? ?, `?` ? ?, [?] ? ?, $$?$$ ? ?, $tag$?$tag$ ? ?", 6, 0},
		{"SELECT 'it''s ?' ? ?, 'a\\'?' ? ?, [a]]?] ? ?", 3, 0},
		{"SELECT ?, '? unterminated :ignored", 1, 0},
		{"SELECT :ok, /* unterminated ? :ignored", 0, 1},
		{"SELECT ?\t\r\n\f\v /* a */ -- b\n ? ?, :a.b2, 1::int", 2, 1},
	} {
		t.Run(tc.SQL, func(t *testing.T) {
			p := ParseParameters(tc.SQL)
			if p.PositionalCount() != tc.positional || p.NamedCount() != tc.named || p.SQL != tc.SQL {
				t.Fatalf("parameters=%+v positional=%d named=%d", p.items, p.PositionalCount(), p.NamedCount())
			}
		})
	}
}

// Pin byte offsets, names and expansion against the pre-optimization scanner,
// including malformed SQL. The seed makes the corpus reproducible.
func TestParameterScanCompatibility(t *testing.T) {
	fragments := []string{"?", "?|", "?&", "@?", "SELECT", "where", "data", ":name", ":nested.Value", "::int", "'it''s ?'", "'a\\'?'", "\"?\"", "`?`", "[?]", "[a]]b]", "$$?$$", "$tag$:a ?$tag$", "/* ? */", "/* a /* ? */ b */", "-- ?\n", " ", "\t\r\n\f\v", ",", "(", ")", "'", "/*", "$tag$", ":", "1", "_a", "\xc3\xa9", "\x00"}
	rng := rand.New(rand.NewSource(42))
	hash := sha256.New()
	for n := 0; n < 10000; n++ {
		var SQL strings.Builder
		for j, count := 0, 1+rng.Intn(20); j < count; j++ {
			SQL.WriteString(fragments[rng.Intn(len(fragments))])
		}
		p := ParseParameters(SQL.String())
		fmt.Fprintf(hash, "%q:%+v:%q\n", p.SQL, p.items, p.ExpandSinglePositional(3))
	}
	t.Logf("compatibility SHA256: %x", hash.Sum(nil))
	const want = "520c9c701c9f8aee55fdbf9242f344fa04e67c7c67320a2c5fc64374929b43c3"
	if got := fmt.Sprintf("%x", hash.Sum(nil)); got != want {
		t.Fatalf("scanner semantics changed: SHA256=%s want=%s", got, want)
	}
}

func TestParameterScanSQLiteLimit(t *testing.T) {
	const count = 32766
	SQL := "SELECT * FROM children WHERE enabled IN (" + strings.Repeat("?,", count-1) + "?) AND tenant_id=:tenant"
	p := ParseParameters(SQL)
	if p.PositionalCount() != count || p.NamedCount() != 1 {
		t.Fatalf("positional=%d named=%d", p.PositionalCount(), p.NamedCount())
	}
	for i := 0; i < count; i++ {
		want := len("SELECT * FROM children WHERE enabled IN (") + 2*i
		if p.items[i] != (parameter{start: want, end: want + 1}) {
			t.Fatalf("placeholder %d: %+v", i, p.items[i])
		}
	}
}

func TestParameterScanBindingPreservesOperators(t *testing.T) {
	SQL := "SELECT data /* ? */ ? :key, '?' FROM t WHERE id IN (:ids) AND flag=? -- ?"
	binder := NewParameterBinder(func(name string) (any, bool, error) {
		values := map[string]any{"key": "field", "ids": []int{1, 2}}
		value, ok := values[name]
		return value, ok, nil
	}, true)
	got, args, err := binder.Bind(SQL)
	want := "SELECT data /* ? */ ? ?, '?' FROM t WHERE id IN (?,?) AND flag=? -- ?"
	if err != nil || got != want || !reflect.DeepEqual(args, []any{"field", 1, 2, true}) {
		t.Fatalf("Bind=%q,%v,%v", got, args, err)
	}
	if err := binder.Complete(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkParameterScanScaling(b *testing.B) {
	for _, kind := range []string{"placeholders", "protected"} {
		for _, count := range []int{512, 2048, 8192, 32766} {
			b.Run(fmt.Sprintf("%s/%d", kind, count), func(b *testing.B) {
				fragment := "?,"
				if kind == "protected" {
					fragment = "'?' /* nested /* ? */ x */ ? ?, :nested.Value,"
				}
				SQL := "SELECT " + strings.Repeat(fragment, count) + "1"
				b.ReportAllocs()
				b.SetBytes(int64(len(SQL)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					p := ParseParameters(SQL)
					if p.PositionalCount() != count {
						b.Fatalf("positional=%d want=%d", p.PositionalCount(), count)
					}
				}
			})
		}
	}
}
