package cache

import (
	"bytes"
	"encoding/json"
	"github.com/francoispqt/gojay"
	"reflect"
	"strings"
	"testing"
)

func TestEscapedStringCacheReplay(t *testing.T) {
	for _, value := range []string{"", "plain", "quotes \" slashes \\ newline\n tab\t", "emoji 😀 and 世界", strings.Repeat(`{"field":"value\\with\"quotes"}`, 32000)} {
		raw, err := json.Marshal([]any{value, nil, "tail"})
		if err != nil {
			t.Fatal(err)
		}
		original := append([]byte(nil), raw...)
		for i := 0; i < 3; i++ {
			d := NewDecoder([]reflect.Type{reflect.TypeOf(""), reflect.TypeOf((*string)(nil)), reflect.TypeOf(" ")}, raw)
			if err = gojay.UnmarshalJSONArray(raw, d); err != nil {
				t.Fatal(err)
			}
			if *d.values[0].(*string) != value || d.values[1] != nil || *d.values[2].(*string) != "tail" {
				t.Fatal("string/null/tail changed")
			}
			if !bytes.Equal(raw, original) {
				t.Fatal("cache payload mutated")
			}
		}
	}
}
func TestInvalidCachedStrings(t *testing.T) {
	for _, raw := range []string{`["bad\q"]`, `["unterminated]`, `["ok"false]`, `[123]`, "[\"raw\nnewline\"]"} {
		d := NewDecoder([]reflect.Type{reflect.TypeOf("")}, []byte(raw))
		if err := gojay.UnmarshalJSONArray([]byte(raw), d); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
func BenchmarkEscapedStringCacheReplay(b *testing.B) {
	value := strings.Repeat(`{"field":"value\\with\"quotes"}`, 32000)
	raw, _ := json.Marshal([]any{value})
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := NewDecoder([]reflect.Type{reflect.TypeOf("")}, raw)
		if err := gojay.UnmarshalJSONArray(raw, d); err != nil {
			b.Fatal(err)
		}
	}
}
