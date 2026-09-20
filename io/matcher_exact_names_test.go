package io

import (
	"reflect"
	"strings"
	"testing"
)

func TestMatcherExactTagsOutrankEarlierFuzzyNames(t *testing.T) {
	type row struct {
		B_ID   int `sqlx:"b_id"`
		BID    int `sqlx:"bid"`
		Dotted int `sqlx:"b.id"`
	}
	for _, names := range [][]string{{"b_id", "bid", "b.id"}, {"bid", "b_id", "b.id"}} {
		columns := make([]Column, len(names))
		for i, name := range names {
			columns[i] = NewColumn(name, "", reflect.TypeFor[int]())
		}
		matched, err := NewMatcher(nil).Match(reflect.TypeFor[row](), columns)
		if err != nil {
			t.Fatal(err)
		}
		expected := map[string]string{"b_id": "B_ID", "bid": "BID", "b.id": "Dotted"}
		for i, field := range matched {
			if field.Field.Name != expected[names[i]] {
				t.Fatalf("%s matched %s, expected %s", names[i], field.Field.Name, expected[names[i]])
			}
		}
	}
}

func TestMatcherExplicitAlternativeMappings(t *testing.T) {
	type mapped struct {
		Value int `sqlx:"bid|b_id|b.id"`
	}
	for _, name := range []string{"bid", "b_id", "b.id"} {
		_, err := NewMatcher(nil).Match(reflect.TypeFor[mapped](), []Column{NewColumn(name, "", reflect.TypeFor[int]())})
		if err != nil {
			t.Fatalf("explicit mapping %s rejected: %v", name, err)
		}
	}
}

func TestMatcherRejectsDuplicateOutputNames(t *testing.T) {
	type row struct{ ID int }
	_, err := NewMatcher(nil).Match(reflect.TypeFor[row](), []Column{NewColumn("id", "", reflect.TypeFor[int]()), NewColumn("ID", "", reflect.TypeFor[int]())})
	if err == nil || !strings.Contains(err.Error(), "assign distinct SQL aliases") {
		t.Fatalf("duplicate outputs not rejected: %v", err)
	}
}

func TestMatcherExplicitTagsDoNotGainPunctuationVariants(t *testing.T) {
	type row struct {
		B_ID int `sqlx:"b_id"`
	}
	for _, name := range []string{"bid", "b.id"} {
		_, err := NewMatcher(nil).Match(reflect.TypeFor[row](), []Column{NewColumn(name, "", reflect.TypeFor[int]())})
		if err == nil {
			t.Fatalf("explicit b_id tag acquired alternate column %s", name)
		}
	}
}
