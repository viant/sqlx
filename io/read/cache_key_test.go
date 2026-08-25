package read

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/sqlx/io"
)

type mapperCacheKeyRecord struct {
	ID int
}

func TestMapperCache_KeyIncludesPackagePath(t *testing.T) {
	recordType := reflect.TypeOf(mapperCacheKeyRecord{})
	key, err := NewMapperCache(4).generateKey(recordType, nil)
	if err != nil {
		t.Fatalf("failed to generate mapper cache key: %v", err)
	}

	expectedTypeKey := recordType.PkgPath() + recordType.String()
	if key != expectedTypeKey {
		t.Fatalf("expected type key %q, got %q", expectedTypeKey, key)
	}
	if recordType.PkgPath() == "" || !strings.HasPrefix(key, recordType.PkgPath()) {
		t.Fatalf("expected key %q to include package path %q", key, recordType.PkgPath())
	}
}

func TestMapperCache_KeyIncludesColumnSignature(t *testing.T) {
	recordType := reflect.TypeOf(mapperCacheKeyRecord{})
	mapperCache := NewMapperCache(4)

	first, err := mapperCache.generateKey(recordType, []io.Column{
		io.NewColumn("id", "INTEGER", reflect.TypeOf(int(0))),
		io.NewColumn("name", "TEXT", reflect.TypeOf("")),
	})
	if err != nil {
		t.Fatalf("failed to generate first mapper cache key: %v", err)
	}
	second, err := mapperCache.generateKey(recordType, []io.Column{
		io.NewColumn("id", "INTEGER", reflect.TypeOf(int(0))),
		io.NewColumn("description", "TEXT", reflect.TypeOf("")),
	})
	if err != nil {
		t.Fatalf("failed to generate second mapper cache key: %v", err)
	}

	if first == second {
		t.Fatalf("expected different column sets to produce different keys, got %q", first)
	}
	if !strings.HasSuffix(first, "/id/name") {
		t.Fatalf("expected column signature in key, got %q", first)
	}
}
