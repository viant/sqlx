package io

import (
	"reflect"
	"testing"

	"github.com/viant/xreflect"
)

func TestNormalizeColumnTypeMissingScanMetadata(t *testing.T) {
	got := NormalizeColumnType(nil, "unknown_driver_type")
	if got != xreflect.InterfaceType {
		t.Fatalf("expected interface type for missing scan metadata, got %v", got)
	}

	// A known database type should still use the database type even when the
	// driver omits ScanType.
	got = NormalizeColumnType(nil, "varchar")
	if got != reflect.TypeOf("") {
		t.Fatalf("expected string type for varchar, got %v", got)
	}
}
