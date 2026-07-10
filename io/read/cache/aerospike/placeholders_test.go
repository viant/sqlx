package aerospike

import (
	"github.com/viant/sqlx/io/read/cache"
	"reflect"
	"testing"
)

func TestPlaceholders_NullableScalarPlaceholder(t *testing.T) {
	field := &cache.Field{
		ColumnName:     "price",
		ColumnScanType: "float64",
		ColumnNullable: true,
	}

	if err := field.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	placeholders := NewPlaceholders(0, []*cache.Field{field})

	scanValue, ok := placeholders.ScanPlaceholders()[0].(**float64)
	if !ok {
		t.Fatalf("expected scan placeholder type **float64, got %T", placeholders.ScanPlaceholders()[0])
	}

	if got, ok := placeholders.ColumnValue(); !ok || got != nil {
		t.Fatalf("expected nil column value before scan, got type=%v ok=%v", reflect.TypeOf(got), ok)
	}

	if got := placeholders.Values()[0]; got != nil {
		t.Fatalf("expected nil row value before scan, got %v", got)
	}

	value := 12.5
	*scanValue = &value

	if got, ok := placeholders.ColumnValue(); !ok || !reflect.DeepEqual(got, value) {
		t.Fatalf("expected column value %v, got value=%v ok=%v", value, got, ok)
	}

	if got := placeholders.Values()[0]; !reflect.DeepEqual(got, value) {
		t.Fatalf("expected row value %v, got %v", value, got)
	}
}

func TestPlaceholders_NonNullableScalarPlaceholder(t *testing.T) {
	field := &cache.Field{
		ColumnName:     "price",
		ColumnScanType: "float64",
	}

	if err := field.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	placeholders := NewPlaceholders(-1, []*cache.Field{field})
	if _, ok := placeholders.ScanPlaceholders()[0].(*float64); !ok {
		t.Fatalf("expected scan placeholder type *float64, got %T", placeholders.ScanPlaceholders()[0])
	}
}
