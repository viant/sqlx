package aerospike

import (
	"reflect"
	"testing"

	"github.com/viant/sqlx/io/read/cache"
)

func TestPlaceholders_PointerByteSliceFromDriver(t *testing.T) {
	for _, scanType := range []string{"[]uint8", "*[]uint8"} {
		t.Run(scanType, func(t *testing.T) {
			testPlaceholdersByteSliceFromDriver(t, scanType)
		})
	}
}

func testPlaceholdersByteSliceFromDriver(t *testing.T, scanType string) {
	t.Helper()
	field := &cache.Field{
		ColumnName:     "household_reach_sketch",
		ColumnScanType: scanType,
	}
	if err := field.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	placeholders := NewPlaceholders(-1, []*cache.Field{field})
	scanner, ok := placeholders.ScanPlaceholders()[0].(*byteSlicePlaceholder)
	if !ok {
		t.Fatalf("scan placeholder type = %T, want *byteSlicePlaceholder", placeholders.ScanPlaceholders()[0])
	}

	source := []byte{1, 2, 3}
	if err := scanner.Scan(&source); err != nil {
		t.Fatalf("Scan(*[]byte) error = %v", err)
	}
	source[0] = 9
	if got := placeholders.Values()[0]; !reflect.DeepEqual(got, []byte{1, 2, 3}) {
		t.Fatalf("Values() = %v, want copied bytes", got)
	}

	if err := scanner.Scan([]byte{4, 5}); err != nil {
		t.Fatalf("Scan([]byte) error = %v", err)
	}
	if got := placeholders.Values()[0]; !reflect.DeepEqual(got, []byte{4, 5}) {
		t.Fatalf("Values() = %v, want []byte", got)
	}

	if err := scanner.Scan(nil); err != nil {
		t.Fatalf("Scan(nil) error = %v", err)
	}
	if got := placeholders.Values()[0]; got != nil {
		t.Fatalf("Values() after NULL = %v, want nil", got)
	}

	var nilBytes []byte
	for _, test := range []struct {
		name   string
		source interface{}
		want   interface{}
	}{
		{"string", "hello", []byte("hello")},
		{"empty string", "", []byte{}},
		{"empty bytes", []byte{}, []byte{}},
		{"nil bytes", nilBytes, nil},
		{"nil pointer", (*[]byte)(nil), nil},
		{"pointer to nil bytes", &nilBytes, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Reuse a populated scanner to verify NULL clears previous data.
			if err := scanner.Scan([]byte{1}); err != nil {
				t.Fatalf("seed Scan() error = %v", err)
			}
			if err := scanner.Scan(test.source); err != nil {
				t.Fatalf("Scan(%T) error = %v", test.source, err)
			}
			if got := placeholders.Values()[0]; !reflect.DeepEqual(got, test.want) {
				t.Fatalf("Values() = %#v, want %#v", got, test.want)
			}
		})
	}
	if err := scanner.Scan(struct{}{}); err == nil {
		t.Fatal("Scan(unsupported type) should return an error")
	}
}

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
