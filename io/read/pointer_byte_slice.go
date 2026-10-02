package read

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
)

// pointerByteSliceScanner bridges drivers that return a pointer to a byte
// slice for fields represented by a *[]byte model field. database/sql can
// assign []byte to *[]byte, but it does not dereference a *[]byte source.
//
// It also exposes the original destination so cache type discovery and cache
// serialization keep using the model field rather than this adapter.
type pointerByteSliceScanner struct {
	destination any
}

var _ sql.Scanner = (*pointerByteSliceScanner)(nil)

func newPointerByteSliceScanner(destination any) *pointerByteSliceScanner {
	return &pointerByteSliceScanner{destination: destination}
}

func (s *pointerByteSliceScanner) Destination() any {
	return s.destination
}

func (s *pointerByteSliceScanner) Scan(source any) error {
	destination := reflect.ValueOf(s.destination)
	if destination.Kind() != reflect.Ptr || destination.IsNil() || destination.Elem().Kind() != reflect.Ptr {
		return fmt.Errorf("expected pointer to pointer-to-byte-slice destination, got %T", s.destination)
	}

	field := destination.Elem()
	if source == nil {
		field.SetZero()
		return nil
	}

	value := reflect.ValueOf(source)
	for value.Kind() == reflect.Ptr {
		if value.IsNil() {
			field.SetZero()
			return nil
		}
		value = value.Elem()
	}

	byteSliceType := field.Type().Elem()
	if value.Type().AssignableTo(byteSliceType) {
		bytes := append([]byte(nil), value.Bytes()...)
		assigned := reflect.New(byteSliceType)
		assigned.Elem().Set(reflect.ValueOf(bytes).Convert(byteSliceType))
		field.Set(assigned)
		return nil
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %s", source, field.Type())
}

func (s *pointerByteSliceScanner) MarshalJSON() ([]byte, error) {
	destination := reflect.ValueOf(s.destination)
	if destination.Kind() != reflect.Ptr || destination.IsNil() || destination.Elem().Kind() != reflect.Ptr {
		return nil, fmt.Errorf("expected pointer to pointer-to-byte-slice destination, got %T", s.destination)
	}
	field := destination.Elem()
	if field.IsNil() {
		return []byte("null"), nil
	}
	return json.Marshal(field.Elem().Interface())
}
