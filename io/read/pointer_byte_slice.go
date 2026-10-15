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
	if destination.Kind() != reflect.Ptr || destination.IsNil() {
		return fmt.Errorf("expected pointer-to-byte-slice destination, got %T", s.destination)
	}

	field := destination.Elem()
	if field.Kind() != reflect.Slice && field.Kind() != reflect.Ptr {
		return fmt.Errorf("expected pointer-to-byte-slice destination, got %T", s.destination)
	}
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

	byteSliceType := field.Type()
	if field.Kind() == reflect.Ptr {
		byteSliceType = byteSliceType.Elem()
	}
	if value.Type().AssignableTo(byteSliceType) {
		bytes := append([]byte(nil), value.Bytes()...)
		assigned := reflect.ValueOf(bytes).Convert(byteSliceType)
		if field.Kind() == reflect.Ptr {
			pointer := reflect.New(byteSliceType)
			pointer.Elem().Set(assigned)
			field.Set(pointer)
		} else {
			field.Set(assigned)
		}
		return nil
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %s", source, field.Type())
}

func (s *pointerByteSliceScanner) MarshalJSON() ([]byte, error) {
	destination := reflect.ValueOf(s.destination)
	if destination.Kind() != reflect.Ptr || destination.IsNil() {
		return nil, fmt.Errorf("expected pointer-to-byte-slice destination, got %T", s.destination)
	}
	field := destination.Elem()
	if field.Kind() == reflect.Ptr {
		if field.IsNil() {
			return []byte("null"), nil
		}
		field = field.Elem()
	}
	if field.Kind() != reflect.Slice || field.IsNil() {
		return []byte("null"), nil
	}
	return json.Marshal(field.Interface())
}
