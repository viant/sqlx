package io

import (
	"database/sql/driver"
	"encoding/csv"
	"encoding/json"
	"fmt"
	stdio "io"
	"reflect"
	"strconv"
	"strings"
)

// CSVEncodedValue bridges one CSV SQL value and a typed Go scalar slice.
type CSVEncodedValue struct {
	Val any
	raw any
}

func (c *CSVEncodedValue) Scan(raw any) error {
	if bytes, ok := raw.([]byte); ok {
		raw = string(bytes)
	}
	c.raw = raw
	value := reflect.ValueOf(c.Val)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("CSV destination must be a pointer")
	}
	value = value.Elem()
	if raw == nil {
		value.SetZero()
		return nil
	}
	text, ok := raw.(string)
	if !ok {
		return fmt.Errorf("CSV source must be text, got %T", raw)
	}
	parser := csv.NewReader(strings.NewReader(text))
	record, err := parser.Read()
	if err != nil && err != stdio.EOF {
		return err
	}
	if err == nil {
		if _, err = parser.Read(); err != stdio.EOF {
			if err == nil {
				return fmt.Errorf("CSV SQL value contains multiple records")
			}
			return err
		}
	}
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			value.Set(reflect.New(value.Type().Elem()))
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Slice {
		return fmt.Errorf("CSV destination must be a slice")
	}
	result := reflect.MakeSlice(value.Type(), len(record), len(record))
	for i, text := range record {
		if err := c.set(result.Index(i), text); err != nil {
			return fmt.Errorf("CSV field %d: %w", i+1, err)
		}
	}
	value.Set(result)
	return nil
}

func (c *CSVEncodedValue) set(value reflect.Value, text string) error {
	for value.Kind() == reflect.Pointer {
		value.Set(reflect.New(value.Type().Elem()))
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.String:
		value.SetString(text)
	case reflect.Bool:
		parsed, err := strconv.ParseBool(text)
		if err != nil {
			return err
		}
		value.SetBool(parsed)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		parsed, err := strconv.ParseInt(text, 10, value.Type().Bits())
		if err != nil {
			return err
		}
		value.SetInt(parsed)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		parsed, err := strconv.ParseUint(text, 10, value.Type().Bits())
		if err != nil {
			return err
		}
		value.SetUint(parsed)
	case reflect.Float32, reflect.Float64:
		parsed, err := strconv.ParseFloat(text, value.Type().Bits())
		if err != nil {
			return err
		}
		value.SetFloat(parsed)
	default:
		return fmt.Errorf("unsupported CSV element type %s", value.Type())
	}
	return nil
}

func (c *CSVEncodedValue) Value() (driver.Value, error) {
	if c.Val == nil {
		return c.raw, nil
	}
	value := reflect.ValueOf(c.Val)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, nil
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Slice {
		return nil, fmt.Errorf("CSV value must be a slice")
	}
	if value.IsNil() {
		return nil, nil
	}
	record := make([]string, value.Len())
	for i := range record {
		field := value.Index(i)
		for field.Kind() == reflect.Pointer {
			if field.IsNil() {
				break
			}
			field = field.Elem()
		}
		if field.Kind() == reflect.Pointer && field.IsNil() {
			continue
		}
		record[i] = fmt.Sprint(field.Interface())
	}
	var text strings.Builder
	writer := csv.NewWriter(&text)
	if err := writer.Write(record); err != nil {
		return nil, err
	}
	writer.Flush()
	return strings.TrimSuffix(text.String(), "\n"), writer.Error()
}

// Cache serialization retains the original source, never decoded slice data.
func (c *CSVEncodedValue) MarshalJSON() ([]byte, error)    { return json.Marshal(c.raw) }
func (c *CSVEncodedValue) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &c.raw) }
