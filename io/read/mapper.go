package read

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
	"reflect"
)

//RowMapper represents a target values mapped to pointer of slice
type RowMapper func(target interface{}) ([]interface{}, error)

//NewRowMapper  new a row mapper function
type NewRowMapper func(columns []io.Column, targetType reflect.Type, tagName string, resolver io.Resolve) (RowMapper, error)

//newRowMapper creates a new record mapped
func newRowMapper(columns []io.Column, targetType reflect.Type, tagName string, resolver io.Resolve) (RowMapper, error) {
	if tagName == "" {
		tagName = option.TagSqlx
	}
	switch targetType.Kind() {
	case reflect.Struct:
		return NewStructMapper(columns, targetType, tagName, resolver)
	case reflect.Ptr:
		if targetType.Elem().Kind() == reflect.Struct {
			return NewStructMapper(columns, targetType.Elem(), tagName, resolver)
		}
	}
	return GenericRowMapper(columns)
}

//NewStructMapper creates a new record mapper for supplied struct type
func NewStructMapper(columns []io.Column, recordType reflect.Type, tagName string, resolver io.Resolve) (RowMapper, error) {
	matcher := io.NewMatcher(tagName, resolver)
	matched, err := matcher.Match(recordType, columns)
	if err != nil {
		return nil, err
	}
	var record = make([]interface{}, len(matched))
	var mapper = func(target interface{}) ([]interface{}, error) {
		ptr := xunsafe.AsPointer(target)
		for i, mapped := range matched {
			record[i] = mapped.Addr(ptr)
		}
		return record, nil
	}
	return mapper, nil
}

//GenericRowMapper creates a new row mapper for supplied slice or map type
func GenericRowMapper(columns []io.Column) (RowMapper, error) {
	var valueProviders = make([]func(index int, values []interface{}), len(columns))
	for i, column := range columns {
		valueProviders[i] = newScanValue(column.ScanType())
	}
	mapper := func(target interface{}) ([]interface{}, error) {
		record, ok := target.([]interface{})
		if !ok || len(record) != len(columns) {
			record = make([]interface{}, len(columns))
		}
		for i := range columns {
			valueProviders[i](i, record)
		}
		return record, nil
	}
	return mapper, nil
}

func newScanValue(scanType reflect.Type) func(index int, values []interface{}) {
	switch scanType.Kind() {
	case reflect.Ptr:
		switch scanType.Elem().Kind() {
		case reflect.Int:
			return func(index int, values []interface{}) {
				val := 0
				valPtr := &val
				values[index] = &valPtr
			}
		case reflect.Int64:
			return func(index int, values []interface{}) {
				val := int64(0)
				valPtr := &val
				values[index] = &valPtr
			}
		case reflect.Float64:
			return func(index int, values []interface{}) {
				val := float64(0)
				valPtr := &val
				values[index] = &valPtr
			}
		case reflect.Float32:
			return func(index int, values []interface{}) {
				val := float32(0)
				valPtr := &val
				values[index] = &valPtr
			}
		case reflect.Uint8:
			return func(index int, values []interface{}) {
				val := uint8(0)
				valPtr := &val
				values[index] = &valPtr
			}

		case reflect.String:
			return func(index int, values []interface{}) {
				val := ""
				valPtr := &val
				values[index] = &valPtr
			}
		case reflect.Bool:
			return func(index int, values []interface{}) {
				val := false
				valPtr := &val
				values[index] = &valPtr
			}
		case reflect.Slice:
			switch scanType.Elem().Elem().Kind() {
			case reflect.String:
				return func(index int, values []interface{}) {
					val := ""
					values[index] = &val
				}
			}

		default:
			return func(index int, values []interface{}) {
				val := reflect.New(scanType).Interface()
				values[index] = val
			}
		}
	case reflect.Int:
		return func(index int, values []interface{}) {
			val := 0
			values[index] = &val
		}
	case reflect.Int64:
		return func(index int, values []interface{}) {
			val := int64(0)
			values[index] = &val
		}
	case reflect.Float64:
		return func(index int, values []interface{}) {
			val := float64(0)
			values[index] = &val
		}
	case reflect.Float32:
		return func(index int, values []interface{}) {
			val := float32(0)
			values[index] = &val
		}
	case reflect.Uint8:
		return func(index int, values []interface{}) {
			val := uint8(0)
			values[index] = &val
		}

	case reflect.String:
		return func(index int, values []interface{}) {
			val := ""
			values[index] = &val
		}
	case reflect.Bool:
		return func(index int, values []interface{}) {
			val := false
			values[index] = &val
		}

	case reflect.Slice:
		switch scanType.Elem().Kind() {
		case reflect.String:
			return func(index int, values []interface{}) {
				val := ""
				values[index] = &val
			}
		}
	}

	if scanType != nil {
		return func(index int, values []interface{}) {
			val := reflect.New(scanType).Interface()
			values[index] = val
		}
	}
	return func(index int, values []interface{}) {
		val := new(interface{})
		values[index] = &val
	}

}
