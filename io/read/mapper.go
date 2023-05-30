package read

import (
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

//RowMapper represents a target values mapped to pointer of slice
type RowMapper func(target interface{}) ([]interface{}, error)

//NewRowMapper  new a row mapper function
type NewRowMapper func(columns []io.Column, targetType reflect.Type, tagName string, resolver io.Resolve, options []option.Option) (RowMapper, error)

type Mapper struct {
	fields         []io.Field
	record         []interface{}
	willWrapFields bool
}

func NewMapper(fields []io.Field) *Mapper {
	m := &Mapper{
		fields: fields,
		record: make([]interface{}, len(fields)),
	}

	m.init()

	return m
}

func (m *Mapper) MapToRow(target interface{}) ([]interface{}, error) {
	ptr := xunsafe.AsPointer(target)
	for i, mapped := range m.fields {
		m.record[i] = mapped.Addr(ptr)
	}

	return m.record, nil
}

func (m *Mapper) MapToSQLRow(target interface{}) ([]interface{}, error) {
	ptr := xunsafe.AsPointer(target)
	for i, mapped := range m.fields {
		m.record[i] = mapped.Addr(ptr)
		if mapped.Tag.Encoding == io.EncodingJSON {
			m.record[i] = &io.JSONEncodedValue{Val: m.record[i]}
		}
	}

	return m.record, nil
}

func (m *Mapper) init() {
	for _, field := range m.fields {
		m.willWrapFields = m.willWrapFields || field.Encoding != ""
	}
}

//newRowMapper creates a new record mapped
func newRowMapper(columns []io.Column, targetType reflect.Type, tagName string, resolver io.Resolve, options []option.Option) (RowMapper, error) {
	if strings.Contains(targetType.String(), "Products") {
		fmt.Println("")
	}

	if tagName == "" {
		tagName = option.TagSqlx
	}

	switch targetType.Kind() {
	case reflect.Struct:
		return NewSQLStructMapper(columns, targetType, tagName, resolver, options...)
	case reflect.Ptr:
		if targetType.Elem().Kind() == reflect.Struct {
			return NewSQLStructMapper(columns, targetType.Elem(), tagName, resolver, options...)
		}
	}
	return GenericRowMapper(columns)
}

//NewStructMapper creates a new record mapper for supplied struct type
func NewStructMapper(columns []io.Column, recordType reflect.Type, tagName string, resolver io.Resolve, options ...option.Option) (RowMapper, error) {
	mapper, err := getMapper(columns, recordType, tagName, resolver, options)
	if err != nil {
		return nil, err
	}

	return mapper.MapToRow, nil
}

//NewSQLStructMapper creates a new record mapper for supplied struct and prepares them to scan / send values with sql.DB
func NewSQLStructMapper(columns []io.Column, recordType reflect.Type, tagName string, resolver io.Resolve, options ...option.Option) (RowMapper, error) {
	mapper, err := getMapper(columns, recordType, tagName, resolver, options)
	if err != nil {
		return nil, err
	}

	if mapper.willWrapFields {
		return mapper.MapToSQLRow, nil
	}

	return mapper.MapToRow, nil
}

func getMapper(columns []io.Column, recordType reflect.Type, tagName string, resolver io.Resolve, options []option.Option) (*Mapper, error) {
	cache, entry, err := mapperCacheEntry(columns, recordType, options, resolver)
	if err != nil {
		return nil, err
	}

	matched, err := fields(entry, columns, recordType, tagName, resolver)
	if err != nil {
		return nil, err
	}

	if cache != nil {
		cache.Put(entry, matched)
	}

	mapper := NewMapper(matched)
	return mapper, nil
}

func mapperCacheEntry(columns []io.Column, recordType reflect.Type, options []option.Option, resolver io.Resolve) (*MapperCache, *MapperCacheEntry, error) {
	var mapperCache *MapperCache
	var disableMapperCache DisableMapperCache
	for _, anOption := range options {
		switch actual := anOption.(type) {
		case *MapperCache:
			mapperCache = actual
		case DisableMapperCache:
			disableMapperCache = actual
		}
	}

	if mapperCache == nil && !disableMapperCache {
		mapperCache = DefaultMapperCache
	}

	if mapperCache == nil {
		return nil, nil, nil
	}

	entry, err := mapperCache.Get(recordType, columns, resolver)
	if err != nil {
		_ = mapperCache.Delete(entry)
		return nil, nil, err
	}

	return mapperCache, entry, nil
}

func fields(entry *MapperCacheEntry, columns []io.Column, recordType reflect.Type, tagName string, resolver io.Resolve) ([]io.Field, error) {
	if entry != nil && entry.HasFields() {
		return entry.Fields(), nil
	}

	matcher := io.NewMatcher(tagName, resolver)
	matched, err := matcher.Match(recordType, columns)
	if err != nil {
		return nil, err
	}

	return matched, nil
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
