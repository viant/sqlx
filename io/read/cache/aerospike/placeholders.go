package aerospike

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/viant/sqlx/converter"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xunsafe"
)

type Placeholders struct {
	fields           []*cache.Field
	deref            []interface{}
	ptrs             []interface{}
	columnIndex      int
	colDereferencers [][]*xunsafe.Type

	indexedColDereferencer []*xunsafe.Type
	actualColumnType       reflect.Type
}

func (p *Placeholders) init() {
	p.deref = make([]interface{}, len(p.fields))
	p.ptrs = make([]interface{}, len(p.fields))
	p.colDereferencers = make([][]*xunsafe.Type, len(p.fields))

	for i := range p.deref {
		p.CreatePlaceholderAt(i)
	}

	for i, field := range p.fields {
		var derefs []*xunsafe.Type

		rType := scanPlaceholderType(field)

		derefs = append(derefs, xunsafe.NewType(rType))
		for rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
			derefs = append(derefs, xunsafe.NewType(rType))
		}

		p.colDereferencers[i] = derefs
	}

	if p.columnIndex != -1 {
		scanType := scanPlaceholderType(p.fields[p.columnIndex])
		p.indexedColDereferencer = append(p.indexedColDereferencer, xunsafe.NewType(scanType))
		for scanType.Kind() == reflect.Ptr {
			scanType = scanType.Elem()
			p.indexedColDereferencer = append(p.indexedColDereferencer, xunsafe.NewType(scanType))
		}
	}
}

func (p *Placeholders) ColumnValue() (interface{}, bool) {
	if p.columnIndex == -1 {
		return nil, true
	}

	value := p.ptrs[p.columnIndex]
	for _, dereferencer := range p.indexedColDereferencer {
		value = p.derefValue(value, dereferencer)
	}
	value = normalizeNilValue(value)

	if value != nil && xunsafe.AsPointer(value) != nil {
		switch actual := value.(type) {
		case []byte:
			if p.actualColumnType == nil {
				p.actualColumnType = deref(p.fields[p.columnIndex].ScanType())
			}
			convert, wasNil, err := converter.Convert(string(actual), p.actualColumnType, "")
			return convert, err == nil && !wasNil
		case string:
			return actual, true
		case uint:
			return actual, true
		case int:
			return actual, true
		case float64:
			return actual, true
		case int64:
			return actual, true
		case uint64:
			return actual, true
		case int32:
			return actual, true
		case uint32:
			return actual, true
		case int16:
			return actual, true
		case uint16:
			return actual, true
		case bool:
			return actual, true
		case float32:
			return actual, true
		}

		of := reflect.TypeOf(value)
		wasPtr := false

		if of.Kind() == reflect.Ptr {
			of = of.Elem()
			wasPtr = true
		}

		destValue := reflect.New(of)
		if !wasPtr {
			destValue = destValue.Elem()
		}

		dest := destValue.Interface()
		xunsafe.Copy(xunsafe.AsPointer(dest), xunsafe.AsPointer(value), int(of.Size()))
		return dest, value != nil
	}
	return value, true
}

func (p *Placeholders) derefValue(value interface{}, dereferencer ...*xunsafe.Type) interface{} {
	if destination, ok := value.(interface{ Destination() any }); ok {
		value = destination.Destination()
	}
	rValue := reflect.ValueOf(value)
	for rValue.IsValid() {
		switch rValue.Kind() {
		case reflect.Interface:
			if rValue.IsNil() {
				return nil
			}
			rValue = rValue.Elem()
		case reflect.Ptr:
			if rValue.IsNil() {
				return nil
			}
			rValue = rValue.Elem()
		default:
			return rValue.Interface()
		}
	}

	return nil
}

func (p *Placeholders) CreatePlaceholderAt(i int) {
	scanType := scanPlaceholderType(p.fields[i])
	byteSliceType := scanType
	if byteSliceType != nil && byteSliceType.Kind() == reflect.Ptr {
		byteSliceType = byteSliceType.Elem()
	}
	if byteSliceType != nil && byteSliceType.Kind() == reflect.Slice && byteSliceType.Elem().Kind() == reflect.Uint8 {
		p.ptrs[i] = &byteSlicePlaceholder{}
		return
	}
	p.ptrs[i] = reflect.New(scanType).Interface()
}

// byteSlicePlaceholder accepts the pointer-valued byte slices returned by
// some drivers while retaining the native *[]byte value for cache indexing.
type byteSlicePlaceholder struct {
	value *[]byte
}

var _ sql.Scanner = (*byteSlicePlaceholder)(nil)

func (p *byteSlicePlaceholder) Destination() any {
	return &p.value
}

func (p *byteSlicePlaceholder) Scan(source any) error {
	var data []byte
	switch value := source.(type) {
	case nil:
		p.value = nil
		return nil
	case string:
		data = []byte(value)
	case []byte:
		data = value
	case *[]byte:
		if value == nil {
			p.value = nil
			return nil
		}
		data = *value
	default:
		return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type *[]byte", source)
	}
	if data == nil {
		p.value = nil
		return nil
	}
	copyOfData := append([]byte{}, data...)
	p.value = &copyOfData
	return nil
}

func (p *Placeholders) ScanPlaceholders() []interface{} {
	return p.ptrs
}

func (p *Placeholders) Values() []interface{} {
	for i, dereferencer := range p.colDereferencers {
		p.deref[i] = normalizeNilValue(p.derefValue(p.ptrs[i], dereferencer...))
	}

	return p.deref
}

func NewPlaceholders(columnIndex int, fields []*cache.Field) *Placeholders {
	result := &Placeholders{
		fields:      fields,
		columnIndex: columnIndex,
	}

	result.init()
	return result
}

func deref(rType reflect.Type) reflect.Type {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	return rType
}

func scanPlaceholderType(field *cache.Field) reflect.Type {
	scanType := field.ScanType()
	if scanType == nil || scanType.Kind() == reflect.Ptr {
		return scanType
	}

	nullable, ok := field.Nullable()
	if !ok || !nullable || !isNullableScalarType(scanType) {
		return scanType
	}

	return reflect.PtrTo(scanType)
}

func isNullableScalarType(scanType reflect.Type) bool {
	switch scanType.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	case reflect.Struct:
		return scanType == reflect.TypeOf(time.Time{})
	}

	return false
}

func normalizeNilValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	rValue := reflect.ValueOf(value)
	switch rValue.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		if rValue.IsNil() {
			return nil
		}
	}

	return value
}
