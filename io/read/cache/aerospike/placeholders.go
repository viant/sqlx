package aerospike

import (
	"github.com/viant/sqlx/converter"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xunsafe"
	"reflect"
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

		rType := field.ScanType()

		derefs = append(derefs, xunsafe.NewType(rType))
		for rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
			derefs = append(derefs, xunsafe.NewType(rType))
		}

		p.colDereferencers[i] = derefs
	}

	if p.columnIndex != -1 {
		scanType := p.fields[p.columnIndex].ScanType()
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
	for _, deref := range dereferencer {
		if asIface, ok := value.(*interface{}); ok {
			value = *asIface
		} else {
			value = deref.Deref(value)
		}
	}

	return value
}

func (p *Placeholders) CreatePlaceholderAt(i int) {
	p.ptrs[i] = reflect.New(p.fields[i].ScanType()).Interface()
}

func (p *Placeholders) ScanPlaceholders() []interface{} {
	return p.ptrs
}

func (p *Placeholders) Values() []interface{} {
	for i, dereferencer := range p.colDereferencers {
		p.deref[i] = p.derefValue(p.ptrs[i], dereferencer...)
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
