package aerospike

import (
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xunsafe"
	"reflect"
)

type Placeholders struct {
	fields              []*cache.Field
	deref               []interface{}
	ptrs                []interface{}
	columnIndex         int
	columnDereferencers []*xunsafe.Type
}

func (p *Placeholders) init() {
	p.deref = make([]interface{}, len(p.fields))
	p.ptrs = make([]interface{}, len(p.fields))

	for i := range p.deref {
		p.CreatePlaceholderAt(i)
	}

	if p.columnIndex != -1 {
		scanType := p.fields[p.columnIndex].ScanType()
		if scanType.Kind() == reflect.Ptr {
			scanType = scanType.Elem()
		}

		for scanType.Kind() == reflect.Ptr {
			p.columnDereferencers = append(p.columnDereferencers, xunsafe.NewType(scanType))
			scanType = scanType.Elem()
		}
	}
}

func (p *Placeholders) ColumnValue() (interface{}, bool) {
	if p.columnIndex == -1 {
		return nil, true
	}

	value := p.deref[p.columnIndex]
	for _, dereferencer := range p.columnDereferencers {
		if dereferencer.Pointer(value) == nil {
			return nil, false
		}

		value = dereferencer.Deref(value)
	}

	if value != nil {
		switch actual := value.(type) {
		case []byte:
			return string(actual), true
		case string, uint, int, float64, int64, uint64, int32, uint32, int16, uint16, bool, float32:
			return actual, true
		}
		of := reflect.TypeOf(value)
		dest := reflect.New(of).Elem().Interface()
		xunsafe.Copy(xunsafe.AsPointer(dest), xunsafe.AsPointer(value), int(of.Size()))
		return dest, true
	}
	return value, true
}

func (p *Placeholders) CreatePlaceholderAt(i int) {
	value := reflect.New(p.fields[i].ScanType()).Elem().Interface()
	p.deref[i] = value
	p.ptrs[i] = &p.deref[i]
}

func (p *Placeholders) ScanPlaceholders() []interface{} {
	return p.ptrs
}

func (p *Placeholders) Values() []interface{} {
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
