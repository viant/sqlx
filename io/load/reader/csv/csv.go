package csv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/pkg/errors"
	io2 "github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
	"io"
	"reflect"
)

type (
	Marshaller struct {
		xType           *xunsafe.Type
		elemType        reflect.Type
		xSlice          *xunsafe.Slice
		fieldsPositions map[string]int
		fields          []*Field
		maxDepth        int
		uniquesFields   map[string]bool
		references      map[string][]string
		pathAccessors   map[string]*xunsafe.Field
		stringifiers    map[reflect.Type]*io2.ObjectStringifier
		config          *Config
	}

	Field struct {
		parentType reflect.Type
		path       string
		name       string
		header     string

		xField      *xunsafe.Field
		depth       int
		unique      bool
		stringifier io2.FieldStringifierFn
	}

	Reference struct {
		ParentField string
		ChildField  string
	}
)

func NewMarshaller(rType reflect.Type, config *Config) (*Marshaller, error) {
	if config == nil {
		config = &Config{}
	}

	if config.EncloseBy == "" {
		config.EncloseBy = `"`
	}

	if config.EscapeBy == "" {
		config.EscapeBy = `\`
	}

	if config.FieldSeparator == "" {
		config.FieldSeparator = `,`
	}

	if config.ObjectSeparator == "" {
		config.ObjectSeparator = "\n"
	}

	if config.NullValue == "" {
		config.NullValue = "null"
	}

	elemType := Elem(rType)
	marshaller := &Marshaller{
		config:          config,
		elemType:        elemType,
		fieldsPositions: map[string]int{},
		uniquesFields:   map[string]bool{},
		references:      map[string][]string{},
		pathAccessors:   map[string]*xunsafe.Field{},
		xType:           xunsafe.NewType(elemType),
	}

	if err := marshaller.init(config); err != nil {
		return nil, err
	}

	return marshaller, nil
}

func (m *Marshaller) init(config *Config) error {
	m.initConfig(config)

	m.xSlice = xunsafe.NewSlice(reflect.SliceOf(m.elemType))
	m.indexByPath(m.elemType, "", 0, nil)

	return nil
}

func (m *Marshaller) indexByPath(parentType reflect.Type, path string, depth int, parentAccessor *xunsafe.Field) {
	elemParentType := Elem(parentType)
	numField := elemParentType.NumField()
	m.pathAccessors[path] = parentAccessor
	for i := 0; i < numField; i++ {
		field := elemParentType.Field(i)
		fieldPath := m.fieldPositionKey(path, field)

		elemType := Elem(field.Type)
		if elemType.Kind() == reflect.Struct {
			m.indexByPath(elemType, fieldPath, depth+1, xunsafe.NewField(field))
			continue
		}

		m.fieldsPositions[fieldPath] = len(m.fields)
		m.fields = append(m.fields, m.newField(path, field, depth, parentType, fieldPath))
	}
}

func (m *Marshaller) fieldPositionKey(path string, field reflect.StructField) string {
	name := field.Tag.Get(TagName)
	if name != "" {
		return name
	}

	return m.combine(path, field.Name)
}

func (m *Marshaller) combine(path, name string) string {
	if path == "" {
		return name
	}

	return path + "." + name
}

func (m *Marshaller) Unmarshal(b []byte, dest interface{}) error {
	reader := csv.NewReader(bytes.NewReader(b))
	headers, err := reader.Read()
	if err != nil {
		return m.asReadError(err)
	}

	fields, err := m.fieldsByName(headers)
	if err != nil {
		return err
	}

	session, err := m.session(fields, dest)
	if err != nil {
		return err
	}

	for {
		record, err := reader.Read()
		if err != nil {
			return m.asReadError(err)
		}

		if len(record) != len(fields) {
			return fmt.Errorf("record header and the record are differ in length. Fields len: %v, Record len: %v", len(fields), len(record))
		}

		if err = session.addRecord(record); err != nil {
			return err
		}
	}
}

func (m *Marshaller) newField(path string, field reflect.StructField, depth int, parentType reflect.Type, fieldPath string) *Field {
	xField := xunsafe.NewField(field)
	return &Field{
		path:        path,
		xField:      xField,
		depth:       depth,
		parentType:  parentType,
		name:        field.Name,
		header:      fieldPath,
		stringifier: io2.Stringifier(xField, false, m.config.NullValue),
	}
}

func (m *Marshaller) asReadError(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}

func (m *Marshaller) initConfig(config *Config) {
	for i := range config.UniqueFields {
		m.uniquesFields[config.UniqueFields[i]] = true
	}

	for _, reference := range config.References {
		m.references[reference.ParentField] = append(m.references[reference.ParentField], reference.ChildField)
	}
}

func (m *Marshaller) session(fields []*Field, dest interface{}) (*UnmarshalSession, error) {
	s := &UnmarshalSession{
		pathIndex: map[string]int{},
		dest:      dest,
	}

	return s, s.init(fields, m.references, m.pathAccessors, m.stringifiers)
}

func (m *Marshaller) fieldsByName(names []string) ([]*Field, error) {
	fields := make([]*Field, 0, len(names))
	for _, header := range names {
		index, ok := m.fieldsPositions[header]
		if !ok {
			return nil, fmt.Errorf("not found field %v", header)
		}

		fields = append(fields, m.fields[index])
	}
	return fields, nil
}

func (m *Marshaller) ReadHeaders(b []byte) ([]string, error) {
	reader := csv.NewReader(bytes.NewReader(b))
	headers, err := reader.Read()
	if err != nil {
		return nil, m.asReadError(err)
	}

	fields, err := m.fieldsByName(headers)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(fields))
	for _, field := range fields {
		result = append(result, m.combine(field.path, field.name))
	}

	return result, nil
}

func (m *Marshaller) Marshal(val interface{}, options ...interface{}) ([]byte, error) {
	valueType := reflect.TypeOf(val)
	if Elem(valueType) != m.elemType {
		return nil, fmt.Errorf("can't marshal %T with %v marshaller", val, m.elemType.String())
	}

	values, size, err := io2.Values(val)
	if err != nil {
		return nil, err
	}

	options = append(options, io2.Parallel(true))

	session, err := m.session(m.fields, nil)
	if err != nil {
		return nil, err
	}

	buffer, err := m.marshalBuffer(values, size, session.parentNode)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(buffer)
}

func (m *Marshaller) marshalBuffer(valueAt io2.ValueAccessor, size int, object *Object) (*Buffer, error) {
	buffer := NewBuffer(1024)
	accessor := object.Accessor(0, m.config)
	headers, bools := accessor.Headers()
	WriteObject(buffer, m.config, headers, bools)

	var xType *xunsafe.Type
	for i := 0; i < size; i++ {
		if i != 0 {
			accessor.Reset()
		}

		at := valueAt(i)
		if i == 0 {
			if reflect.TypeOf(at).Kind() == reflect.Ptr {
				xType = m.xType
			}
		}

		if xType != nil {
			at = xType.Deref(at)
		}

		accessor.Set(xunsafe.AsPointer(at))
		for accessor.Has() {
			headers, bools = accessor.Stringify()
			m.writeObject(buffer, headers, bools)
		}
	}

	return buffer, nil
}

func (m *Marshaller) writeObject(buffer *Buffer, headers []string, bools []bool) {
	if buffer.len() > 0 {
		buffer.writeString(m.config.ObjectSeparator)
	}

	WriteObject(buffer, m.config, headers, bools)
}

func Elem(rType reflect.Type) reflect.Type {
	for {
		switch rType.Kind() {
		case reflect.Ptr, reflect.Slice:
			rType = rType.Elem()
		default:
			return rType
		}
	}

}
