package cache

import (
	"github.com/viant/xunsafe"
	"reflect"
)

type ScanTypeHolder struct {
	scanTypes []reflect.Type
	dataTypes []string
}

func (t *ScanTypeHolder) InitType(values []interface{}) {
	if len(t.scanTypes) > 0 {
		return
	}

	t.scanTypes = make([]reflect.Type, len(values))
	t.dataTypes = make([]string, len(values))
	for i, value := range values {
		rValue := reflect.ValueOf(value)
		valueType := rValue.Type()
		t.scanTypes[i] = valueType.Elem()
		t.dataTypes[i] = t.scanTypes[i].String()
	}
}

func (t *ScanTypeHolder) Match(entry *Entry) bool {
	if t == nil {
		return false
	}

	if !t.matchesEntryType(entry) {
		return false
	}

	entry.Meta.Type = t.dataTypes
	return true
}

func (t *ScanTypeHolder) matchesEntryType(entry *Entry) bool {
	if len(entry.Meta.Type) <= 0 {
		return true
	}

	actualTypes := entry.Meta.Type
	if len(actualTypes) != len(t.dataTypes) {
		return false
	}

	for i, dataType := range t.dataTypes {
		if dataType != actualTypes[i] {
			return false
		}
	}

	return true
}

type XTypesHolder struct {
	entry  *Entry
	xTypes []*xunsafe.Type
}

func NewXTypeHolder(entry *Entry) *XTypesHolder {
	return &XTypesHolder{
		entry: entry,
	}
}

func (s *XTypesHolder) XTypes() []*xunsafe.Type {
	if s.xTypes != nil {
		return s.xTypes
	}

	s.xTypes = make([]*xunsafe.Type, len(s.entry.Meta.Fields))
	for i, field := range s.entry.Meta.Fields {
		s.xTypes[i] = xunsafe.NewType(field.ScanType())
	}

	return nil
}
