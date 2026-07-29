package cache

import (
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

var signedIntegerCompatTypes = map[string]struct{}{
	"int":   {},
	"int8":  {},
	"int16": {},
	"int32": {},
	"int64": {},
}

type ScanTypeHolder struct {
	scanTypes []reflect.Type
	dataTypes []string
}

type TypeMismatch struct {
	Index                int
	DestinationType      string
	NormalizedDestType   string
	CachedType           string
	NormalizedCachedType string
	Reason               string
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
		t.dataTypes[i] = normalizeCompatType(t.scanTypes[i]).String()
	}
}

func (t *ScanTypeHolder) Match(entry *Entry) bool {
	if t == nil {
		return false
	}

	if ok, _ := t.matchesEntryType(entry); !ok {
		return false
	}

	entry.Meta.Type = t.dataTypes
	return true
}

func (t *ScanTypeHolder) Mismatch(entry *Entry) *TypeMismatch {
	_, mismatch := t.matchesEntryType(entry)
	return mismatch
}

func (t *ScanTypeHolder) matchesEntryType(entry *Entry) (bool, *TypeMismatch) {
	actualTypes := entry.Meta.EffectiveType()
	if len(actualTypes) <= 0 {
		return true, nil
	}

	if len(actualTypes) != len(t.dataTypes) {
		return false, &TypeMismatch{
			Index:  -1,
			Reason: "length_mismatch",
		}
	}

	for i, dataType := range t.dataTypes {
		normalizedCachedType := normalizeCompatTypeName(actualTypes[i])
		if !isCompatibleCacheType(dataType, normalizedCachedType) {
			return false, &TypeMismatch{
				Index:                i,
				DestinationType:      scanTypeStringAt(t.scanTypes, i),
				NormalizedDestType:   dataType,
				CachedType:           actualTypes[i],
				NormalizedCachedType: normalizedCachedType,
				Reason:               "incompatible_type",
			}
		}
	}

	return true, nil
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

	fields := s.entry.Meta.EffectiveFields()
	s.xTypes = make([]*xunsafe.Type, len(fields))
	for i, field := range fields {
		s.xTypes[i] = xunsafe.NewType(field.ScanType())
	}

	return s.xTypes
}

func normalizeCompatType(rType reflect.Type) reflect.Type {
	for rType != nil && rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType
}

func normalizeCompatTypeName(typeName string) string {
	typeName = strings.TrimSpace(typeName)
	for strings.HasPrefix(typeName, "*") {
		typeName = strings.TrimPrefix(typeName, "*")
	}
	return typeName
}

func isCompatibleCacheType(destinationType string, cachedType string) bool {
	if destinationType == cachedType {
		return true
	}
	if destinationType == "bool" && cachedType == "int" {
		return true
	}
	if destinationType == "float64" && isSignedIntegerCompatType(cachedType) {
		return true
	}
	return false
}

func scanTypeStringAt(types []reflect.Type, index int) string {
	if index < 0 || index >= len(types) || types[index] == nil {
		return ""
	}
	return types[index].String()
}

func isSignedIntegerCompatType(typeName string) bool {
	_, ok := signedIntegerCompatTypes[typeName]
	return ok
}
