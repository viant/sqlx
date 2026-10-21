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
		if destination, ok := value.(interface{ Destination() any }); ok {
			value = destination.Destination()
		}
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

	// Projected entries keep Meta.Type in stored cache order. Overwriting it
	// with destination-order types causes EffectiveType to project twice on
	// subsequent rows when the requested field order differs from stored order.
	if !entry.Meta.Projected() {
		entry.Meta.Type = t.dataTypes
	}
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
		if len(entry.Meta.Type) == 0 {
			normalizedCachedType = normalizeDriverScanTypeName(normalizedCachedType)
			// SQLX exposes JSON driver columns as RawMessage. The initial DB
			// scan still owns conversion; persisted rows retain destination type.
			if normalizedCachedType == "json.RawMessage" && (dataType == "string" || dataType == "[]uint8") {
				normalizedCachedType = dataType
			}
		}
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

func normalizeDriverScanTypeName(typeName string) string {
	switch typeName {
	case "sql.NullBool":
		return "bool"
	case "sql.NullByte":
		return "uint8"
	case "sql.NullInt16":
		return "int16"
	case "sql.NullInt32":
		return "int32"
	case "sql.NullInt64":
		return "int64"
	case "sql.NullFloat64":
		return "float64"
	case "sql.NullString":
		return "string"
	case "sql.NullTime":
		return "time.Time"
	}
	if strings.HasPrefix(typeName, "sql.Null[") && strings.HasSuffix(typeName, "]") {
		return strings.TrimSuffix(strings.TrimPrefix(typeName, "sql.Null["), "]")
	}
	return typeName
}

func isSQLNullType(typ reflect.Type) bool {
	if typ == nil || typ.Kind() != reflect.Struct || typ.PkgPath() != "database/sql" || !strings.HasPrefix(typ.Name(), "Null") || typ.NumField() != 2 {
		return false
	}
	return typ.Field(1).Name == "Valid" && typ.Field(1).Type.Kind() == reflect.Bool
}

func isCompatibleCacheType(destinationType string, cachedType string) bool {
	// Interface scan metadata does not identify a concrete SQL result type
	// (for example SQLite aggregates). The destination scanner validates the
	// actual encoded value rather than rejecting unknown metadata up front.
	if cachedType == "interface {}" {
		return true
	}
	if destinationType == cachedType {
		return true
	}
	// Nullable destinations can decode both typed nullable objects and scalar
	// rows. The reverse is not safe: scalar decoders do not consume objects.
	if normalized := normalizeDriverScanTypeName(destinationType); normalized != destinationType && normalized == cachedType {
		return true
	}
	// SQL temporal fields may use a textual intermediate scanner. Warmed
	// time.Time values are JSON strings, just as database/sql converts times
	// into RFC3339Nano text for a string destination.
	if destinationType == "string" && cachedType == "time.Time" {
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
