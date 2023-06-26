package io

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/viant/sqlx/option"
	"github.com/viant/structology"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type JSONEncodedValue struct {
	Val interface{}
}

func (j *JSONEncodedValue) Value() (driver.Value, error) {
	marshal, err := json.Marshal(j.Val)
	if err != nil {
		return nil, err
	}

	return string(marshal), nil
}

func (j *JSONEncodedValue) Scan(v interface{}) error {
	if v == nil {
		return nil
	}

	switch actual := v.(type) {
	case []byte:
		if len(actual) == 0 {
			return nil
		}

		return json.Unmarshal(actual, j.Val)

	case string:
		if len(actual) == 0 {
			return nil
		}

		return json.Unmarshal([]byte(actual), j.Val)

	default:
		dst := reflect.ValueOf(j.Val)
		src := reflect.ValueOf(v)
		elem := dst.Elem()
		elemType := elem.Type()

		if src.Type().ConvertibleTo(elemType) {
			elem.Set(src.Convert(elemType))
			return nil
		}

		return fmt.Errorf("unconvertible %v into %v", src.Type().String(), dst.String())

	}
}

//ColumnMapper maps src to columns and its placeholders
type ColumnMapper func(src interface{}, tagName string, options ...option.Option) ([]Column, PlaceholderBinder, error)

type columnMapperBuilder struct {
	columns           []ColumnWithFields
	identityColumns   []ColumnWithFields
	setMarker         *option.SetMarker
	columnRestriction option.ColumnRestriction
	identityOnly      bool
}

//StructColumnMapper returns genertic column mapper
func StructColumnMapper(src interface{}, tagName string, options ...option.Option) ([]Column, PlaceholderBinder, error) {
	recordType, ok := src.(reflect.Type)
	if !ok {
		recordType = reflect.TypeOf(src)
	}

	if recordType.Kind() == reflect.Ptr {
		recordType = recordType.Elem()
	}

	builder, err := newColumnBuilder(options, recordType)
	if err != nil {
		return nil, nil, err
	}

	for i := 0; i < recordType.NumField(); i++ {
		field := recordType.Field(i)
		if err = builder.appendColumns(field, tagName); err != nil {
			return nil, nil, err
		}
	}

	columns := builder.mergeColumns()
	var getters []xunsafe.Getter
	filedPos := map[string]int{}
	transientFields := map[string]int{}
	for i, col := range columns {
		fields := col.Fields()
		field := fields[len(fields)-1]
		aTag := col.Tag()
		if aTag != nil && !aTag.PresenceProvider {
			filedPos[field.Name] = i
			if aTag.Transient {
				transientFields[col.Name()] = int(field.Index)
			}

		}

		if aTag.isIdentity(col.Name()) {
			if builder.setMarker != nil {
				builder.setMarker.IdentityIndex = i
			}
		}

		getter, err := fieldGetter(aTag, field, recordType)
		if err != nil {
			return nil, nil, err
		}

		getters = append(getters, getter)
	}

	//	marker, _ := structology.NewMarker(recordType, structology.)

	if builder.setMarker != nil && structology.HasSetMarker(recordType) {
		if builder.setMarker.Marker, err = structology.NewMarker(recordType, structology.WithIndex(filedPos)); err != nil {
			return nil, nil, err
		}
	}

	return asColumnSlice(columns), func(src interface{}, params []interface{}, offset, limit int) {
		holderPtr := xunsafe.AsPointer(src)
		end := offset + limit
		for i, ptr := range getters[offset:end] {
			params[i] = ptr(holderPtr)
		}
	}, nil
}

func fieldGetter(tag *Tag, field *xunsafe.Field, recordType reflect.Type) (xunsafe.Getter, error) {
	if tag == nil || tag.Encoding == "" {
		return field.Addr, nil
	}

	switch tag.Encoding {
	case EncodingJSON:
		return jsonFieldEncodder(tag, field, recordType), nil
	default:
		return nil, fmt.Errorf("unsupported column encoding type %v", tag.Encoding)
	}
}

func asColumnSlice(columns []ColumnWithFields) []Column {
	result := make([]Column, 0, len(columns))
	for _, col := range columns {
		result = append(result, col)
	}

	return result
}

func newColumnBuilder(options []option.Option, recordType reflect.Type) (*columnMapperBuilder, error) {
	var columnRestriction option.ColumnRestriction
	if val := option.Options(options).Columns(); len(val) > 0 {
		columnRestriction = val.Restriction()
	}

	setMarker := option.Options(options).SetMarker()
	if recordType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid record type: %v", recordType.Kind())
	}

	builder := &columnMapperBuilder{
		setMarker:         setMarker,
		identityOnly:      option.Options(options).IdentityOnly(),
		columnRestriction: columnRestriction,
	}

	return builder, nil
}

func (b *columnMapperBuilder) appendColumns(field reflect.StructField, tagName string, holders ...*xunsafe.Field) error {
	xField := xunsafe.NewField(field)
	tag := ParseTag(field.Tag.Get(tagName))
	if len(holders) > 0 {
		actualHolder := holders[0]
		if actualHolder.Type.Kind() == reflect.Struct {
			xField.Offset += actualHolder.Offset
			holders = nil
		}
	}

	holders = append(holders, xField)
	if isExported := field.PkgPath == ""; !isExported {
		return nil
	}
	if err := tag.validateWithField(field); err != nil {
		return err
	}

	if tag.Transient {
		return nil
	}

	if xField.Anonymous {
		fieldType := xField.Type
		for fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}

		if fieldType.Kind() == reflect.Struct {
			numField := fieldType.NumField()
			for i := 0; i < numField; i++ {
				if err := b.appendColumns(fieldType.Field(i), tagName, xField); err != nil {
					return err
				}
			}

			return nil
		}
	}

	columnName := tag.getColumnName(field)
	if tag.isIdentity(columnName) {
		tag.PrimaryKey = true
		tag.Column = columnName
		b.identityColumns = append(b.identityColumns, NewColumnWithFields(columnName, "", field.Type, holders, tag))
		return nil
	}

	if b.identityOnly {
		return nil
	}

	if b.columnRestriction.CanUse(columnName) {
		b.columns = append(b.columns, NewColumnWithFields(columnName, "", field.Type, holders, tag))
	}

	return nil
}

func (b *columnMapperBuilder) mergeColumns() []ColumnWithFields {
	//make sure identity columns are at the end
	var columns []ColumnWithFields
	columns = append(columns, b.columns...)
	columns = append(columns, b.identityColumns...)

	return columns
}

func IsStruct(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Struct:
		return true
	case reflect.Ptr:
		return IsStruct(t.Elem())
	}
	return false
}

func jsonFieldEncodder(tag *Tag, field *xunsafe.Field, recordType reflect.Type) func(structPtr unsafe.Pointer) interface{} {
	fType := field.Type
	isPointer := false
	if fType.Kind() == reflect.Ptr {
		fType = fType.Elem()
		isPointer = true
	}

	return func(structPtr unsafe.Pointer) interface{} {
		holderPtr := field.Pointer(structPtr)
		if isPointer && holderPtr == nil {
			return sql.NullString{}
		}

		value := field.Interface(structPtr)
		return &JSONEncodedValue{Val: value}
		//marshaled, err := json.Marshal(value)
		//if err != nil {
		//	return err.Error()
		//}
		//
		//return marshaled
	}
}
