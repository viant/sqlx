package io

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

//ColumnMapper maps src to columns and its placeholders
type ColumnMapper func(src interface{}, tagName string, options ...option.Option) ([]Column, PlaceholderBinder, error)

//StructColumnMapper returns genertic column mapper
func StructColumnMapper(src interface{}, tagName string, options ...option.Option) ([]Column, PlaceholderBinder, error) {
	recordType, ok := src.(reflect.Type)
	if !ok {
		recordType = reflect.TypeOf(src)
	}
	if recordType.Kind() == reflect.Ptr {
		recordType = recordType.Elem()
	}
	identityOnly := option.Options(options).IdentityOnly()
	var columnRestriction option.ColumnRestriction
	if val := option.Options(options).Columns(); len(val) > 0 {
		columnRestriction = val.Restriction()
	}
	presenceProvider := option.Options(options).PresenceProvider()

	if recordType.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("invalid record type: %v", recordType.Kind())
	}
	var columns []Column
	var identityColumns []Column
	var getters []xunsafe.Getter

	var filedPos = make(map[string]int)
	var transientPos = make(map[string]int)

	for i := 0; i < recordType.NumField(); i++ {
		field := recordType.Field(i)

		xField := xunsafe.NewField(field)
		tag := ParseTag(field.Tag.Get(tagName))
		if tag.PresenceProvider && presenceProvider != nil {
			presenceProvider.Holder = xunsafe.NewField(field)
		}
		if isExported := field.PkgPath == ""; !isExported {
			continue
		}

		if err := tag.validateWithField(field); err != nil {
			return nil, nil, err
		}
		if tag.Transient {
			transientPos[field.Name] = int(field.Index[0])
			continue
		}

		columnName := tag.getColumnName(field)
		if tag.isIdentity(columnName) {
			tag.PrimaryKey = true
			tag.Column = columnName
			tag.FieldIndex = i
			identityColumns = append(identityColumns, NewColumn(columnName, "", field.Type, tag))
			continue
		}

		if identityOnly {
			continue
		}

		if columnRestriction.CanUse(columnName) {
			columns = append(columns, NewColumn(columnName, "", field.Type, tag))
			getter := xField.Addr
			var err error
			if IsStruct(xField.Type) {
				if getter, err = structGetter(tag, field, getter, recordType); err != nil {
					return nil, nil, err
				}
			}

			if getter == nil {
				getter = xField.Addr
			}

			pos := len(getters)
			getters = append(getters, getter)
			if presenceProvider != nil {
				filedPos[xField.Name] = pos
			}
		}
	}

	appendIdentityColumns(identityColumns, recordType, &getters, &columns, presenceProvider, filedPos)

	if presenceProvider != nil && len(filedPos) > 0 {
		if err := presenceProvider.Init(filedPos, transientPos); err != nil {
			return nil, nil, err
		}
	}

	return columns, func(src interface{}, params []interface{}, offset, limit int) {
		holderPtr := xunsafe.AsPointer(src)
		end := offset + limit
		for i, ptr := range getters[offset:end] {
			params[i] = ptr(holderPtr)
		}
	}, nil
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

func structGetter(tag *Tag, field reflect.StructField, getter func(structPtr unsafe.Pointer) interface{}, recordType reflect.Type) (func(structPtr unsafe.Pointer) interface{}, error) {
	if tag.Encoding != EncodingJSON {
		return nil, nil
	}

	fType := field.Type
	isPointer := false
	if fType.Kind() == reflect.Ptr {
		fType = fType.Elem()
		isPointer = true
	}
	getter = func(structPtr unsafe.Pointer) interface{} {
		xField := xunsafe.FieldByName(recordType, field.Name)
		holderPtr := xField.ValuePointer(structPtr)

		if isPointer && holderPtr == nil {
			return sql.NullString{}
		}
		value := xField.Interface(structPtr)
		marshaled, err := json.Marshal(value)
		if err != nil {
			return err.Error()
		}

		return marshaled
	}
	return getter, nil
}

func appendIdentityColumns(identityColumns []Column, recordType reflect.Type, getters *[]xunsafe.Getter, columns *[]Column, presenceProvider *option.PresenceProvider, filedPos map[string]int) {
	//make sure identity columns are at the end
	if len(identityColumns) > 0 {
		for i, item := range identityColumns {
			fieldIndex := item.Tag().FieldIndex
			xField := xunsafe.FieldByIndex(recordType, fieldIndex)
			getter := xField.Addr
			pos := len(*getters)
			*getters = append(*getters, getter)
			*columns = append(*columns, identityColumns[i])
			if presenceProvider != nil {
				if presenceProvider.IdentityIndex == 0 {
					presenceProvider.IdentityIndex = pos
				}
				filedPos[xField.Name] = pos
			}
		}
	}
}
