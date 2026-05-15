package cache

import (
	"bytes"
	"encoding/json"
	"reflect"
)

type Indexed struct {
	ColumnValue interface{}
	Data        *bytes.Buffer
	Column      string
}

func NewIndexed(columnValue interface{}) *Indexed {
	return &Indexed{
		Data:        bytes.NewBufferString(""),
		ColumnValue: columnValue,
	}
}

func (i *Indexed) StringifyData(data []interface{}) error {
	sanitized := sanitizeNilInterfaces(data)
	dataMarshal, err := json.Marshal(sanitized)
	if err != nil {
		return err
	}

	if i.Data.Len() > 0 {
		i.Data.WriteByte('\n')
	}

	_, err = i.Data.Write(dataMarshal)

	return err
}

func sanitizeNilInterfaces(data []interface{}) []interface{} {
	result := make([]interface{}, len(data))
	for idx, value := range data {
		result[idx] = sanitizeValue(value)
	}
	return result
}

func sanitizeValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	rv := reflect.ValueOf(value)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return nil
	}
	return rv.Interface()
}
