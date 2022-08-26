package cache

import (
	"bytes"
	"encoding/json"
)

type Indexed struct {
	ColumnValue interface{}
	Data        *bytes.Buffer
}

func NewIndexed(columnValue interface{}) *Indexed {
	return &Indexed{
		Data:        bytes.NewBufferString(""),
		ColumnValue: columnValue,
	}
}

func (i *Indexed) StringifyData(data []interface{}) error {
	dataMarshal, err := json.Marshal(data)
	if err != nil {
		return err
	}

	if i.Data.Len() > 0 {
		i.Data.WriteByte('\n')
	}

	_, err = i.Data.Write(dataMarshal)

	return err
}
