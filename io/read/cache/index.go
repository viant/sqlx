package cache

import (
	"bytes"
	"encoding/json"
)

type Indexed struct {
	ColumnValue interface{}
	Data        *bytes.Buffer
	ReadOrder   []int
}

func NewIndexed(columnValue interface{}) *Indexed {
	return &Indexed{
		Data:        bytes.NewBufferString(""),
		ColumnValue: columnValue,
	}
}

func (i *Indexed) StringifyData(position int, data []interface{}) error {
	dataMarshal, err := json.Marshal(data)
	if err != nil {
		return err
	}

	if i.Data.Len() > 0 {
		i.Data.WriteByte('\n')
	}

	_, err = i.Data.Write(dataMarshal)
	i.ReadOrder = append(i.ReadOrder, position)

	return err
}
