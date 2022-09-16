package parquet

import (
	"bytes"
	aParquet "github.com/segmentio/parquet-go"
	"github.com/viant/sqlx/io"
	goIo "io"
)

// NewReader returns Reader instance which supports parquet format
func NewReader(any interface{}) (goIo.Reader, error) {
	valueAt, size, err := io.Values(any)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	writer := aParquet.NewWriter(buf)

	for i := 0; i < size; i++ {
		err = writer.Write(valueAt(i)) // func Write adds '\n'
		if err != nil {
			return nil, err
		}
	}
	if err = writer.Flush(); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}

	return buf, nil
}
