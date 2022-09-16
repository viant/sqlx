package json

import (
	"bytes"
	"encoding/json"
	"github.com/viant/sqlx/io"
	goIo "io"
)

// NewReader returns Reader instance which supports json format
func NewReader(any interface{}) (goIo.Reader, error) {
	valueAt, size, err := io.Values(any)
	if err != nil {
		return nil, err
	}
	buffer := new(bytes.Buffer)
	enc := json.NewEncoder(buffer)
	for i := 0; i < size; i++ {
		if err = enc.Encode(valueAt(i)); err != nil {
			return nil, err
		}
	}
	return buffer, nil
}
