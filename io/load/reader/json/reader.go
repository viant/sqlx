package json

import (
	"bytes"
	"encoding/json"
	"github.com/viant/sqlx/io"
	sio "io"
)

// NewReader returns Reader instance and actual data struct type.
func NewReader(any interface{}) (sio.Reader, error) {
	valueAt, size, err := io.Values(any)
	if err != nil {
		return nil, err
	}
	buffer := new(bytes.Buffer)
	enc := json.NewEncoder(buffer)
	for i := 0; i < size; i++ {
		if i > 0 {
			buffer.WriteByte('\n')
		}
		if err = enc.Encode(valueAt(i)); err != nil {
			return nil, err
		}
	}
	return buffer, nil
}
