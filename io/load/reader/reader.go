package reader

import (
	"github.com/viant/sqlx/io"
	goIo "io"
	"reflect"
	"strings"
)

//Reader represents plain text reader
type Reader struct {
	config      *Config
	valueAt     func(index int) interface{}
	stringifier io.ObjectStringifier
	buffer      *Buffer
	itemCount   int
	index       int
	offset      int
	isEOF       bool
}

//Read stringify and reads data into buffer, separates objects and fields values with given separators.
func (r *Reader) Read(buffer []byte) (n int, err error) {
	if r.isEOF {
		return 0, goIo.EOF
	}
	if r.index > r.itemCount {
		return 0, goIo.EOF
	}

	r.offset = 0
	i := r.index
	offset, readValue := r.buffer.Read(buffer[r.offset:])
	if offset == 0 && !readValue {
		return 0, nil
	}

	if offset != 0 {
		i++
		r.offset += offset
	}

	var stringifiedFieldValues []string
	var wasString []bool
	for ; i < r.itemCount; i++ {
		record := r.valueAt(i)
		stringifiedFieldValues, wasString = r.stringifier(record)

		if i != 0 {
			r.buffer.WriteString(r.config.ObjectSeparator)
		}

		for j := 0; j < len(stringifiedFieldValues); j++ {
			if j != 0 {
				r.buffer.WriteString(r.config.FieldSeparator)
			}

			stringifiedFieldValues[j] = r.escapeSpecialChars(stringifiedFieldValues[j])
			if wasString[j] {
				stringifiedFieldValues[j] = r.config.EncloseBy + stringifiedFieldValues[j] + r.config.EncloseBy
			}
			r.buffer.WriteString(stringifiedFieldValues[j])
		}

		offset, _ = r.buffer.Read(buffer[r.offset:])
		if offset == 0 {
			r.index = i
			return r.offset, nil
		}

		r.offset += offset
	}
	r.isEOF = true
	return r.offset, nil
}

func (r *Reader) escapeSpecialChars(value string) string {
	value = strings.ReplaceAll(value, r.config.EscapeBy, r.config.EscapeBy+r.config.EscapeBy)
	value = strings.ReplaceAll(value, r.config.FieldSeparator, r.config.EscapeBy+r.config.FieldSeparator)
	value = strings.ReplaceAll(value, r.config.ObjectSeparator, r.config.EscapeBy+r.config.ObjectSeparator)
	value = strings.ReplaceAll(value, r.config.EncloseBy, r.config.EscapeBy+r.config.EncloseBy)
	return value
}

//NewReader returns Reader instance and actual data struct type.
//e.g. data is type of []*Foo - returns Foo.
func NewReader(any interface{}, config *Config) (*Reader, reflect.Type, error) {
	valueAt, size, err := io.Values(any)
	if err != nil {
		return nil, nil, err
	}

	structType := io.EnsureDereference(valueAt(0))
	stringifier := io.TypeStringifier(structType, config.NullValue, true)

	if err != nil {
		return nil, nil, err
	}

	return &Reader{
		config:      config,
		valueAt:     valueAt,
		itemCount:   size,
		stringifier: stringifier,
		buffer:      NewBuffer(1024),
		offset:      0,
	}, structType, nil
}

func (r *Reader) ItemCount() int {
	return r.itemCount
}
