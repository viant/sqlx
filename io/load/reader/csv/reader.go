package csv

import (
	"github.com/viant/sqlx/io"
	goIo "io"
	"reflect"
	"strings"
)

// Reader represents plain text reader
type Reader struct {
	config                  *Config
	valueAt                 func(index int) interface{}
	stringifier             io.ObjectStringifier
	itemBuffer              *Buffer
	itemCount               int
	index                   int
	offsetOfCurrentRead     int
	isEOF                   bool
	initialized             bool
	currentItemReadRequired bool
}

// Read data into itemBuffer
func (r *Reader) Read(buffer []byte) (n int, err error) {

	if r.isEOF || r.index >= r.itemCount {
		return 0, goIo.EOF
	}

	r.offsetOfCurrentRead = 0

	if !r.initialized {
		r.fillItemBuffer(0)
		r.initialized = true
	}

	var canRead = true
	for ok := true; ok; ok = canRead {

		if r.currentItemReadRequired {
			n, err = r.itemBuffer.Read(buffer[r.offsetOfCurrentRead:])
			if err == goIo.EOF {
				r.currentItemReadRequired = false
			}
			r.offsetOfCurrentRead += n

			if r.offsetOfCurrentRead == len(buffer) {
				canRead = false
				return r.offsetOfCurrentRead, nil
			}
		} else {
			r.index += 1
			if r.index >= r.itemCount {
				canRead = false
				r.isEOF = true
				return r.offsetOfCurrentRead, nil
			}
			r.fillItemBuffer(r.index)
			r.currentItemReadRequired = true
		}
	}

	r.isEOF = true
	return r.offsetOfCurrentRead, nil
}

// fillItemBuffer stringifies and reads data into r.itemBuffer, separates objects and fields values with given separators.
func (r *Reader) fillItemBuffer(i int) {

	var stringifiedFieldValues []string
	var wasString []bool

	r.itemBuffer.reset()
	record := r.valueAt(i)
	stringifiedFieldValues, wasString = r.stringifier(record)

	if i != 0 {
		r.itemBuffer.writeString(r.config.ObjectSeparator)
	}

	for j := 0; j < len(stringifiedFieldValues); j++ {
		if j != 0 {
			r.itemBuffer.writeString(r.config.FieldSeparator)
		}

		stringifiedFieldValues[j] = r.escapeSpecialChars(stringifiedFieldValues[j])
		if wasString[j] {
			stringifiedFieldValues[j] = r.config.EncloseBy + stringifiedFieldValues[j] + r.config.EncloseBy
		}
		r.itemBuffer.writeString(stringifiedFieldValues[j])
	}

	r.itemBuffer.offset = 0
}

func (r *Reader) escapeSpecialChars(value string) string {
	value = strings.ReplaceAll(value, r.config.EscapeBy, r.config.EscapeBy+r.config.EscapeBy)
	value = strings.ReplaceAll(value, r.config.FieldSeparator, r.config.EscapeBy+r.config.FieldSeparator)
	if !r.config.Stringify.IgnoreObjetSeparator {
		value = strings.ReplaceAll(value, r.config.ObjectSeparator, r.config.EscapeBy+r.config.ObjectSeparator)
	}
	if !r.config.Stringify.IgnoreEncloseBy {
		value = strings.ReplaceAll(value, r.config.EncloseBy, r.config.EscapeBy+r.config.EncloseBy)
	}
	return value
}

// NewReader returns Reader instance and actual data struct type.
// e.g. data is type of []*Foo - returns Foo.
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
		config:                  config,
		valueAt:                 valueAt,
		itemCount:               size,
		stringifier:             stringifier,
		itemBuffer:              NewBuffer(1024),
		offsetOfCurrentRead:     0,
		index:                   0,
		currentItemReadRequired: true,
	}, structType, nil
}

func (r *Reader) ItemCount() int {
	return r.itemCount
}
