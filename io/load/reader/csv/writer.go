package csv

import (
	"bytes"
	io2 "github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
	"reflect"
)

type writer struct {
	beforeFirst   string
	writtenObject bool
	dereferencer  *xunsafe.Type
	buffer        *Buffer
	config        *Config
	accessor      *Accessor
	valueAt       io2.ValueAccessor
	size          int
}

func newWriter(accessor *Accessor, config *Config, buffer *Buffer, dereferencer *xunsafe.Type, valueAt io2.ValueAccessor, size int, beforeFirst string) *writer {
	return &writer{
		dereferencer: dereferencer,
		buffer:       buffer,
		config:       config,
		accessor:     accessor,
		valueAt:      valueAt,
		size:         size,
		beforeFirst:  beforeFirst,
	}
}

func (w *writer) writeObjects(headers []string) {
	w.writeHeadersIfNeeded(headers)
	var xType *xunsafe.Type

	for i := 0; i < w.size; i++ {
		if i != 0 {
			w.accessor.Reset()
		}

		at := w.valueAt(i)
		if i == 0 {
			if reflect.TypeOf(at).Kind() == reflect.Ptr {
				xType = w.dereferencer
			}
		}

		if xType != nil {
			at = xType.Deref(at)
		}

		w.accessor.Set(xunsafe.AsPointer(at))
		for w.accessor.Has() {
			w.accessor.Stringify(w)
		}
	}
}

func (w *writer) writeHeadersIfNeeded(headers []string) {
	if len(headers) == 0 {
		return
	}

	wasHeaderString := make([]bool, 0, len(headers))
	for range headers {
		wasHeaderString = append(wasHeaderString, true)
	}

	w.writeObject(headers, wasHeaderString)
}

func (w *writer) writeObject(data []string, wasStrings []bool) {
	if w.writtenObject {
		w.writeObjectSeparator()
	} else {
		w.buffer.writeString(w.beforeFirst)
	}

	WriteObject(w.buffer, w.config, data, wasStrings)
	w.writtenObject = true
}

func (w *writer) appendObject(data []string, wasStrings []bool) {
	if !bytes.HasSuffix(w.buffer.buffer, []byte(w.config.ObjectSeparator)) {
		w.buffer.writeString(w.config.FieldSeparator)
	}

	WriteObject(w.buffer, w.config, data, wasStrings)
}

func (w *writer) writeObjectSeparator() {
	w.buffer.writeString(w.config.ObjectSeparator)
}
