package aerospike

import (
	"bytes"
	"github.com/viant/sqlx/io/read/cache"
	"io"
	"io/ioutil"
	"sync"
)

type MultiReader struct {
	mux     sync.Mutex
	readers []*Reader
	buffer  bytes.Buffer
}

func (m *MultiReader) Close() error {
	var err error
	for _, reader := range m.readers {
		if errr := reader.Close(); errr != nil {
			err = errr
		}
	}

	return err
}

func (m *MultiReader) Read(p []byte) (n int, err error) {
	for _, reader := range m.readers {
		all, err := ioutil.ReadAll(reader)
		if err != nil {
			return 0, err
		}

		m.buffer.Write(all)
	}

	m.readers = nil
	if m.buffer.Len() == 0 {
		return 0, io.EOF
	}

	if m.buffer.Len() < len(p) {
		m.buffer.Reset()
		return copy(p, m.buffer.Bytes()), nil
	}

	return 0, nil
}

func (m *MultiReader) ReadLine() (line []byte, prefix bool, err error) {
	if len(m.readers) == 0 {
		return nil, false, io.EOF
	}

	aLine, _, err := m.readLine()
	if err == io.EOF {
		m.readers = m.readers[1:]
		return m.ReadLine()
	}

	return aLine, false, err
}

func (m *MultiReader) readLine() ([]byte, bool, error) {
	line, err := cache.ReadLine(m.readers[0])
	return line, false, err
}

func (m *MultiReader) AddReader(reader *Reader) {
	m.mux.Lock()
	m.readers = append(m.readers, reader)
	m.mux.Unlock()
}
