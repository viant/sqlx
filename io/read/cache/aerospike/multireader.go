package aerospike

import (
	"bytes"
	"github.com/viant/sqlx/io/read/cache"
	"io"
	"io/ioutil"
	"sync"
)

type (
	MultiReader struct {
		matcher       *cache.ParmetrizedQuery
		mux           sync.Mutex
		readers       []*Reader
		buffer        bytes.Buffer
		readSoFar     int
		currentReader *Reader
	}
)

func NewMultiReader(matcher *cache.ParmetrizedQuery) *MultiReader {
	return &MultiReader{
		matcher: matcher,
	}
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
	return m.recordReadLiner()
}

func (m *MultiReader) AddReader(reader *Reader) {
	m.mux.Lock()
	m.readers = append(m.readers, reader)
	m.mux.Unlock()
}

func (m *MultiReader) recordReadLiner() ([]byte, bool, error) {
	if m.currentReader == nil {
		m.currentReader = m.recordReader()
	}

	for {
		if m.currentReader == nil {
			return nil, false, io.EOF
		}

		line, prefix, err := m.currentReader.ReadLine()
		m.readSoFar++
		if err == nil && ((m.readSoFar <= m.matcher.Limit) || m.matcher.Limit <= 0) {
			return line, prefix, err
		}

		m.currentReader = m.recordReader()

		m.readSoFar = 0
	}
}

func (m *MultiReader) recordReader() *Reader {
	for len(m.readers) > 0 {
		if m.matcher.Offset == 0 {
			reader := m.readers[0]
			m.readers = m.readers[1:]
			return reader
		}

		reader := m.readers[0]
		var lastErr error
		for i := 0; i < m.matcher.Offset; i++ {
			_, _, lastErr = reader.ReadLine()
		}

		if lastErr == nil {
			return reader
		}

		m.readers = m.readers[1:]
	}

	return nil
}
