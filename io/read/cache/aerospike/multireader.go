package aerospike

import (
	"bytes"
	"fmt"
	"github.com/viant/sqlx/io/read/cache"
	"io"
	"io/ioutil"
	"sort"
	"sync"
)

type (
	MultiReader struct {
		mux     sync.Mutex
		readers []*Reader
		buffer  bytes.Buffer

		index     map[int]int
		readOrder []int
		sorted    bool

		readSoFar int
		toSkip    int
		limit     int
	}
)

func NewMultiReader(matcher *cache.SmartMatcher) *MultiReader {
	return &MultiReader{
		index:  map[int]int{},
		toSkip: matcher.Offset,
		limit:  matcher.Limit,
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
	m.sortIfNeeded()
	if err = m.skipLines(); err != nil {
		return nil, false, err
	}

	if m.readSoFar == m.limit && m.limit != 0 {
		return nil, false, io.EOF
	}

	for len(m.readOrder) > 0 {
		reader, ok := m.popReader()
		if !ok {
			return nil, false, io.EOF
		}

		m.readSoFar++
		return m.readLine(reader)
	}

	return nil, false, io.EOF
}

func (m *MultiReader) readLine(reader *Reader) ([]byte, bool, error) {
	line, err := cache.ReadLine(reader)
	return line, false, err
}

func (m *MultiReader) AddReader(reader *Reader) {
	m.mux.Lock()

	actualLen := len(m.readers)

	m.readOrder = append(m.readOrder, reader.order...)

	for _, i := range reader.order {
		m.index[i] = actualLen
	}

	m.readers = append(m.readers, reader)
	m.mux.Unlock()
}

func (m *MultiReader) sortIfNeeded() {
	if m.sorted {
		return
	}

	sort.Ints(m.readOrder)
	m.sorted = true
}

func (m *MultiReader) skipLines() error {
	for m.toSkip > 0 && len(m.readOrder) > 0 {
		m.toSkip--
		reader, ok := m.popReader()
		if !ok {
			return fmt.Errorf("no reader found")
		}

		_, _, err := m.readLine(reader)
		if err != nil && err != io.EOF {
			return err
		}
	}

	return nil
}

func (m *MultiReader) popReader() (*Reader, bool) {
	readerIndex, ok := m.index[m.readOrder[0]]

	if !ok {
		return nil, false
	}

	m.readOrder = m.readOrder[1:]
	return m.readers[readerIndex], true
}
