package aerospike

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/viant/sqlx/io/read/cache"
)

type MultiReader struct {
	matcher       *cache.ParmetrizedQuery
	mux           sync.Mutex
	readers       []*Reader
	buffer        bytes.Buffer
	bufferPrefix  bool
	readSoFar     int
	currentReader *Reader
}

func NewMultiReader(matcher *cache.ParmetrizedQuery) *MultiReader {
	if matcher == nil {
		matcher = &cache.ParmetrizedQuery{}
	}
	return &MultiReader{matcher: matcher}
}

func (m *MultiReader) Close() error {
	var result error
	if m.currentReader != nil {
		result = m.currentReader.Close()
		m.currentReader = nil
	}
	for _, reader := range m.readers {
		result = errors.Join(result, reader.Close())
	}
	m.readers = nil
	m.buffer.Reset()
	return result
}

// Read and ReadLine consume the same windowed row stream. Buffering one fragment
// preserves forward progress for callers using buffers smaller than a row.
func (m *MultiReader) Read(target []byte) (int, error) {
	if len(target) == 0 {
		return 0, nil
	}
	for m.buffer.Len() == 0 {
		line, prefix, err := m.recordReadLiner()
		if err != nil {
			return 0, err
		}
		m.buffer.Write(line)
		m.bufferPrefix = prefix
		if !prefix {
			m.buffer.WriteByte('\n')
		}
	}
	return m.buffer.Read(target)
}

func (m *MultiReader) ReadLine() ([]byte, bool, error) {
	if m.buffer.Len() > 0 {
		line := m.buffer.Next(m.buffer.Len())
		if !m.bufferPrefix && len(line) > 0 {
			line = line[:len(line)-1]
		}
		return line, m.bufferPrefix, nil
	}
	return m.recordReadLiner()
}

func (m *MultiReader) AddReader(reader *Reader) {
	m.mux.Lock()
	m.readers = append(m.readers, reader)
	m.mux.Unlock()
}

func (m *MultiReader) recordReadLiner() ([]byte, bool, error) {
	for {
		if m.currentReader == nil {
			var err error
			m.currentReader, err = m.recordReader()
			if err != nil {
				return nil, false, err
			}
			if m.currentReader == nil {
				return nil, false, io.EOF
			}
			m.readSoFar = 0
		}
		if m.matcher.Limit > 0 && m.readSoFar >= m.matcher.Limit {
			err := m.currentReader.Close()
			m.currentReader = nil
			if err != nil {
				return nil, false, err
			}
			continue
		}
		line, prefix, err := m.currentReader.ReadLine()
		if err != nil {
			closeErr := m.currentReader.Close()
			m.currentReader = nil
			if !errors.Is(err, io.EOF) {
				return nil, false, errors.Join(err, closeErr)
			}
			if closeErr != nil {
				return nil, false, closeErr
			}
			continue
		}
		if !prefix {
			m.readSoFar++
		}
		return line, prefix, nil
	}
}

func (m *MultiReader) recordReader() (*Reader, error) {
	for len(m.readers) > 0 {
		reader := m.readers[0]
		m.readers = m.readers[1:]
		skipped := 0
		for skipped < m.matcher.Offset {
			_, prefix, err := reader.ReadLine()
			if err != nil {
				closeErr := reader.Close()
				if !errors.Is(err, io.EOF) {
					return nil, errors.Join(err, closeErr)
				}
				if closeErr != nil {
					return nil, closeErr
				}
				reader = nil
				break
			}
			if !prefix {
				skipped++
			}
		}
		if reader != nil {
			return reader, nil
		}
	}
	return nil, nil
}
