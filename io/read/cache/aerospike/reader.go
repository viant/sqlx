package aerospike

import (
	"bufio"
	"bytes"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	sio "io"
)

type (
	Reader struct {
		namespace string
		cache     *Cache
		record    *as.Record
		key       *as.Key

		reader *bufio.Reader
		set    string
	}

	readerWrapper struct {
		err    error
		reader *Reader
	}
)

func (r *Reader) ReadLine() (line []byte, prefix bool, err error) {
	if err := r.ensureReader(); err != nil {
		return nil, false, err
	}

	readLine, isPrefix, err := r.reader.ReadLine()
	child := r.record.Bins[childBin]
	if err != nil {
		if err == sio.EOF && len(readLine) == 0 && child != nil {
			if err = r.fetchChild(child); err != nil {
				return nil, false, err
			}
			return r.ReadLine()
		}
		return readLine, isPrefix, err
	}

	if isPrefix {
		var combined []byte
		combined = append(combined, readLine...)
		for isPrefix {
			var chunk []byte
			chunk, isPrefix, err = r.reader.ReadLine()
			if err != nil {
				return combined, isPrefix, err
			}
			combined = append(combined, chunk...)
		}
		readLine = combined
	}

	if len(readLine) == 0 && child != nil {
		if err = r.fetchChild(child); err != nil {
			return nil, false, err
		}

		return r.ReadLine()
	}

	return readLine, isPrefix, err
}

func (r *Reader) Close() error {
	return nil
}

func (r *Reader) Read(b []byte) (int, error) {
	if err := r.ensureReader(); err != nil {
		return 0, err
	}

	return r.reader.Read(b)
}

func (r *Reader) ensureReader() error {
	if r.reader != nil {
		return nil
	}

	content, err := r.dataContent()
	if err != nil {
		return err
	}

	r.reader = bufio.NewReader(bytes.NewBuffer(content))
	return nil
}

func (r *Reader) dataContent() ([]byte, error) {
	if data, ok := r.record.Bins[compDataBin]; ok {
		return uncompress(data.([]byte))
	}

	data := r.record.Bins[dataBin]
	if data == nil {
		return []byte{}, nil
	}

	dataString, ok := data.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected cache value type, expected %T, but got %T", dataString, data)
	}

	return []byte(dataString), nil
}

func (r *Reader) fetchChild(childKeyValue interface{}) error {
	key, err := as.NewKey(r.namespace, r.set, childKeyValue)
	if err != nil {
		return err
	}

	r.record, err = r.cache.getRecord(key, indexedReadBins()...)

	if err != nil {
		return err
	}

	r.reader = nil
	return r.ensureReader()
}

func indexedReadBins() []string {
	return []string{dataBin, compDataBin, childBin}
}
