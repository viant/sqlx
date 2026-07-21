package aerospike

import (
	"bytes"
	"encoding/json"
	as "github.com/aerospike/aerospike-client-go"
	"strconv"
)

type indexedValue struct {
	ColumnValue interface{}
	Column      string

	cache             *Cache
	baseKey           string
	metaBin           as.BinMap
	includeMetaOnRoot bool

	chunkIndex int
	buffer     *bytes.Buffer
}

func newIndexedValue(cache *Cache, columnValue interface{}, column string, url string, metaBin as.BinMap, includeMetaOnRoot bool) (*indexedValue, error) {
	baseKey := url
	if column != "" {
		marshal, err := json.Marshal(columnValue)
		if err != nil {
			return nil, err
		}
		baseKey = cache.columnValueURL(column, marshal, url)
	}

	return &indexedValue{
		ColumnValue:       columnValue,
		Column:            column,
		cache:             cache,
		baseKey:           baseKey,
		metaBin:           metaBin,
		includeMetaOnRoot: includeMetaOnRoot,
		buffer:            bytes.NewBuffer(nil),
	}, nil
}

func (i *indexedValue) appendRow(data []interface{}) (int, error) {
	before := i.buffer.Len()
	encoded, err := json.Marshal(data)
	if err != nil {
		return 0, err
	}

	needed := len(encoded)
	if i.buffer.Len() > 0 {
		needed++
	}
	if i.buffer.Len() > 0 && !fitsIndexedChunk(i.buffer, needed) {
		if err = i.flush(i.chunkKey(i.chunkIndex + 1)); err != nil {
			return 0, err
		}
		i.chunkIndex++
	}

	if i.buffer.Len() > 0 {
		if err = i.buffer.WriteByte('\n'); err != nil {
			return 0, err
		}
	}

	if _, err = i.buffer.Write(encoded); err != nil {
		return 0, err
	}
	return i.buffer.Len() - before, nil
}

func (i *indexedValue) close() error {
	if i.buffer.Len() == 0 && i.chunkIndex == 0 && i.includeMetaOnRoot {
		key, err := i.cache.key(i.chunkKey(i.chunkIndex))
		if err != nil {
			return err
		}
		binMap := cloneBinMap(i.metaBin)
		binMap[dataBin] = ""
		return i.cache.put(key, binMap)
	}
	return i.flush("")
}

func (i *indexedValue) flush(nextChunkKey string) error {
	if i.buffer.Len() == 0 {
		return nil
	}

	binMap := as.BinMap{}
	if i.chunkIndex == 0 && i.includeMetaOnRoot {
		binMap = cloneBinMap(i.metaBin)
	}

	data := i.buffer.Bytes()
	isCompressed := false
	if len(data) > compressionThreshold {
		compressed, ok := compress(data)
		if ok {
			binMap[compDataBin] = compressed
			isCompressed = true
		}
	}
	if !isCompressed {
		binMap[dataBin] = string(data)
	}
	if nextChunkKey != "" {
		binMap[childBin] = nextChunkKey
	}

	key, err := i.cache.key(i.chunkKey(i.chunkIndex))
	if err != nil {
		return err
	}
	if err = i.cache.put(key, binMap); err != nil {
		return err
	}

	i.buffer.Reset()
	return nil
}

func (i *indexedValue) chunkKey(index int) string {
	if index == 0 {
		return i.baseKey
	}
	return i.baseKey + "#" + strconv.Itoa(index)
}

func cloneBinMap(source as.BinMap) as.BinMap {
	if len(source) == 0 {
		return as.BinMap{}
	}
	result := make(as.BinMap, len(source))
	for k, v := range source {
		result[k] = v
	}
	return result
}

func fitsIndexedChunk(buffer *bytes.Buffer, newDataLen int) bool {
	return buffer.Len()+newDataLen < availableSize
}
