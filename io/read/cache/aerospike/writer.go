package aerospike

import (
	"bytes"
	"encoding/json"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/viant/sqlx/io/read/cache"
	"strconv"
)

type Writer struct {
	client   *as.Client
	mainKey  *as.Key
	buffers  []*bytes.Buffer
	id       string
	children []string
	sql      string
	args     string
	fields   *string
	entry    *cache.Entry
	cache    *Cache

	expirationTimeInSeconds uint32
}

func (w *Writer) Flush() error {
	var err error

	if err = w.ensureFields(); err != nil {
		return err
	}

	var childKey *as.Key
	var previousKeyValue string
	for i := len(w.buffers) - 1; i >= 0; i-- {
		childKeyValue := w.id
		if i != 0 {
			childKeyValue += "#" + strconv.Itoa(i)
		}

		binMap := w.binMap(i, previousKeyValue)

		lastInsertedKey, err := w.cache.key(childKeyValue)
		if err != nil {
			return err
		}

		policy := w.cache.writePolicy()
		if err = w.client.Put(policy, lastInsertedKey, binMap); err != nil {
			w.delete(childKey)
			return err
		}

		childKey = lastInsertedKey
		previousKeyValue = childKeyValue
	}

	return nil
}

func (w *Writer) Close() error {
	return nil
}

func (w *Writer) Write(b []byte) (int, error) {
	if err := w.ensureFields(); err != nil {
		return 0, err
	}

	lastBuffer := w.lastBuffer()
	if !w.fitsInBuffer(lastBuffer, b) {
		lastBuffer = w.newChild()
	}

	if lastBuffer.Len() != 0 {
		lastBuffer.WriteByte('\n')
	}

	lastBuffer.Write(b)
	return len(b), nil
}

func (w *Writer) ensureFields() error {
	if w.fields != nil {
		return nil
	}

	marshal, err := json.Marshal(w.entry.Meta.Fields)
	if err != nil {
		return err
	}

	asString := string(marshal)
	w.fields = &asString
	return nil
}

func (w *Writer) lastBuffer() *bytes.Buffer {
	return w.buffers[len(w.buffers)-1]
}

func (w *Writer) fitsInBuffer(buffer *bytes.Buffer, newData []byte) bool {
	return buffer.Len()+len(newData)+len(*w.fields)+len(w.sql)+len(w.args) < availableSize
}

func (w *Writer) newChild() *bytes.Buffer {
	buffer := bytes.NewBufferString("")
	w.buffers = append(w.buffers, buffer)
	return buffer
}

func (w *Writer) delete(key *as.Key) {
	if key == nil {
		return
	}

	err := w.cache.deleteCascade(key)
	if err != nil {
		fmt.Printf("error while removing entry %v\n", err.Error())
	}
}

func (w *Writer) binMap(i int, childKey string) as.BinMap {
	binMap := as.BinMap{dataBin: w.buffers[i].String()}
	if childKey != "" {
		binMap[childBin] = childKey
	}

	if i == 0 {
		binMap[sqlBin] = w.sql
		binMap[argsBin] = w.args
		binMap[fieldsBin] = *w.fields
	}

	return binMap
}
