package aerospike

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/google/uuid"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/afs"
	"sync"
	"time"
)

const (
	sqlBin    = "SQL"
	argsBin   = "Args"
	dataBin   = "Data"
	typesBin  = "Type"
	fieldsBin = "Fields"
	childBin  = "Child"
)

type (
	Cache struct {
		recorder          cache.Recorder
		typeHolder        *cache.ScanTypeHolder
		client            *as.Client
		set               string
		namespace         string
		mux               sync.Mutex
		expirationTimeInS uint32
	}
)

func (a *Cache) Rollback(ctx context.Context, entry *cache.Entry) error {
	return a.Delete(ctx, entry)
}

func New(namespace string, setName string, client *as.Client, expirationTimeInS uint32, options ...interface{}) (*Cache, error) {
	var recorder cache.Recorder
	for _, anOption := range options {
		switch actual := anOption.(type) {
		case cache.Recorder:
			recorder = actual
		}
	}

	return &Cache{
		client:            client,
		namespace:         namespace,
		set:               setName,
		recorder:          recorder,
		expirationTimeInS: expirationTimeInS,
	}, nil
}

func (a *Cache) AsSource(ctx context.Context, entry *cache.Entry) (cache.Source, error) {
	return &Source{
		cache: a,
		entry: entry,
	}, nil
}

func (a *Cache) AddValues(ctx context.Context, entry *cache.Entry, values []interface{}) error {
	if a.recorder != nil {
		a.recorder.AddValues(values)
	}

	marshal, err := json.Marshal(values)
	if err != nil {
		return err
	}

	return entry.Write(marshal)
}

func (a *Cache) Get(ctx context.Context, SQL string, args []interface{}) (*cache.Entry, error) {
	aKey, err := afs.GenerateURL(SQL, "", "", args)
	if err != nil {
		return nil, err
	}

	key, err := a.key(aKey)
	if err != nil {
		return nil, err
	}

	record, _ := a.client.Get(as.NewPolicy(), key, []string{typesBin, argsBin, sqlBin, dataBin, fieldsBin}...)
	argsMarshal, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	anEntry := &cache.Entry{
		Meta: cache.Meta{
			SQL:        SQL,
			Args:       argsMarshal,
			TimeToLive: int(time.Now().Add(time.Duration(a.expirationTimeInS)).UnixNano()),
		},
		Id: aKey,
	}

	if !a.recordMatches(record, SQL, argsMarshal) {
		if record != nil {
			_ = a.Delete(ctx, anEntry)
		}

		anEntry.Id += uuid.New().String()
		writer := a.newWriter(key, aKey, SQL, argsMarshal)
		anEntry.SetWriter(writer, writer)
		writer.entry = anEntry

		return anEntry, nil
	}

	if err = a.updateEntry(record, anEntry); err != nil {
		return nil, err
	}

	reader := a.reader(key, record)
	anEntry.SetReader(reader, reader)
	return anEntry, nil
}

func (a *Cache) AssignRows(entry *cache.Entry, rows *sql.Rows) error {
	return entry.AssignRows(rows)
}

func (a *Cache) UpdateType(ctx context.Context, entry *cache.Entry, args []interface{}) (bool, error) {
	a.ensureTypeHolder(args)

	if !a.typeHolder.Match(entry) {
		return false, a.Delete(ctx, entry)
	}

	return true, nil
}

func (a *Cache) Close(ctx context.Context, entry *cache.Entry) error {
	err := entry.Close()
	if err != nil {
		_ = a.Delete(ctx, entry)
		return err
	}

	return nil
}

func (a *Cache) Delete(ctx context.Context, entry *cache.Entry) error {
	key, err := a.key(entry.Id)
	if err != nil {
		return err
	}

	return a.deleteCascade(key)
}

func (a *Cache) deleteCascade(key *as.Key) error {
	var err error
	aRecord, _ := a.client.Get(as.NewPolicy(), key, childBin)
	var ok bool
	for aRecord != nil {
		if ok, err = a.client.Delete(as.NewWritePolicy(0, a.expirationTimeInS), key); err != nil || !ok {
			return err
		}

		childKey := aRecord.Bins[childBin]
		if childKey == nil {
			return nil
		}

		key, err = a.key(childKey)
	}

	return nil
}

func (a *Cache) recordMatches(record *as.Record, SQL string, args []byte) bool {
	if record == nil {
		return false
	}

	sqlValue, ok := record.Bins[sqlBin].(string)
	if !ok || sqlValue != SQL {
		return false
	}

	argsValue, ok := record.Bins[argsBin].(string)
	if !ok || argsValue != string(args) {
		return false
	}

	return true
}

func (a *Cache) newWriter(key *as.Key, aKey string, SQL string, args []byte) *Writer {
	return &Writer{
		client:                  a.client,
		expirationTimeInSeconds: a.expirationTimeInS,
		mainKey:                 key,
		buffers:                 []*bytes.Buffer{bytes.NewBuffer(nil)},
		id:                      aKey,
		sql:                     SQL,
		args:                    string(args),
		cache:                   a,
	}
}

func (a *Cache) key(keyValue interface{}) (*as.Key, error) {
	aKey, err := as.NewKey(a.namespace, a.set, keyValue)
	return aKey, err
}

func (a *Cache) reader(key *as.Key, record *as.Record) *Reader {
	return &Reader{
		key:       key,
		client:    a.client,
		namespace: a.namespace,
		record:    record,
		set:       a.set,
	}
}

func (a *Cache) ensureTypeHolder(values []interface{}) {
	if a.typeHolder != nil {
		return
	}

	a.mux.Lock()
	a.typeHolder = &cache.ScanTypeHolder{}
	a.typeHolder.InitType(values)
	a.mux.Unlock()
}

func (a *Cache) updateEntry(record *as.Record, entry *cache.Entry) error {
	return a.updateEntryFields(record, entry)
}

func (a *Cache) updateEntryFields(record *as.Record, entry *cache.Entry) error {
	fieldsValue := record.Bins[fieldsBin]
	if fieldsValue == nil {
		return nil
	}

	fieldsJSON, ok := fieldsValue.(string)
	if !ok {
		return fmt.Errorf("unexpected cache value type, expected %T got %T", fieldsJSON, fieldsValue)
	}

	var fields []*cache.Field
	if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
		return err
	}

	entry.Meta.Fields = fields

	for _, field := range entry.Meta.Fields {
		if err := field.Init(); err != nil {
			return err
		}
	}

	return nil
}
