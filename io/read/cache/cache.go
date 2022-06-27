package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/xunsafe"
	"hash/fnv"
	"reflect"
	"strconv"
	"sync"
	"time"
)

type (
	Cache struct {
		storage   string
		afs       afs.Service
		ttl       time.Duration
		extension string

		scanTypes []*xunsafe.Type
		scanners  []*Scanner
		mux       sync.Mutex
	}
)

func NewCache(storage string, ttl time.Duration) *Cache {
	return &Cache{
		afs:       afs.New(),
		ttl:       ttl,
		storage:   storage,
		extension: ".json",
	}
}

func (c *Cache) Get(ctx context.Context, SQL string, args []interface{}) (*Entry, error) {
	URL, err := c.generateURL(SQL, args)
	if err != nil {
		return nil, err
	}

	entry := &Entry{
		URL:   URL,
		SQL:   SQL,
		Args:  args,
		cache: c,
	}

	data, ok, err := c.readData(ctx, URL)

	if !ok || err != nil {
		return entry, err
	}

	if err = json.Unmarshal(data, entry); err != nil {
		return entry, err
	}

	if entry.SQL != SQL && !argsEqual(entry.Args, args) {
		entry.URL = URL
		entry.Data = nil
		entry.Args = args

		return entry, c.afs.Delete(ctx, URL)
	}

	entry.found = true
	return entry, nil
}

func argsEqual(x []interface{}, y []interface{}) bool {
	if len(x) != len(y) {
		return false
	}

	for index, value := range x {
		if y[index] != value {
			return false
		}
	}

	return true
}

func (c *Cache) Put(ctx context.Context, entry *Entry) error {
	dataMarshal, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	expireAt := time.Now().Add(c.ttl)
	nano := []byte(strconv.Itoa(int(expireAt.UnixNano())))
	return c.afs.Upload(ctx, entry.URL, file.DefaultFileOsMode, bytes.NewReader(append(nano, dataMarshal...)))
}

func (c *Cache) generateURL(SQL string, args []interface{}) (string, error) {
	argMarshal, err := json.Marshal(args)
	if err != nil {
		return "", err
	}

	hasher := fnv.New64()
	_, err = hasher.Write(append([]byte(SQL), argMarshal...))

	if err != nil {
		return "", err
	}

	entryKey := strconv.Itoa(int(hasher.Sum64()))
	return c.storage + entryKey + c.extension, nil
}

func (c *Cache) readData(ctx context.Context, URI string) ([]byte, bool, error) {
	if ok, _ := c.afs.Exists(ctx, URI); !ok {
		return nil, false, nil
	}

	data, err := c.afs.DownloadWithURL(ctx, URI)
	if err != nil {
		return nil, false, err
	}

	expireTime, err := strconv.Atoi(string(data[:19]))
	if err != nil {
		return nil, false, err
	}

	inNano := time.Unix(0, int64(expireTime))
	if time.Now().After(inNano) {
		return nil, false, c.afs.Delete(ctx, URI, option.NewObjectKind(true))
	}

	return data[19:], true, nil
}

func (c *Cache) CreateXTypes(args []interface{}) {
	if c.scanTypes != nil {
		return
	}

	c.mux.Lock()
	defer c.mux.Unlock()

	c.scanTypes = make([]*xunsafe.Type, len(args))
	c.scanners = make([]*Scanner, len(args))
	for i, arg := range args {
		c.scanTypes[i] = xunsafe.NewType(reflect.TypeOf(arg).Elem())
		c.scanners[i] = NewScanner(arg)
	}
}
