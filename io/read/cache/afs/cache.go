package afs

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/hash"
	"strings"
	"sync"
	"time"
)

const (
	NotExistStatus = iota
	InUseStatus
	ErrorStatus
	ExistsStatus
)

type (
	Cache struct {
		typeHolder *cache.ScanTypeHolder

		storage   string
		afs       afs.Service
		ttl       time.Duration
		extension string

		mux       sync.RWMutex
		signature string
		canWrite  map[string]bool
		stream    *option.Stream
		recorder  cache.Recorder
	}
)

func (c *Cache) IndexBy(ctx context.Context, db *sql.DB, column, SQL string, args []interface{}) (int, error) {
	return 0, nil
}

func (c *Cache) Rollback(ctx context.Context, entry *cache.Entry) error {
	return c.Delete(ctx, entry)
}

// NewCache creates new cache.
func NewCache(URL string, ttl time.Duration, signature string, stream *option.Stream, options ...interface{}) (*Cache, error) {
	var recorder cache.Recorder
	for _, anOption := range options {
		switch actual := anOption.(type) {
		case cache.Recorder:
			recorder = actual
		}
	}

	if URL[len(URL)-1] != '/' {
		URL += "/"
	}
	cache := &Cache{
		afs:       afs.New(),
		ttl:       ttl,
		storage:   URL,
		extension: ".json",
		signature: signature,
		canWrite:  map[string]bool{},
		stream:    stream,
		recorder:  recorder,
	}

	return cache, nil
}

func (c *Cache) Get(ctx context.Context, SQL string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	URL, err := hash.GenerateURL(SQL, c.storage, c.extension, args)
	if err != nil {
		return nil, err
	}

	if c.mark(URL) {
		return nil, nil
	}

	entry, err := c.getEntry(ctx, SQL, args, err, URL)
	if err != nil || entry == nil {
		c.unmark(URL)
		return entry, err
	}

	if entry.Has() {
		c.unmark(URL)
	}

	return entry, err
}

func (c *Cache) getEntry(ctx context.Context, SQL string, args []interface{}, err error, URL string) (*cache.Entry, error) {
	argsMarshal, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	entry := &cache.Entry{
		Meta: cache.Meta{
			SQL:       SQL,
			Args:      argsMarshal,
			URL:       URL,
			Signature: c.signature,
		},
	}

	status, err := c.updateEntry(ctx, err, URL, entry)
	if err != nil {
		return nil, err
	}

	switch status {
	case InUseStatus:
		return nil, nil
	case ErrorStatus:
		return nil, err
	}

	return entry, nil
}

func (c *Cache) updateEntry(ctx context.Context, err error, URL string, entry *cache.Entry) (int, error) {
	status, err := c.readData(ctx, entry)
	if status == NotExistStatus || status == InUseStatus || err != nil {
		if status == NotExistStatus {
			id := strings.ReplaceAll(uuid.New().String(), "-", "")
			entry.Meta.URL += id
			entry.Id = id
		}

		if err == nil {
			c.mux.RLock()
			c.canWrite[URL] = false
			c.mux.RUnlock()
		}

		return status, err
	}

	metaCorrect, err := c.checkMeta(entry.ReadCloser, &entry.Meta)
	if !metaCorrect || err != nil {
		return status, c.afs.Delete(ctx, URL)
	}

	return status, nil
}

func (c *Cache) checkMeta(dataReader cache.LineReader, entryMeta *cache.Meta) (bool, error) {
	data, err := cache.ReadLine(dataReader)
	meta := cache.Meta{}
	if err = json.Unmarshal(data, &meta); err != nil {
		return false, nil
	}

	if c.expired(meta) || c.wrongSignature(meta, entryMeta) || c.wrongSQL(meta, entryMeta) || c.wrongArgs(meta, entryMeta) {
		return false, nil
	}

	entryMeta.Type = meta.Type
	entryMeta.Fields = meta.Fields

	for _, field := range entryMeta.Fields {
		if err = field.Init(); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (c *Cache) readData(ctx context.Context, entry *cache.Entry) (int, error) {
	if ok, err := c.afs.Exists(ctx, entry.Meta.URL); !ok || err != nil {
		return NotExistStatus, nil
	}

	afsReader, err := c.afs.OpenURL(ctx, entry.Meta.URL, c.stream)
	if isRateError(err) || isPreConditionError(err) {
		return InUseStatus, nil
	}

	if err != nil {
		return ErrorStatus, nil
	}

	reader := bufio.NewReader(afsReader)
	if err != nil {
		return ErrorStatus, err
	}

	entry.SetReader(reader, afsReader)
	return ExistsStatus, nil
}

func (c *Cache) wrongArgs(meta cache.Meta, entryMeta *cache.Meta) bool {
	return !bytes.Equal(meta.Args, entryMeta.Args)
}

func (c *Cache) wrongSQL(meta cache.Meta, entryMeta *cache.Meta) bool {
	return meta.SQL != entryMeta.SQL
}

func (c *Cache) wrongSignature(meta cache.Meta, entryMeta *cache.Meta) bool {
	return meta.Signature != entryMeta.Signature
}

func (c *Cache) expired(meta cache.Meta) bool {
	return int(cache.Now().UnixNano()) > meta.TimeToLive
}

func (c *Cache) writeMeta(ctx context.Context, m *cache.Entry) error {
	writer, err := c.afs.NewWriter(ctx, m.Meta.URL, 0644, &option.SkipChecksum{Skip: true})
	if err != nil {
		return err
	}
	if writer == nil {
		return fmt.Errorf("invalid writer location: %v", m.Meta.URL)
	}

	bufioWriter := bufio.NewWriterSize(writer, 2048)
	m.WriteCloser = cache.NewWriteCloser(cache.NewLineWriter(bufioWriter), writer)

	m.Meta.TimeToLive = int(cache.Now().Add(c.ttl).UnixNano())
	data, err := json.Marshal(m.Meta)
	if err != nil {
		return err
	}

	if err = m.Write(data); err != nil {
		return err
	}

	return nil
}

func (c *Cache) UpdateType(ctx context.Context, entry *cache.Entry, values []interface{}) (bool, error) {
	c.initializeCacheType(values)

	if !c.typeHolder.Match(entry) {
		return false, c.Delete(ctx, entry)
	}

	return true, nil
}

func (c *Cache) Delete(ctx context.Context, entry *cache.Entry) error {
	return c.afs.Delete(ctx, entry.Meta.URL)
}

func (c *Cache) mark(URL string) bool {
	c.mux.RLock()
	_, isInMap := c.canWrite[URL]
	c.canWrite[URL] = false
	c.mux.RUnlock()
	return isInMap
}

func (c *Cache) unmark(url string) {
	c.mux.RLock()
	delete(c.canWrite, url)
	c.mux.RUnlock()
}

func (c *Cache) scanner(e *cache.Entry) cache.ScannerFn {
	return cache.NewScanner(c.typeHolder, c.recorder).New(e)
}

func (c *Cache) Close(ctx context.Context, e *cache.Entry) error {
	actualURL := strings.ReplaceAll(e.Meta.URL, ".json"+e.Id, ".json")
	defer c.unmark(actualURL)
	err := c.close(e)
	if err != nil {
		_ = c.Delete(ctx, e)
		return err
	}

	if err = c.moveIfNeeded(ctx, e, actualURL); err != nil {
		return err
	}

	return nil
}

func (c *Cache) moveIfNeeded(ctx context.Context, e *cache.Entry, actualURL string) error {
	if e.Has() {
		return nil
	}

	if err := c.afs.Move(ctx, e.Meta.URL, actualURL); err != nil {
		return err
	}
	return nil
}

func (c *Cache) close(e *cache.Entry) error {
	return e.Close()
}

func (c *Cache) AddValues(ctx context.Context, e *cache.Entry, values []interface{}) error {
	if c.recorder != nil {
		c.recorder.AddValues(values)
	}

	err := c.addRow(ctx, e, values)
	if err != nil && e.WriteCloser != nil {
		_ = e.WriteCloser.Close()
	}

	return err
}

func (c *Cache) AssignRows(entry *cache.Entry, rows *sql.Rows) error {
	return entry.AssignRows(rows)
}

func (c *Cache) initializeCacheType(values []interface{}) {
	if c.typeHolder != nil {
		return
	}

	c.mux.Lock()
	c.typeHolder = &cache.ScanTypeHolder{}
	c.typeHolder.InitType(values)
	c.mux.Unlock()
}

func (c *Cache) writeMetaIfNeeded(ctx context.Context, e *cache.Entry) error {
	if e.RowAdded {
		return nil
	}

	err := c.writeMeta(ctx, e)
	if err != nil && e.WriteCloser != nil {
		return e.WriteCloser.Close()
	}

	e.RowAdded = true
	return nil
}

func (c *Cache) addRow(ctx context.Context, e *cache.Entry, values []interface{}) error {
	if err := c.writeMetaIfNeeded(ctx, e); err != nil {
		return err
	}

	marshal, err := json.Marshal(values)
	if err != nil {
		return err
	}

	return e.Write(marshal)
}
