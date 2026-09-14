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

func (c *Cache) Rollback(ctx context.Context, entry *cache.Entry) error {
	if entry != nil && entry.ReadOnly {
		return entry.Close()
	}
	if entry == nil {
		return nil
	}
	if !entry.Has() {
		defer c.unmark(strings.ReplaceAll(entry.Meta.URL, ".json"+entry.Id, ".json"))
	}
	_ = entry.Close()
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

func (c *Cache) Get(ctx context.Context, SQL string, args []interface{}, options ...interface{}) (result *cache.Entry, readErr error) {
	var stats *cache.Stats
	for _, option := range options {
		if value, ok := option.(*cache.Stats); ok && value != nil {
			stats = value
			*stats = cache.Stats{}
		}
	}
	defer func() { c.observeEntry(stats, result, readErr) }()
	var refresh bool
	var readOnly bool
	for _, option := range options {
		if only, ok := option.(lookupOnly); ok {
			readOnly = bool(only)
		}
		if requested, ok := option.(cache.Refresh); ok {
			refresh = bool(requested)
		}
	}

	if refresh {
		for _, option := range options {
			if matcher, ok := option.(*cache.ParmetrizedQuery); ok && matcher != nil {
				if err := c.refreshWarmup(ctx, matcher, stats); err != nil {
					return nil, err
				}
			}
		}
	}

	for _, option := range options {
		if refresh {
			break
		}
		if matcher, ok := option.(*cache.ParmetrizedQuery); ok && matcher != nil && matcher.IdentitySQL != "" && matcher.By == "" && len(matcher.ByColumns) == 0 {
			entry, err := c.queryEntry(ctx, matcher)
			if err != nil || entry != nil {
				if stats != nil && entry != nil && entry.Has() {
					stats.Type = cache.TypeReadMulti
					stats.FoundWarmup = true
					stats.WarmupKey = entry.Meta.URL
				}
				return entry, err
			}
		}
		if matcher, ok := option.(*cache.ParmetrizedQuery); ok && matcher != nil && (matcher.By != "" && len(matcher.In) > 0 || len(matcher.ByColumns) > 0 && len(matcher.InTuples) > 0) {
			entry, err := c.indexedEntry(ctx, matcher)
			if err != nil || entry != nil {
				if stats != nil && entry != nil && entry.Has() {
					stats.Type = cache.TypeReadMulti
					stats.FoundWarmup = true
					stats.WarmupKey = entry.Meta.URL
					stats.MarkerKey = entry.Meta.URL
				}
				return entry, err
			}
		}
	}
	URL, err := hash.GenerateURL(SQL, c.storage, c.extension, args)
	if err != nil {
		return nil, err
	}
	if stats != nil {
		stats.Key = URL
	}
	// Published entries are immutable; cache readers must not compete for the
	// exclusive lease used while creating a missing entry.
	if !refresh {
		if entry, err := c.cachedEntry(ctx, SQL, args, URL); entry != nil || err != nil {
			return entry, err
		}
	}

	if readOnly {
		return nil, nil
	}
	if c.mark(URL) {
		if refresh {
			return nil, fmt.Errorf("cache refresh conflicts with an active query writer")
		}
		return nil, nil
	}
	if refresh {
		exists, err := c.afs.Exists(ctx, URL)
		if err == nil && exists {
			err = c.afs.Delete(ctx, URL)
		}
		if err != nil {
			c.unmark(URL)
			return nil, err
		}
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
	entry, err := c.newEntry(SQL, args, URL)
	if err != nil {
		return nil, err
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

func (c *Cache) newEntry(SQL string, args []interface{}, URL string) (*cache.Entry, error) {
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

	return entry, nil
}

func (c *Cache) cachedEntry(ctx context.Context, SQL string, args []interface{}, URL string) (*cache.Entry, error) {
	entry, err := c.newEntry(SQL, args, URL)
	if err != nil {
		return nil, err
	}
	status, err := c.readData(ctx, entry)
	if err != nil || status != ExistsStatus {
		return nil, err
	}
	valid, err := c.checkMeta(entry.ReadCloser, &entry.Meta)
	if err != nil || !valid {
		_ = entry.Close()
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
			c.mux.Lock()
			c.canWrite[URL] = false
			c.mux.Unlock()
		}

		return status, err
	}

	metaCorrect, err := c.checkMeta(entry.ReadCloser, &entry.Meta)
	if !metaCorrect || err != nil {
		_ = entry.ReadCloser.Close()
		entry.ReadCloser = nil
		if err != nil {
			return ErrorStatus, err
		}
		if err := c.afs.Delete(ctx, URL); err != nil {
			return ErrorStatus, err
		}
		entry.Id = strings.ReplaceAll(uuid.New().String(), "-", "")
		entry.Meta.URL += entry.Id
		return NotExistStatus, nil
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
	entryMeta.StoredFields = meta.StoredFields
	entryMeta.Partial = meta.Partial
	entryMeta.Generation = meta.Generation

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
	return int(cache.Now().UnixMilli()) > meta.ExpiryTimeMs
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

	m.Meta.ExpiryTimeMs = int(cache.Now().Add(c.ttl).UnixMilli())
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
	if entry.ScanTypes == nil {
		entry.ScanTypes = &cache.ScanTypeHolder{}
		entry.ScanTypes.InitType(values)
	}

	if !entry.ScanTypes.Match(entry) {
		return false, c.Delete(ctx, entry)
	}

	return true, nil
}

func (c *Cache) Delete(ctx context.Context, entry *cache.Entry) error {
	if entry.ReadOnly {
		return entry.Close()
	}
	return c.afs.Delete(ctx, entry.Meta.URL)
}

func (c *Cache) mark(URL string) bool {
	c.mux.Lock()
	_, isInMap := c.canWrite[URL]
	c.canWrite[URL] = false
	c.mux.Unlock()
	return isInMap
}

func (c *Cache) unmark(url string) {
	c.mux.Lock()
	delete(c.canWrite, url)
	c.mux.Unlock()
}

func (c *Cache) scanner(e *cache.Entry) cache.ScannerFn {
	if e.Meta.Projected() {
		return cache.NewProjectedScanner(e, e.Meta.ProjectedIndexes, e.ScanTypes, c.recorder)
	}
	return cache.NewScanner(e.ScanTypes, c.recorder).New(e)
}

func (c *Cache) Close(ctx context.Context, e *cache.Entry) error {
	if e.ReadOnly {
		return e.Close()
	}
	actualURL := strings.ReplaceAll(e.Meta.URL, ".json"+e.Id, ".json")
	if !e.Has() {
		defer c.unmark(actualURL)
	}
	if !e.Has() && !e.RowAdded && len(e.Meta.Fields) > 0 {
		if err := c.writeMetaIfNeeded(ctx, e); err != nil {
			return err
		}
	}
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
