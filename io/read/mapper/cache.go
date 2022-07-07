package mapper

import (
	"github.com/viant/sqlx/io"
	"hash/fnv"
	"reflect"
	"sync"
)

type (
	Cache struct {
		first  *Segment
		second *Segment
		mutex  sync.Mutex
	}

	Segment struct {
		index   map[uint64]int
		cache   []*Entry
		maxSize int
	}

	Entry struct {
		rawKey      string
		key         uint64
		matchesType bool
		wasCached   bool
		fields      []io.Field
	}
)

func (e *Entry) HasFields() bool {
	return len(e.fields) > 0
}

func (s *Segment) match(key uint64) (*Entry, bool) {
	index, ok := s.index[key]
	if !ok {
		return nil, false
	}

	return s.cache[index], true
}

func (s *Segment) add(entry *Entry) {
	s.index[entry.key] = len(s.cache)
	s.cache = append(s.cache, entry)
}

func (s *Segment) reset() {
	s.index = map[uint64]int{}
	s.cache = s.cache[:0]
}

func (s *Segment) delete(entry *Entry) {
	delete(s.index, entry.key)
}

func New(size int) *Cache {
	actualSize := size / 2
	return &Cache{
		first:  newSegment(actualSize),
		second: newSegment(actualSize),
	}
}

func (e *Entry) Fields() []io.Field {
	return e.fields
}

func (c *Cache) Get(structType reflect.Type, columns []io.Column, resolver io.Resolve) (*Entry, error) {
	signature, err := c.generateKey(structType, columns)
	if err != nil {
		return nil, err
	}

	hashed, err := c.hashKey(signature)
	if err != nil {
		return nil, err
	}

	cachedEntry, ok := c.match(hashed, signature)
	if !ok {
		entry := &Entry{
			rawKey: signature,
			key:    hashed,
		}

		return entry, nil
	}

	cachedEntry, err = c.updateUnresolvedFields(cachedEntry, resolver)
	if err != nil {
		return nil, err
	}

	return cachedEntry, nil
}

func (c *Cache) match(key uint64, signature string) (*Entry, bool) {
	c.mutex.Lock()
	entry, ok := c.matchKey(key)
	c.mutex.Unlock()

	if !ok || entry.rawKey != signature {
		return nil, false
	}

	return entry, true
}

func (c *Cache) matchKey(key uint64) (*Entry, bool) {
	fields, ok := c.first.match(key)
	if ok {
		return fields, true
	}

	fields, ok = c.second.match(key)
	if ok {
		return fields, true
	}
	return nil, false
}

func (c *Cache) updateUnresolvedFields(entry *Entry, resolver io.Resolve) (*Entry, error) {
	if entry.matchesType {
		return entry, nil
	}

	//if any field was resolved, we need to recreate fields that were resolved.
	fields := entry.fields
	newEntry := *entry
	newFields := make([]io.Field, len(fields))
	for i, field := range fields {
		if field.Field != nil {
			newFields[i] = fields[i]
			continue
		}

		fieldPtr, err := updateField(fields[i], resolver)
		if err != nil {
			return nil, err
		}

		newFields[i] = *fieldPtr
	}

	newEntry.fields = newFields
	return &newEntry, nil
}

func updateField(field io.Field, resolver io.Resolve) (*io.Field, error) {
	fieldPtr := &field
	if err := io.UpdateUnresolved(fieldPtr, resolver); err != nil {
		return nil, err
	}

	return fieldPtr, nil
}

func matchesType(fields []io.Field) bool {
	for _, field := range fields {
		if !field.MatchesType {
			return false
		}
	}

	return true
}

func newSegment(size int) *Segment {
	return &Segment{
		index:   map[uint64]int{},
		cache:   make([]*Entry, 0, size),
		maxSize: size,
	}
}

func (c *Cache) Put(entry *Entry, fields []io.Field) {
	if entry.rawKey == "" || entry.wasCached {
		return
	}

	entry.wasCached = true
	entry.matchesType = matchesType(fields)

	fieldsCopy := make([]io.Field, len(fields))
	for i, field := range fields {
		if field.MatchesType {
			fieldsCopy[i] = fields[i] //copying because Resolve is "stateful". It is required to override fields that will be resolved.
		} else {
			fieldsCopy[i] = io.Field{Column: field.Column}
		}
	}

	entry.fields = fieldsCopy
	c.mutex.Lock()
	if len(c.first.cache) < c.first.maxSize {
		c.first.add(entry)
	} else {
		c.second, c.first = c.first, c.second
		c.first.reset()
		c.first.add(entry)
	}
	c.mutex.Unlock()
}

func (c *Cache) generateKey(structType reflect.Type, columns []io.Column) (string, error) {
	dataType := structType.String()
	size := len(dataType) + len(columns)
	for _, column := range columns {
		size += len(column.Name())
	}

	keyBytes := make([]byte, size)
	offset := copy(keyBytes, dataType)
	for _, column := range columns {
		keyBytes[offset] = '/'
		offset++
		offset += copy(keyBytes[offset:], column.Name())
	}

	return string(keyBytes), nil
}

func (c *Cache) Delete(entry *Entry) error {
	c.mutex.Lock()
	c.first.delete(entry)
	c.second.delete(entry)
	c.mutex.Unlock()

	return nil
}

func (c *Cache) hashKey(key string) (uint64, error) {
	h := fnv.New64a()
	if _, err := h.Write([]byte(key)); err != nil {
		return 0, err
	}

	return h.Sum64(), nil
}
