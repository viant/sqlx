package aerospike

import (
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/viant/sqlx/io/read/cache"
	"strings"
)

type (
	IndexSource interface {
		Close() error
		Index(value interface{}) (*indexedValue, error)
		ColumnIndex() int
		Count() int
	}

	UnorderedSource struct {
		index       map[interface{}]int
		indexed     []*indexedValue
		columnIndex int
		cache       *Cache
		column      string
		url         string
		metaBin     as.BinMap
	}

	OrderedSource struct {
		currentValue interface{}
		indexed      *indexedValue
		columnIndex  int
		count        int
		cache        *Cache
		column       string
		url          string
		metaBin      as.BinMap
	}

	SingleSource struct {
		indexed *indexedValue
		cache   *Cache
		column  string
		url     string
		metaBin as.BinMap
	}
)

func (u *UnorderedSource) ColumnIndex() int { return u.columnIndex }
func (o *OrderedSource) ColumnIndex() int   { return o.columnIndex }
func (s *SingleSource) ColumnIndex() int    { return -1 }

func (u *UnorderedSource) Count() int { return len(u.indexed) }
func (o *OrderedSource) Count() int   { return o.count }
func (s *SingleSource) Count() int {
	if s.indexed == nil {
		return 0
	}
	return 1
}

func NewIndexSource(aCache *Cache, column string, ordered bool, fields []*cache.Field, url string, metaBin as.BinMap) (IndexSource, error) {
	if column == "" {
		return NewSingleSource(aCache, column, url, metaBin), nil
	}

	columnLower := strings.ToLower(column)
	columnIndex := -1
	for i, field := range fields {
		if strings.ToLower(field.Name()) == columnLower {
			columnIndex = i
			break
		}
	}
	if columnIndex == -1 {
		return nil, fmt.Errorf("not found column %v in the database response", column)
	}
	if ordered {
		return NewOrderedSource(aCache, column, url, metaBin, columnIndex), nil
	}
	return NewUnorderedSource(aCache, column, url, metaBin, columnIndex), nil
}

func NewSingleSource(aCache *Cache, column string, url string, metaBin as.BinMap) *SingleSource {
	return &SingleSource{cache: aCache, column: column, url: url, metaBin: cloneBinMap(metaBin)}
}

func (s *SingleSource) Close() error {
	if s.indexed == nil {
		indexed, err := newIndexedValue(s.cache, nil, s.column, s.url, s.metaBin, true)
		if err != nil {
			return err
		}
		s.indexed = indexed
	}
	return s.indexed.close()
}

func NewUnorderedSource(aCache *Cache, column string, url string, metaBin as.BinMap, index int) *UnorderedSource {
	return &UnorderedSource{
		index:       map[interface{}]int{},
		columnIndex: index,
		cache:       aCache,
		column:      column,
		url:         url,
		metaBin:     cloneBinMap(metaBin),
	}
}

func (u *UnorderedSource) Close() error {
	for i := range u.indexed {
		if err := u.indexed[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func (u *UnorderedSource) Index(columnValue interface{}) (*indexedValue, error) {
	argIndex, ok := u.index[columnValue]
	if !ok {
		indexed, err := newIndexedValue(u.cache, columnValue, u.column, u.url, u.metaBin, false)
		if err != nil {
			return nil, err
		}
		argIndex = len(u.indexed)
		u.index[columnValue] = argIndex
		u.indexed = append(u.indexed, indexed)
	}
	return u.indexed[argIndex], nil
}

func NewOrderedSource(aCache *Cache, column string, url string, metaBin as.BinMap, index int) *OrderedSource {
	return &OrderedSource{
		columnIndex: index,
		cache:       aCache,
		column:      column,
		url:         url,
		metaBin:     cloneBinMap(metaBin),
	}
}

func (o *OrderedSource) Close() error {
	if o.indexed == nil {
		return nil
	}
	err := o.indexed.close()
	o.indexed = nil
	return err
}

func (o *OrderedSource) Index(value interface{}) (*indexedValue, error) {
	if o.currentValue == nil {
		indexed, err := newIndexedValue(o.cache, value, o.column, o.url, o.metaBin, false)
		if err != nil {
			return nil, err
		}
		o.currentValue = value
		o.indexed = indexed
		o.count++
		return o.indexed, nil
	}

	if o.currentValue != value {
		if err := o.indexed.close(); err != nil {
			return nil, err
		}
		indexed, err := newIndexedValue(o.cache, value, o.column, o.url, o.metaBin, false)
		if err != nil {
			return nil, err
		}
		o.currentValue = value
		o.indexed = indexed
		o.count++
	}
	return o.indexed, nil
}

func (s *SingleSource) Index(value interface{}) (*indexedValue, error) {
	if s.indexed != nil {
		return s.indexed, nil
	}
	indexed, err := newIndexedValue(s.cache, nil, s.column, s.url, s.metaBin, true)
	if err != nil {
		return nil, err
	}
	s.indexed = indexed
	return s.indexed, nil
}
