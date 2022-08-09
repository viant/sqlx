package aerospike

import (
	"fmt"
	"github.com/viant/sqlx/io/read/cache"
	"strings"
)

type (
	IndexSource interface {
		Close() error
		Index(value interface{}) *cache.Indexed
		ColumnIndex() int
	}

	UnorderedSource struct {
		index       map[interface{}]int
		buffer      []*cache.Indexed
		dest        chan *cache.Indexed
		columnIndex int
	}

	OrderedSource struct {
		last        *cache.Indexed
		current     *cache.Indexed
		dest        chan *cache.Indexed
		columnIndex int
	}

	SingleSource struct {
		indexed *cache.Indexed
		dest    chan *cache.Indexed
	}
)

func (u *UnorderedSource) ColumnIndex() int {
	return u.columnIndex
}

func (o *OrderedSource) ColumnIndex() int {
	return o.columnIndex
}

func (s *SingleSource) ColumnIndex() int {
	return -1
}

func (s *SingleSource) Index(value interface{}) *cache.Indexed {
	return s.indexed
}

func (o *OrderedSource) Index(value interface{}) *cache.Indexed {
	//TODO implement me
	panic("implement me")
}

func NewIndexSource(column string, ordered bool, fields []*cache.Field, dest chan *cache.Indexed) (IndexSource, error) {
	if column == "" {
		return NewSingleSource(dest), nil
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
		return NewOrderedSource(dest, columnIndex), nil
	} else {
		return NewUnorderedSource(dest, columnIndex), nil
	}
}

func NewSingleSource(dest chan *cache.Indexed) *SingleSource {
	return &SingleSource{
		indexed: cache.NewIndexed(nil),
		dest:    dest,
	}
}

func (s *SingleSource) Close() error {
	s.dest <- s.indexed
	return nil
}

func NewUnorderedSource(dest chan *cache.Indexed, index int) *UnorderedSource {
	return &UnorderedSource{
		index:       map[interface{}]int{},
		dest:        dest,
		columnIndex: index,
	}
}

func (u *UnorderedSource) Close() error {
	for i := range u.buffer {
		u.dest <- u.buffer[i]
	}

	return nil
}

func (u *UnorderedSource) Index(columnValue interface{}) *cache.Indexed {
	argIndex, ok := u.index[columnValue]
	if !ok {
		argIndex = len(u.buffer)
		u.index[columnValue] = argIndex
		u.buffer = append(u.buffer, cache.NewIndexed(columnValue))
	}

	return u.buffer[argIndex]
}

func NewOrderedSource(dest chan *cache.Indexed, index int) *OrderedSource {
	return &OrderedSource{
		dest:        dest,
		columnIndex: index,
	}
}

func (o *OrderedSource) Close() error {
	//TODO implement me
	panic("implement me")
}
