package cache

import (
	"context"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/source"
	"github.com/viant/xunsafe"
)

type Source struct {
	entry     *Entry
	cache     *Service
	ioColumns []io.Column
	xTypes    []*xunsafe.Type
	scanner   ScannerFn
}

func (s *Source) ConvertColumns() ([]io.Column, error) {
	if s.ioColumns != nil {
		return s.ioColumns, nil
	}

	s.ioColumns = make([]io.Column, len(s.entry.Meta.Fields))
	for i := range s.entry.Meta.Fields {
		s.ioColumns[i] = s.entry.Meta.Fields[i]
	}

	return s.ioColumns, nil
}

func (s *Source) Scanner(context.Context) func(args ...interface{}) error {
	if s.scanner != nil {
		return s.scanner
	}

	scanner := s.cache.scanner(s.entry)
	s.scanner = scanner

	return scanner
}

func (s *Source) XTypes() []*xunsafe.Type {
	if s.xTypes != nil {
		return s.xTypes
	}

	s.xTypes = make([]*xunsafe.Type, len(s.entry.Meta.Fields))
	for i, field := range s.entry.Meta.Fields {
		s.xTypes[i] = xunsafe.NewType(field.ScanType())
	}

	return nil
}

func (s *Source) CheckType(ctx context.Context, values []interface{}) (bool, error) {
	return s.cache.UpdateType(ctx, s.entry, values)
}

func (s *Source) Close(ctx context.Context) error {
	return s.cache.Close(ctx, s.entry)
}

func (s *Source) Next() bool {
	return s.entry.Next()
}

func (c *Service) AsSource(ctx context.Context, entry *Entry) (source.Source, error) {
	return &Source{
		entry: entry,
		cache: c,
	}, nil
}
