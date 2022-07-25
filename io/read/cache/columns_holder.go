package cache

import "github.com/viant/sqlx/io"

type ColumnsHolder struct {
	ioColumns []io.Column
	entry     *Entry
}

func NewColumnsHolder(entry *Entry) *ColumnsHolder {
	return &ColumnsHolder{entry: entry}
}

func (s *ColumnsHolder) ConvertColumns() ([]io.Column, error) {
	if s.ioColumns != nil {
		return s.ioColumns, nil
	}

	s.ioColumns = make([]io.Column, len(s.entry.Meta.Fields))
	for i := range s.entry.Meta.Fields {
		s.ioColumns[i] = s.entry.Meta.Fields[i]
	}

	return s.ioColumns, nil
}
