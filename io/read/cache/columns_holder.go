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

	fields := s.entry.Meta.EffectiveFields()
	s.ioColumns = make([]io.Column, len(fields))
	for i := range fields {
		s.ioColumns[i] = fields[i]
	}

	return s.ioColumns, nil
}
