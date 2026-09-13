package read

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache"
)

// windowSource applies the same per-key/per-tuple selection to database rows
// and native lazy-cache replay. Native indexed sources already apply windows.
type windowSource struct {
	cache.Source
	matcher     *cache.ParmetrizedQuery
	indexes     []int
	counts      map[string]int
	keys        map[string]bool
	initialized bool
	disabled    bool
}

func withWindow(source cache.Source, matcher *cache.ParmetrizedQuery, entry *cache.Entry) cache.Source {
	if matcher == nil || entry != nil && entry.Windowed || matcher.By == "" && len(matcher.ByColumns) == 0 {
		return source
	}
	// Original scalar reads with zero/one key are already constrained by SQL;
	// their projection need not contain the matcher key. Composite matching is
	// explicit and remains independently windowed.
	if len(matcher.ByColumns) == 0 && len(matcher.In) <= 1 {
		return source
	}
	return &windowSource{Source: source, matcher: matcher, counts: map[string]int{}, keys: map[string]bool{}}
}

func (s *windowSource) ConvertColumns() ([]io.Column, error) {
	columns, err := s.Source.ConvertColumns()
	if err != nil || s.initialized {
		return columns, err
	}
	names := s.matcher.ByColumns
	if len(names) == 0 {
		names = []string{s.matcher.By}
	}
	for _, name := range names {
		index := -1
		for i, column := range columns {
			if strings.EqualFold(column.Name(), name) {
				if index != -1 {
					return nil, fmt.Errorf("ambiguous matcher column %q", name)
				}
				index = i
			}
		}
		if index == -1 {
			if len(s.matcher.ByColumns) == 0 {
				s.disabled = true
				s.initialized = true
				return columns, nil
			}
			return nil, fmt.Errorf("matcher column %q is not in the result", name)
		}
		s.indexes = append(s.indexes, index)
	}
	if len(s.matcher.ByColumns) > 0 {
		for _, tuple := range s.matcher.InTuples {
			if len(tuple) != len(s.indexes) {
				return nil, fmt.Errorf("matcher tuple has %d values for %d columns", len(tuple), len(s.indexes))
			}
			key, err := json.Marshal(tuple)
			if err != nil {
				return nil, err
			}
			s.keys[string(key)] = true
		}
	} else {
		for _, value := range s.matcher.In {
			key, err := json.Marshal([]any{value})
			if err != nil {
				return nil, err
			}
			s.keys[string(key)] = true
		}
	}
	if s.matcher.Offset < 0 || s.matcher.Limit < 0 {
		return nil, fmt.Errorf("matcher window must be non-negative")
	}
	s.initialized = true
	return columns, nil
}

func (s *windowSource) Scanner(ctx context.Context) cache.ScannerFn {
	scan := s.Source.Scanner(ctx)
	if s.disabled {
		return scan
	}
	return func(values ...interface{}) error {
		if err := scan(values...); err != nil {
			return err
		}
		tuple := make([]any, len(s.indexes))
		for i, index := range s.indexes {
			tuple[i] = values[index]
		}
		encoded, err := json.Marshal(tuple)
		if err != nil {
			return err
		}
		key := string(encoded)
		if len(s.keys) > 0 && !s.keys[key] {
			return SkipError("matcher key excluded")
		}
		s.counts[key]++
		count := s.counts[key]
		if count <= s.matcher.Offset || s.matcher.Limit > 0 && count-s.matcher.Offset > s.matcher.Limit {
			return SkipError("matcher window excluded")
		}
		return nil
	}
}
