package cache

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ScalarIndexMatcher describes native scalar group retrieval without changing
// the caller's complete tuple predicate or its per-tuple row window.
func (m *ParmetrizedQuery) ScalarIndexMatcher() (*ParmetrizedQuery, bool, error) {
	if m == nil || m.By == "" || len(m.ByColumns) == 0 {
		return m, false, nil
	}
	index := -1
	for i, column := range m.ByColumns {
		if strings.EqualFold(column, m.By) {
			if index != -1 {
				return nil, false, fmt.Errorf("ambiguous scalar matcher column %q", m.By)
			}
			index = i
		}
	}
	if index == -1 {
		return nil, false, fmt.Errorf("scalar matcher column %q is not in composite columns", m.By)
	}
	result := *m
	result.ByColumns, result.InTuples, result.In = nil, nil, nil
	result.Offset, result.Limit = 0, 0
	seen := map[string]bool{}
	for _, tuple := range m.InTuples {
		if len(tuple) != len(m.ByColumns) {
			return nil, false, fmt.Errorf("invalid matcher tuple width")
		}
		encoded, err := json.Marshal(tuple[index])
		if err != nil {
			return nil, false, err
		}
		if !seen[string(encoded)] {
			result.In = append(result.In, tuple[index])
			seen[string(encoded)] = true
		}
	}
	return &result, true, nil
}

// MatchColumnsAvailable guards tuple filtering after native projection.
func (m *ParmetrizedQuery) MatchColumnsAvailable(meta *Meta) bool {
	for _, name := range m.ByColumns {
		count := 0
		for _, field := range meta.EffectiveFields() {
			if field != nil && strings.EqualFold(field.Name(), name) {
				count++
			}
		}
		if count != 1 {
			return false
		}
	}
	return true
}
