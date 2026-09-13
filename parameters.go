package sqlx

import (
	"strings"

	"github.com/viant/sqlparser/source"
)

type parameter struct {
	start, end int
	name       string
}

// Parameters preserves SQL text and the positions of executable ? and :name
// placeholders. SQLparser owns quoted strings, comments and identifier regions.
type Parameters struct {
	SQL   string
	items []parameter
}

func ParseParameters(SQL string) *Parameters {
	result := &Parameters{SQL: SQL}
	scanner := source.NewCodeScanner(SQL, 0)
	end := 0
	for i, ok := scanner.Next(); ok; i, ok = scanner.Next() {
		if i < end {
			continue
		}
		if SQL[i] == '?' {
			if result.questionOperator(i) {
				continue
			}
			result.items = append(result.items, parameter{start: i, end: i + 1})
			continue
		}
		if SQL[i] != ':' || i+1 >= len(SQL) || i > 0 && SQL[i-1] == ':' || !parameterNameStart(SQL[i+1]) {
			continue
		}
		end = i + 2
		for end < len(SQL) {
			if parameterNameStart(SQL[end]) || SQL[end] >= '0' && SQL[end] <= '9' {
				end++
				continue
			}
			if SQL[end] == '.' && end+1 < len(SQL) && parameterNameStart(SQL[end+1]) {
				end += 2
				continue
			}
			break
		}
		result.items = append(result.items, parameter{start: i, end: end, name: SQL[i+1 : end]})
	}
	return result
}

func (p *Parameters) questionOperator(position int) bool {
	SQL := p.SQL
	if position > 0 && SQL[position-1] == '@' {
		return true
	}
	if position+1 < len(SQL) && (SQL[position+1] == '|' || SQL[position+1] == '&') {
		return true
	}
	previous := position - 1
	for previous >= 0 {
		if source.IsWhitespace(SQL[previous]) {
			previous--
			continue
		}
		start, _, kind := source.ProtectedRangeAt(SQL, previous)
		if strings.Contains(kind, "comment") {
			previous = start - 1
			continue
		}
		if kind != "" {
			return true
		}
		break
	}
	if previous < 0 {
		return false
	}
	if len(p.items) > 0 && p.items[len(p.items)-1].end == previous+1 {
		return true
	}
	if SQL[previous] == '?' {
		return false
	}
	if SQL[previous] == ')' || SQL[previous] == ']' {
		return true
	}
	if !parameterNameStart(SQL[previous]) && !(SQL[previous] >= '0' && SQL[previous] <= '9') {
		return false
	}
	start := previous
	for start > 0 && (parameterNameStart(SQL[start-1]) || SQL[start-1] >= '0' && SQL[start-1] <= '9') {
		start--
	}
	switch strings.ToLower(SQL[start : previous+1]) {
	case "select", "where", "and", "or", "not", "like", "ilike", "in", "between", "when", "then", "else", "on", "having", "as", "set", "values", "returning", "is", "by", "offset", "limit", "case", "from", "join", "update", "insert", "into", "delete", "distinct", "all", "exists", "union", "except", "intersect":
		return false
	}
	return true
}

func parameterNameStart(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= 128
}
func (p *Parameters) Count() int {
	if p == nil {
		return 0
	}
	return len(p.items)
}
func (p *Parameters) NamedCount() int {
	count := 0
	if p != nil {
		for _, item := range p.items {
			if item.name != "" {
				count++
			}
		}
	}
	return count
}
func (p *Parameters) PositionalCount() int { return p.Count() - p.NamedCount() }
func (p *Parameters) HasPositional() bool  { return p.PositionalCount() != 0 }

// ExpandSinglePositional expands the sole executable positional placeholder.
// Existing named placeholders, literal text and comments remain byte-for-byte.
func (p *Parameters) ExpandSinglePositional(count int) string {
	if p == nil {
		return ""
	}
	if count <= 1 || p.PositionalCount() != 1 {
		return p.SQL
	}
	for _, item := range p.items {
		if item.name == "" {
			return p.SQL[:item.start] + strings.TrimSuffix(strings.Repeat("?,", count), ",") + p.SQL[item.end:]
		}
	}
	return p.SQL
}
