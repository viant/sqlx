package validator

import "strings"

type (
	queryContext struct {
		SQL          string
		placeholders []string
		values       []interface{}
		index        map[interface{}]*queryValue
	}
	queryValue struct {
		value interface{}
		field string
		path  *Path
	}
)

func (p *queryContext) Append(value interface{}, field string, path *Path) {
	if len(p.index) == 0 {
		p.index = map[interface{}]*queryValue{}
	}
	p.placeholders = append(p.placeholders, "?")
	p.values = append(p.values, value)
	p.index[mapKey(value)] = &queryValue{
		value: value,
		field: field,
		path:  path,
	}
}

func (p *queryContext) Query() string {
	return p.SQL + " IN (" + strings.Join(p.placeholders, ",") + ")"
}

func newQueryContext(SQL string) *queryContext {
	return &queryContext{index: map[interface{}]*queryValue{}, SQL: SQL}
}
