package validator

import "strings"

type (
	queryContext struct {
		SQL             string
		placeholders    []string
		values          []interface{}
		index           map[interface{}]*queryValue
		queryExclusions []*queryExclusion
	}
	queryValue struct {
		value interface{}
		field string
		path  *Path
	}
	queryExclusion struct {
		columnNames  []string
		placeholders []string
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

func (p *queryContext) AddExclusion(values []interface{}, fields []string, paths []*Path) {
	if len(values) != len(fields) || len(values) != len(paths) {
		return
	}

	if len(p.queryExclusions) == 0 {
		p.queryExclusions = []*queryExclusion{}
	}

	queryExclusion := &queryExclusion{
		columnNames:  fields,
		placeholders: make([]string, len(fields)),
	}

	for i := 0; i < len(queryExclusion.placeholders); i++ {
		queryExclusion.placeholders[i] = "?"
	}

	p.queryExclusions = append(p.queryExclusions, queryExclusion)

	for i := 0; i < len(fields); i++ {
		p.values = append(p.values, values[i])
		p.index[mapKey(values[i])] = &queryValue{
			value: values[i],
			field: fields[i],
			path:  paths[i],
		}
	}
}

func (p *queryContext) Query() string {
	return p.SQL + " IN (" + strings.Join(p.placeholders, ",") + ")"
}

func (p *queryContext) QueryWithExclusions() string {

	var sb strings.Builder
	sb.WriteString(p.Query())

	for _, exclusion := range p.queryExclusions {
		sb.WriteString(" AND ")
		if len(exclusion.columnNames) > 1 {
			sb.WriteString("(")
		}
		sb.WriteString(strings.Join(exclusion.columnNames, ","))
		if len(exclusion.columnNames) > 1 {
			sb.WriteString(")")
		}
		sb.WriteString(" NOT IN (")
		sb.WriteString(strings.Join(exclusion.placeholders, ","))
		sb.WriteString(")")
	}

	return sb.String()
}

func newQueryContext(SQL string) *queryContext {
	return &queryContext{index: map[interface{}]*queryValue{}, SQL: SQL}
}
