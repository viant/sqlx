package validator

import (
	"github.com/viant/sqlx/io"
	"strings"
	"unsafe"
)

type (
	queryContext struct {
		SQL             string
		placeholders    []string
		values          []interface{}
		index           map[interface{}]*queryValue
		queryExclusions []*additionalCriteria
		queryInclusion  []*additionalCriteria
	}
	queryValue struct {
		value interface{}
		field string
		path  *Path
	}
	additionalCriteria struct {
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

func (p *queryContext) AddInclusion(columns []*io.Column, recUPtr unsafe.Pointer, itemPath *Path) {
	criteria, done := p.addCriteria(columns, recUPtr, itemPath)
	if done {
		return
	}
	p.queryInclusion = append(p.queryInclusion, criteria)
}

func (p *queryContext) AddExclusion(columns []*io.Column, recUPtr unsafe.Pointer, itemPath *Path) {
	queryExclusion, done := p.addCriteria(columns, recUPtr, itemPath)
	if done {
		return
	}
	p.queryExclusions = append(p.queryExclusions, queryExclusion)
}

func (p *queryContext) addCriteria(columns []*io.Column, recUPtr unsafe.Pointer, itemPath *Path) (*additionalCriteria, bool) {
	if len(columns) == 0 {
		return nil, true
	}

	if len(p.queryExclusions) == 0 {
		p.queryExclusions = []*additionalCriteria{}
	}

	queryExclusion := &additionalCriteria{
		columnNames:  make([]string, len(columns)),
		placeholders: make([]string, len(columns)),
	}

	for i, column := range columns {
		columnFielder, ok := (*column).(io.ColumnWithFields)
		if !ok {
			return nil, true
		}

		fields := columnFielder.Fields()
		field := fields[len(fields)-1]

		fieldPath := itemPath.AppendField(field.Name)
		fieldValue := field.Value(recUPtr)

		p.values = append(p.values, fieldValue)
		p.index[mapKey(fieldValue)] = &queryValue{
			value: fieldValue,
			field: field.Name,
			path:  fieldPath,
		}

		queryExclusion.placeholders[i] = "?"
		queryExclusion.columnNames[i] = columnFielder.Name()
	}
	return queryExclusion, false
}

/*
id, dep, unk
1, 2, 3
->
*/
func (p *queryContext) Query() string {
	return p.SQL + " IN (" + strings.Join(p.placeholders, ",") + ")"
}

func (p *queryContext) QueryWithCriteria() string {

	var sb strings.Builder
	sb.WriteString(p.Query())

	for _, criteria := range p.queryExclusions {
		sb.WriteString(" AND ")
		if len(criteria.columnNames) > 1 {
			sb.WriteString("(")
		}
		sb.WriteString(strings.Join(criteria.columnNames, ","))
		if len(criteria.columnNames) > 1 {
			sb.WriteString(")")
		}
		sb.WriteString(" NOT IN (")
		sb.WriteString(strings.Join(criteria.placeholders, ","))
		sb.WriteString(")")
	}

	for _, criteria := range p.queryInclusion {
		sb.WriteString(" AND ")
		if len(criteria.columnNames) > 1 {
			sb.WriteString("(")
		}
		sb.WriteString(strings.Join(criteria.columnNames, ","))
		if len(criteria.columnNames) > 1 {
			sb.WriteString(")")
		}
		sb.WriteString(" IN (")
		sb.WriteString(strings.Join(criteria.placeholders, ","))
		sb.WriteString(")")
	}
	return sb.String()
}

func newQueryContext(SQL string) *queryContext {
	return &queryContext{index: map[interface{}]*queryValue{}, SQL: SQL}
}
