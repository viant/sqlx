package option

import (
	"strings"

	"github.com/viant/sqlx"
)

// Criteria combines native predicate options with ordered bound arguments.
// It does not evaluate predicates or decide whether an input was provided.
func (o Options) Criteria() *sqlx.Criteria {
	var expressions []string
	var values []any
	for _, candidate := range o {
		var criteria *sqlx.Criteria
		switch value := candidate.(type) {
		case *sqlx.Criteria:
			criteria = value
		case sqlx.Criteria:
			criteria = &value
		}
		if criteria == nil {
			continue
		}
		if strings.TrimSpace(criteria.Expression) == "" && len(criteria.Placeholders) == 0 {
			continue
		}
		expressions = append(expressions, "("+criteria.Expression+")")
		values = append(values, criteria.Placeholders...)
	}
	if len(expressions) == 0 {
		return nil
	}
	return &sqlx.Criteria{Expression: strings.Join(expressions, " AND "), Placeholders: values}
}
