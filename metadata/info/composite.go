package info

import "strings"

// CompositeIn renders a dialect-aware composite IN predicate using generic '?'
// placeholders. Placeholder numbering, when needed, is applied later by EnsurePlaceholders.
func (d *Dialect) CompositeIn(columns []string, rowCount int) string {
	if len(columns) == 0 || rowCount <= 0 {
		return "1 = 0"
	}

	if len(columns) == 1 {
		return scalarIn(columns[0], rowCount)
	}

	if d != nil && d.CompositeInRenderer != nil {
		return d.CompositeInRenderer(columns, rowCount)
	}
	return defaultCompositeIn(columns, rowCount)
}

func scalarIn(column string, rowCount int) string {
	if strings.TrimSpace(column) == "" || rowCount <= 0 {
		return "1 = 0"
	}
	builder := &strings.Builder{}
	builder.WriteString(column)
	builder.WriteString(" IN (")
	for i := 0; i < rowCount; i++ {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteByte('?')
	}
	builder.WriteByte(')')
	return builder.String()
}

func defaultCompositeIn(columns []string, rowCount int) string {
	builder := &strings.Builder{}
	builder.WriteByte('(')
	builder.WriteString(strings.Join(columns, ", "))
	builder.WriteString(") IN (")
	for row := 0; row < rowCount; row++ {
		if row > 0 {
			builder.WriteString(", ")
		}
		builder.WriteByte('(')
		for col := range columns {
			if col > 0 {
				builder.WriteString(", ")
			}
			builder.WriteByte('?')
		}
		builder.WriteByte(')')
	}
	builder.WriteByte(')')
	return builder.String()
}
