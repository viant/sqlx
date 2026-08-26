package bigquery

import "strings"

func compositeIn(columns []string, rowCount int) string {
	builder := &strings.Builder{}
	builder.WriteByte('(')
	builder.WriteString(strings.Join(columns, ", "))
	builder.WriteString(") IN (")
	for row := 0; row < rowCount; row++ {
		if row > 0 {
			builder.WriteString(" UNION ALL ")
		}
		builder.WriteString("SELECT AS STRUCT ")
		for col := range columns {
			if col > 0 {
				builder.WriteString(", ")
			}
			builder.WriteByte('?')
		}
	}
	builder.WriteByte(')')
	return builder.String()
}
