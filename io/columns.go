package io

import (
	"database/sql"
)

//TypesToColumns converts []*sql.ColumnType type to []sqlx.column
func TypesToColumns(columns []*sql.ColumnType) []Column {
	var result = make([]Column, len(columns))
	for i := range columns {
		result[i] = columns[i]
	}
	return result
}

//NamesToColumns converts []string to []sqlx.column
func NamesToColumns(columns []string) []Column {
	var result = make([]Column, len(columns))
	for i := range columns {
		result[i] = &column{name: columns[i]}
	}
	return result
}
