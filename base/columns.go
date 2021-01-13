package base

import (
	"database/sql"
	"github.com/viant/sqlx"
)

//TypesToColumns converts []*sql.ColumnType type to []sqlx.Column
func TypesToColumns(columns []*sql.ColumnType) []sqlx.Column {
	var result = make([]sqlx.Column, len(columns))
	for i := range columns {
		result[i] = columns[i]
	}
	return result
}

//NamesToColumns converts []string to []sqlx.Column
func NamesToColumns(columns []string) []sqlx.Column {
	var result = make([]sqlx.Column, len(columns))
	for i := range columns {
		result[i] = &Column{name: columns[i]}
	}
	return result
}
