package io

import (
	"database/sql"
)



//Columns represents columns
type Columns []Column

//Autoincrement returns position of autoincrement column or -1
func (c Columns) Autoincrement() int {
	if len(c) == 0 {
		return -1
	}
	for i, item := range c {
		if tag := item.Tag();tag != nil && tag.Autoincrement {
			return i
		}
	}
	return -1
}


func (c Columns) Names() []string {
	var result = make([]string, len(c))
	for i, item := range c {
		result[i] = item.Name()
	}
	return result
}


//TypesToColumns converts []*sql.ColumnType type to []sqlx.column
func TypesToColumns(columns []*sql.ColumnType) []Column {
	var result = make([]Column, len(columns))
	for i := range columns {
		result[i] = &columnType{columns[i]}
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
