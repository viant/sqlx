package sink

import (
	"strings"
	"unicode"
)

//Column represents column metadata
type Column struct {
	Catalog         string  `sqlx:"TABLE_CATALOG"`
	Schema          string  `sqlx:"TABLE_SCHEMA"`
	Table           string  `sqlx:"TABLE_NAME"`
	Name            string  `sqlx:"COLUMN_NAME"`
	Position        int     `sqlx:"ORDINAL_POSITION"`
	Comments        string  `sqlx:"COLUMN_COMMENT"`
	Type            string  `sqlx:"DATA_TYPE"`
	Length          *int64  `sqlx:"CHARACTER_MAXIMUM_LENGTH"`
	Precision       *int64  `sqlx:"NUMERIC_PRECISION"`
	Scale           *int64  `sqlx:"NUMERIC_SCALE"`
	Nullable        string  `sqlx:"IS_NULLABLE"`
	Default         *string `sqlx:"COLUMN_DEFAULT"`
	Key             string  `sqlx:"COLUMN_KEY"`
	Descending      string  `sqlx:"DESCENDING"`
	Index           string  `sqlx:"INDEX_NAME"`
	IndexPosition   int     `sqlx:"INDEX_POSITION"`
	Collation       *string `sqlx:"COLLATION"`
	IsAutoincrement *bool   `sqlx:"IS_AUTOINCREMENT"`
}

func (c *Column) IsNullable() bool {
	if c.Nullable == "" {
		return false
	}
	switch unicode.ToLower(rune(c.Nullable[0])) {
	case rune('y'), rune('t'), rune('1'):
		return true
	}
	return false
}

func (c *Column) IsUnique() bool {
	if c.Key == "" {
		return false
	}
	switch strings.ToLower(c.Key) {
	case "uni": //unique key
		return true
	}
	return true
}
