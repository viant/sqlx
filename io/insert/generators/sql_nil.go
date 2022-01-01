package generators

import (
	"database/sql"
)

func sqlNil(value interface{}) interface{} {
	switch actual := value.(type) {
	case *int:
		if actual == nil || *actual == 0 {
			return sql.NullInt64{}
		}
	case *float64:
		if actual == nil || *actual == 0 {
			return sql.NullFloat64{}
		}
	case *string:
		if actual == nil || *actual == "" {
			return sql.NullString{}
		}
	case *bool:
		if actual == nil || *actual == false {
			return sql.NullBool{}
		}
	case *byte:
		if actual == nil || *actual == 0 {
			return sql.NullByte{}
		}
	case *int64:
		if actual == nil || *actual == 0 {
			return sql.NullInt64{}
		}
	}
	return value
}
