package validator

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
	"reflect"
)

const (
	CheckKidUnique  = CheckKid("unique")
	CheckKidRefKey  = CheckKid("refKey")
	CheckKidNotNull = CheckKid("notnull")
)

type (
	CheckKid string

	Check struct {
		SQL        string
		Field      *xunsafe.Field
		CheckType  reflect.Type
		CheckField *xunsafe.Field
	}

	Validation struct {
		Type   reflect.Type
		Unique []*Check
		RefKey []*Check
		NoNull []*Check
	}
)

func NewValidation(p reflect.Type) (*Validation, error) {
	var result = &Validation{Type: p}
	columns, err := io.StructColumns(p, option.TagSqlx)
	if err != nil {
		return nil, err
	}
	sType := p
	if sType.Kind() == reflect.Ptr {
		sType = sType.Elem()
	}
	for _, column := range columns {
		tag := column.Tag()
		if tag == nil {
			continue
		}
		xField := xunsafe.NewField(sType.Field(tag.FieldIndex))
		if tag.NotNull {
			result.NoNull = append(result.NoNull, &Check{
				Field: xField,
			})
		}

		if tag.IsUnique && tag.Table != "" {
			checkType := reflect.StructOf([]reflect.StructField{{Name: xField.Name, Type: xField.Type, Tag: `sqlx:"Val"`}})
			checkField := xunsafe.NewField(checkType.Field(0))
			result.Unique = append(result.Unique, &Check{
				SQL:        "SELECT " + column.Name() + " AS Val FROM " + schema(tag.Db) + tag.Table + " WHERE " + column.Name(),
				CheckType:  checkType,
				CheckField: checkField,
				Field:      xField,
			})
			continue
		}

		if tag.RefColumn != "" && tag.RefTable != "" {
			checkType := reflect.StructOf([]reflect.StructField{{Name: xField.Name, Type: xField.Type, Tag: `sqlx:"Val"`}})
			checkField := xunsafe.NewField(checkType.Field(0))
			result.RefKey = append(result.RefKey, &Check{
				SQL:        "SELECT " + tag.RefColumn + " AS Val FROM " + schema(tag.RefDb) + tag.RefTable + " WHERE " + tag.RefColumn,
				CheckType:  checkType,
				CheckField: checkField,
				Field:      xField,
			})
		}
	}
	return result, nil
}

func schema(db string) string {
	if db == "" {
		return db
	}
	return "." + db
}
