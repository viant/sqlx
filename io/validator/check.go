package validator

import (
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

const (
	CheckKidUnique  = CheckKid("unique")
	CheckKidRefKey  = CheckKid("refKey")
	CheckKidNotNull = CheckKid("notnull")
)

type (
	CheckKid string

	Check struct {
		SQL              string
		Field            *xunsafe.Field
		ErrorMsg         string
		CheckType        reflect.Type
		CheckField       *xunsafe.Field
		UniqueSetColumns []*io.Column
		IdentityColumn   *io.Column
	}

	Checks struct {
		Type     reflect.Type
		Unique   []*Check
		RefKey   []*Check
		NoNull   []*Check
		presence *option.SetMarker
	}
)

func NewChecks(p reflect.Type, presence *option.SetMarker) (*Checks, error) {
	var result = &Checks{Type: p}
	sType := p
	if sType.Kind() == reflect.Ptr {
		sType = sType.Elem()
	}
	var opts []option.Option
	if presence != nil {
		opts = append(opts, presence)
	}
	columns, err := io.StructColumns(p, opts...)
	if err != nil {
		return nil, err
	}
	result.presence = presence

	identityColPos := io.Columns(columns).IdentityColumnPos()
	var identityColumn io.Column
	if identityColPos > -1 {
		identityColumn = columns[identityColPos]
	}

	columnByName := make(map[string]io.Column)
	for _, column := range columns {
		columnByName[column.Name()] = column
	}

	for _, column := range columns {
		tag := column.Tag()
		if tag == nil {
			continue
		}

		fielder, ok := column.(io.Fielder)
		if !ok {
			continue
		}

		fields := fielder.Fields()

		xField := fields[len(fields)-1]

		if tag.Required {
			result.NoNull = append(result.NoNull, &Check{
				Field:    xField,
				ErrorMsg: tag.ErrorMgs,
			})
		}

		var setColumns []*io.Column

		if tag.IsUnique && tag.Table != "" {
			checkType := reflect.StructOf([]reflect.StructField{{Name: xField.Name, Type: xField.Type, Tag: `sqlx:"Val"`}})
			checkField := xunsafe.NewField(checkType.Field(0))
			if tag.UniqueSet != "" {
				for _, uniqueColumn := range strings.Split(tag.UniqueSet, ",") {
					uniqueSetColumnName := strings.TrimSpace(uniqueColumn)
					setColumn, ok := columnByName[uniqueSetColumnName]
					if !ok {
						return nil, fmt.Errorf("column %s form unique set not preset in type: %s", uniqueSetColumnName, p.String())
					}
					setColumns = append(setColumns, &setColumn)
				}
			}

			result.Unique = append(result.Unique, &Check{
				SQL:              "SELECT " + column.Name() + " AS Val FROM " + schema(tag.Db) + tag.Table + " WHERE " + column.Name(),
				CheckType:        checkType,
				CheckField:       checkField,
				Field:            xField,
				ErrorMsg:         tag.ErrorMgs,
				IdentityColumn:   &identityColumn,
				UniqueSetColumns: setColumns,
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
				ErrorMsg:   tag.ErrorMgs,
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
