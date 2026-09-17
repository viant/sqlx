package validator

import (
	"fmt"
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

	// Reference identifies an exact compiled reference constraint. Schema is the
	// declared RefDb, including an empty value; Field is the canonical Go name.
	Reference struct {
		Field  string
		Schema string
		Table  string
		Column string
	}

	Check struct {
		SQL            string
		Reference      Reference
		Field          *xunsafe.Field
		ErrorMsg       string
		CheckType      reflect.Type
		CheckField     *xunsafe.Field
		UniqueDep      *io.Column
		IdentityColumn *io.Column
		columnIndex    int
		columns        []io.Column
		bind           io.PlaceholderBinder
	}

	Checks struct {
		Type     reflect.Type
		Unique   []*Check
		RefKey   []*Check
		NoNull   []*Check
		presence *option.SetMarker
		bind     io.PlaceholderBinder
	}
)

func NewChecks(p reflect.Type, presence *option.SetMarker) (*Checks, error) {
	var result = &Checks{Type: p}
	sType := p
	if sType.Kind() == reflect.Ptr {
		sType = sType.Elem()
	}
	// Compile presence independently of the first invocation's validation mode.
	// A cached full-record check must remain usable by a later sparse check.
	compiledPresence := &option.SetMarker{}
	columns, bind, err := io.StructColumnMapper(p, compiledPresence)
	if err != nil {
		return nil, err
	}
	result.presence = compiledPresence
	result.bind = bind
	if presence != nil {
		*presence = *compiledPresence
	}

	identityColPos := io.Columns(columns).IdentityColumnPos()
	var identityColumn io.Column
	if identityColPos > -1 {
		identityColumn = columns[identityColPos]
	}

	columnByName := make(map[string]io.Column)
	for _, column := range columns {
		columnByName[column.Name()] = column
	}

	for columnIndex, column := range columns {
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
				Field:       xField,
				ErrorMsg:    tag.ErrorMgs,
				columnIndex: columnIndex,
			})
		}

		var uniqueDep *io.Column

		if tag.IsUnique && tag.Table != "" {
			checkType := reflect.StructOf([]reflect.StructField{{Name: xField.Name, Type: xField.Type, Tag: `sqlx:"Val"`}})
			checkField := xunsafe.NewField(checkType.Field(0))
			if tag.UniqueDep != "" {
				setColumn, ok := columnByName[tag.UniqueDep]
				if !ok {
					return nil, fmt.Errorf("column %s form unique set not preset in type: %s", tag.UniqueDep, p.String())
				}
				uniqueDep = &setColumn
			}

			result.Unique = append(result.Unique, &Check{
				columnIndex:    columnIndex,
				columns:        columns,
				bind:           bind,
				SQL:            "SELECT " + column.Name() + " AS Val FROM " + schema(tag.Db) + tag.Table + " WHERE " + column.Name(),
				CheckType:      checkType,
				CheckField:     checkField,
				Field:          xField,
				ErrorMsg:       tag.ErrorMgs,
				IdentityColumn: &identityColumn,
				UniqueDep:      uniqueDep,
			})
		}

		if tag.RefColumn != "" && tag.RefTable != "" {
			checkType := reflect.StructOf([]reflect.StructField{{Name: xField.Name, Type: xField.Type, Tag: `sqlx:"Val"`}})
			checkField := xunsafe.NewField(checkType.Field(0))
			result.RefKey = append(result.RefKey, &Check{
				Reference:  Reference{Field: xField.Name, Schema: tag.RefDb, Table: tag.RefTable, Column: tag.RefColumn},
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

// ValidateReferences rejects unknown, wrong-target and duplicate receipts using
// compiled metadata. It performs no validation callbacks or database queries.
func (c *Checks) ValidateReferences(references []Reference) error {
	seen := make(map[Reference]bool, len(references))
	for _, reference := range references {
		if seen[reference] {
			return fmt.Errorf("duplicate reference receipt: %+v", reference)
		}
		seen[reference] = true
		found := false
		for _, check := range c.RefKey {
			if check.Reference == reference {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unknown reference receipt: %+v", reference)
		}
	}
	return nil
}

func schema(db string) string {
	if db == "" {
		return db
	}
	return db + "."
}
