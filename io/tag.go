package io

import (
	"fmt"
	"reflect"
	"strings"
)

//Tag represent field tag
type Tag struct {
	Column        string
	Autoincrement bool
	PrimaryKey    bool
	Sequence      string
	FieldIndex    int
	Transient     bool
	Ns            string
	Generator     string
	NullifyEmpty  bool
}

//CanExpand return true if field can expend fied struct fields
func (f *Field) CanExpand() bool {
	if f.Tag.Ns != "" {
		return true
	}

	if !f.Anonymous {
		return false
	}

	candidateType := f.Type
	if candidateType.Kind() == reflect.Ptr {
		candidateType = candidateType.Elem()
	}
	return candidateType.Kind() == reflect.Struct
}

//ParseTag parses tag
func ParseTag(tagString string) *Tag {
	tag := &Tag{}
	if tagString == "-" {
		tag.Transient = true
		return tag
	}
	elements := strings.Split(tagString, ",")
	if len(elements) == 0 {
		return tag
	}
	for i, element := range elements {
		nv := strings.Split(element, "=")
		switch len(nv) {
		case 2:
			switch strings.ToLower(strings.TrimSpace(nv[0])) {
			case "name":
				tag.Column = strings.TrimSpace(nv[1])
			case "ns":
				tag.Ns = strings.TrimSpace(nv[1])
			case "sequence":
				tag.Sequence = strings.TrimSpace(nv[1])
			case "primarykey":
				if strings.TrimSpace(nv[1]) == "true" {
					tag.PrimaryKey = true
				}
			case "autoincrement":
				tag.Autoincrement = true
			case "generator":
				generatorStrat := strings.TrimSpace(nv[1])
				tag.Generator = generatorStrat
				if generatorStrat == "autoincrement" {
					tag.Autoincrement = true
					tag.Generator = ""
				}
			case "nullifyempty":
				nullifyEmpty := strings.TrimSpace(nv[1])
				tag.NullifyEmpty = nullifyEmpty == "true" || nullifyEmpty == ""
			}
			continue
		case 1:
			if i == 0 {
				tag.Column = strings.TrimSpace(element)
				continue
			}
			switch strings.ToLower(element) {
			case "autoincrement":
				tag.PrimaryKey = true
			case "primarykey":
				tag.PrimaryKey = true
			case "nullifyempty":
				tag.NullifyEmpty = true
			}
		}

	}
	tag.PrimaryKey = tag.PrimaryKey || tag.Autoincrement
	return tag
}

func (t *Tag) getColumnName(field reflect.StructField) string {
	columnName := field.Name
	if names := t.Column; names != "" {
		columns := strings.Split(names, "|")
		columnName = columns[0]
	}
	return columnName
}

func (t *Tag) isIdentity(name string) bool {
	return t.Autoincrement || t.PrimaryKey || strings.ToLower(t.Column) == "id" || strings.ToLower(name) == "id"
}

func (t *Tag) validateWithField(field reflect.StructField) error {
	if t.Sequence != "" {
		columnName := t.getColumnName(field)
		return t.validate(columnName)
	}
	return nil
}

func (t *Tag) validate(name string) error {
	if t.Sequence != "" && !t.isIdentity(name) {
		return fmt.Errorf("invalid tag combination: a sequence cannot be used for a non-identity field (column, sequence) = (%s, %s)", t.Column, t.Sequence)
	}
	return nil
}
