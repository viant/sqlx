package io

import (
	"fmt"
	"github.com/viant/structology/format/text"
	"reflect"
	"strings"
)

const (
	EncodingJSON = "JSON"
)

// Tag represent field tag
type Tag struct {
	Column           string
	Autoincrement    bool
	PrimaryKey       bool
	Sequence         string
	Transient        bool
	Ns               string
	Generator        string
	IsUnique         bool
	Db               string
	Table            string
	RefDb            string
	RefTable         string
	RefColumn        string
	Required         bool
	NullifyEmpty     bool
	ErrorMgs         string
	PresenceProvider bool
	Bit              bool
	Encoding         string
	CaseFormat       text.CaseFormat
}

// CanExpand return true if field can expend fied struct fields
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

// ParseTag parses tag
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
			case "presence":
				tag.PresenceProvider = true
				tag.Transient = true
			case "primarykey":
				tag.PrimaryKey = strings.TrimSpace(nv[1]) == "true"
			case "autoincrement":
				tag.Autoincrement = true
			case "unique":
				tag.IsUnique = strings.TrimSpace(nv[1]) == "true"
			case "db":
				tag.Db = nv[1]
			case "caseformat":
				tag.CaseFormat = text.NewCaseFormat(nv[1])
			case "table":
				tag.Table = nv[1]
			case "refdb":
				tag.RefDb = nv[1]
			case "reftable":
				tag.RefTable = nv[1]
			case "refcolumn":
				tag.RefColumn = nv[1]
			case "transient":
				tag.Transient = strings.TrimSpace(nv[1]) == "true"
			case "bit":
				tag.Bit = strings.TrimSpace(nv[1]) == "true"
			case "required":
				tag.Required = strings.TrimSpace(nv[1]) == "true"
			case "errormsg":
				tag.ErrorMgs = strings.ReplaceAll(nv[1], "$coma", ",")
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
			case "enc":
				tag.Encoding = nv[1]
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
			case "bit":
				tag.Bit = true
			case "primarykey":
				tag.PrimaryKey = true
			case "unique":
				tag.IsUnique = true
			case "nullifyempty":
				tag.NullifyEmpty = true
			case "required":
				tag.Required = true
			case "-":
				tag.Transient = true
			case "presence":
				tag.PresenceProvider = true
				tag.Transient = true
			}
		}

	}
	tag.PrimaryKey = tag.PrimaryKey || tag.Autoincrement
	return tag
}

func (t *Tag) getColumnName(field reflect.StructField) string {
	columnName := ""
	if name := t.Name(); name != "" {
		columnName = name
	}
	if columnName == "" {
		if strings.ToUpper(columnName[0:]) == columnName[0:] {
			columnName = text.CaseFormatUpperCamel.Format(field.Name, t.CaseFormat)
		} else {
			columnName = text.CaseFormatLowerCamel.Format(field.Name, t.CaseFormat)
		}
	}
	return columnName
}

func (t *Tag) Name() string {
	column := t.Column
	if names := t.Column; names != "" {
		columns := strings.Split(names, "|")
		column = columns[0]
	}
	return column
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
