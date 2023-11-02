package io

import (
	"fmt"
	"github.com/viant/structology/format/text"
	"github.com/viant/structology/tags"
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
	if tagString == "-" {
		tag.Transient = true
	}
	values := tags.Values(tagString)
	name, values := values.Name()
	tag.Column = name
	_ = values.MatchPairs(tag.updateTagKey)
	tag.PrimaryKey = tag.PrimaryKey || tag.Autoincrement
	return tag
}
func (t *Tag) updateTagKey(key string, value string) error {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "name", "column":
		t.Column = strings.TrimSpace(value)
	case "ns":
		t.Ns = strings.TrimSpace(value)
	case "sequence":
		t.Sequence = strings.TrimSpace(value)
	case "presence":
		t.PresenceProvider = true
		t.Transient = true
	case "primarykey":
		t.PrimaryKey = strings.TrimSpace(value) == "true"
	case "autoincrement":
		t.Autoincrement = true
	case "unique":
		t.IsUnique = strings.TrimSpace(value) == "true"
	case "db":
		t.Db = value
	case "caseformat":
		t.CaseFormat = text.NewCaseFormat(value)
	case "table":
		t.Table = value
	case "refdb":
		t.RefDb = value
	case "reftable":
		t.RefTable = value
	case "refcolumn":
		t.RefColumn = value
	case "transient":
		t.Transient = strings.TrimSpace(value) == "true"
	case "bit":
		t.Bit = strings.TrimSpace(value) == "true"
	case "required":
		t.Required = strings.TrimSpace(value) == "true"
	case "errormsg":
		t.ErrorMgs = strings.ReplaceAll(value, "$coma", ",")
	case "generator":
		generatorStrat := strings.TrimSpace(value)
		t.Generator = generatorStrat
		if generatorStrat == "autoincrement" {
			t.Autoincrement = true
			t.Generator = ""
		}
	case "nullifyempty":
		nullifyEmpty := strings.TrimSpace(value)
		t.NullifyEmpty = nullifyEmpty == "true" || nullifyEmpty == ""
	case "enc":
		t.Encoding = value
	case "-":
		t.Transient = true
	}
	return nil
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
	if t.Ns != "" {
		return t.Ns + columnName
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
