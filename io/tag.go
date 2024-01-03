package io

import (
	"fmt"
	"github.com/viant/tagly/format/text"
	"github.com/viant/tagly/tags"
	"reflect"
	"strings"
)

const (
	EncodingJSON = "JSON"
	//TagSqlx defines sqlx annotation

	TagSqlx = "sqlx"
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
	DataType         string
	Raw              string
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
func ParseTag(structTag reflect.StructTag) *Tag {
	tagName := TagSqlx
	tagString := structTag.Get(tagName)
	tag := &Tag{Raw: string(structTag)}
	if tagString == "-" {
		tag.Transient = true
		return tag
	}

	if _, ok := structTag.Lookup("on"); ok {
		tag.Transient = true
	}
	if tagString == "-" {
		tag.Transient = true
	}

	if tag.Transient {
		return tag
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
		t.PrimaryKey = strings.TrimSpace(value) == "true" || strings.TrimSpace(value) == ""
	case "autoincrement":
		t.Autoincrement = true
	case "unique":
		t.IsUnique = strings.TrimSpace(value) == "true" || strings.TrimSpace(value) == ""
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
		t.Transient = strings.TrimSpace(value) == "true" || strings.TrimSpace(value) == ""
	case "bit":
		t.Bit = strings.TrimSpace(value) == "true" || strings.TrimSpace(value) == ""
	case "type":
		t.DataType = strings.TrimSpace(value)
	case "required":
		t.Required = strings.TrimSpace(value) == "true" || strings.TrimSpace(value) == ""
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
