package io

import (
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
		if len(nv) == 2 {
			switch strings.ToLower(strings.TrimSpace(nv[0])) {
			case "name":
				tag.Column = strings.TrimSpace(nv[1])
			case "ns":
				tag.Ns = strings.TrimSpace(nv[1])
			case "sequence":
				tag.Sequence = strings.TrimSpace(nv[1])
			case "primaryKey":
				if strings.TrimSpace(nv[1]) == "true" {
					tag.PrimaryKey = true
				}
			case "generator":
				generatorStrat := strings.TrimSpace(nv[1])
				tag.Generator = generatorStrat
				if generatorStrat == "autoincrement" {
					tag.Autoincrement = true
					tag.PrimaryKey = true
					tag.Generator = ""
				}
			}
			continue
		}
		if i == 0 {
			tag.Column = strings.TrimSpace(element)
		}

	}
	return tag
}
