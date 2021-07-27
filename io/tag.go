package io

import "strings"

//Tag represent field tag
type Tag struct {
	Column        string
	Autoincrement bool
	PrimaryKey    bool
	Sequence      string
	FieldIndex    int
	Transient bool
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
			case "sequence":
				tag.Sequence = strings.TrimSpace(nv[1])
			case "autoincrement":
				if strings.TrimSpace(nv[1]) == "true" {
					tag.Autoincrement = true
					tag.PrimaryKey = true
				}
			case "primaryKey":
				if strings.TrimSpace(nv[1]) == "true" {
					tag.PrimaryKey = true
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
