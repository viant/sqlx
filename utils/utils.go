package utils

import "strings"

type Tag struct {
	FieldName     string
	Autoincrement bool
}

func ParseTag(tagString string) *Tag {
	elements := strings.Split(tagString, ",")
	if len(elements) == 0 {
		return nil
	}

	tag := &Tag{}
	for i, element := range elements {
		nv := strings.Split(element, "=")
		if len(nv) == 2 {
			switch strings.TrimSpace(nv[0]) {
			case "name":
				tag.FieldName = strings.TrimSpace(nv[1])
			case "autoincrement":
				if strings.TrimSpace(nv[1]) == "true" {
					tag.Autoincrement = true
				}
			}
			continue
		}
		if i == 0 {
			tag.FieldName = strings.TrimSpace(element)
		}

	}
	return tag
}
