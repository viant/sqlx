package validator

import (
	"strconv"
	"strings"
)

type Path struct {
	Elements []string
	IsSlice  bool
}

func (p *Path) AppendField(field string) *Path {
	result := &Path{
		Elements: append(p.Elements, field),
	}
	return result
}

func (p *Path) AppendIndex(index int) *Path {
	if !p.IsSlice {
		return p
	}
	result := &Path{
		Elements: append(p.Elements, "["+strconv.Itoa(index)+"]"),
	}
	return result
}

func (p *Path) String() string {
	return strings.Join(p.Elements, ".")
}
