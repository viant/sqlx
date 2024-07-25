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
	ret := strings.Builder{}
	for i, element := range p.Elements {
		if !strings.Contains(element, "[") && i > 0 {
			ret.WriteString(".")
		}
		ret.WriteString(element)
	}
	return ret.String()
}
