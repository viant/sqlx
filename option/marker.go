package option

import (
	"github.com/viant/structology"
	"github.com/viant/xunsafe"
)

type SetMarker struct {
	*structology.Marker
	IdentityIndex int
}

func (p *SetMarker) Placeholders(record interface{}, placeholders []interface{}) []interface{} {
	if p.Marker == nil {
		return placeholders
	}
	var result = make([]interface{}, 0, len(placeholders))
	ptr := xunsafe.AsPointer(record)
	for i := 0; i < p.IdentityIndex; i++ {
		if !p.Marker.IsSet(ptr, i) {
			continue
		}
		result = append(result, placeholders[i])
	}

	for i := p.IdentityIndex; i < len(placeholders); i++ {
		result = append(result, placeholders[i])
	}
	return result
}
