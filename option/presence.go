package option

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type PresenceProvider struct {
	Holder        *xunsafe.Field
	Fields        []*xunsafe.Field
	IdentityIndex int
}

//IsFieldSet returns true if field has been set
func (p *PresenceProvider) IsFieldSet(ptr unsafe.Pointer, index int) bool {
	if p == nil || p.Holder == nil {
		return true //we do not have field presence provider so we assume all fields are set
	}
	return p.Has(ptr, index)
}

//Has returns true if value on holder field with index has been set
func (p *PresenceProvider) Has(ptr unsafe.Pointer, index int) bool {
	hasPtr := p.Holder.ValuePointer(ptr)
	return p.Fields[index].Bool(hasPtr)
}

func (p *PresenceProvider) Init(filedPos, transientPos map[string]int) error {
	if p.Holder == nil || len(filedPos) == 0 {
		return nil
	}

	if holder := p.Holder; holder != nil {
		p.Fields = make([]*xunsafe.Field, len(filedPos))
		holderType := holder.Type
		if holderType.Kind() == reflect.Ptr {
			holderType = holderType.Elem()
		}
		for i := 0; i < holderType.NumField(); i++ {
			presentField := holderType.Field(i)
			pos, ok := filedPos[presentField.Name]
			if !ok {
				if _, ok := transientPos[presentField.Name]; ok {
					continue
				}
				return fmt.Errorf("failed to match presence field %v %v", presentField.Name, filedPos)
			}

			p.Fields[pos] = xunsafe.NewField(presentField)
		}
	}
	return nil
}

func (p *PresenceProvider) Placeholders(record interface{}, placeholders []interface{}) []interface{} {
	if p.Holder == nil {
		return placeholders
	}
	var result = make([]interface{}, 0, len(placeholders))
	ptr := xunsafe.AsPointer(record)
	holderPtr := p.Holder.ValuePointer(ptr)
	for i := 0; i < p.IdentityIndex; i++ {
		field := p.Fields[i]
		if field == nil {
			continue
		}
		if field.Bool(holderPtr) {
			result = append(result, placeholders[i])
		}
	}
	for i := p.IdentityIndex; i < len(placeholders); i++ {
		result = append(result, placeholders[i])
	}
	return result
}
