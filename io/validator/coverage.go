package validator

import "github.com/viant/xunsafe"

func (o *Options) includes(record interface{}, field string) bool {
	if o.fieldFilter != nil {
		return o.fieldFilter(field)
	}
	if o.SetMarker != nil {
		return o.SetMarker.Marker.IsFieldSet(xunsafe.AsPointer(record), field)
	}
	return true
}
