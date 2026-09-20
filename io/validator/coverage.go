package validator

import "github.com/viant/xunsafe"

func (o *Options) deferredAt(index int, field string) bool {
	if !o.candidatePoliciesSet {
		return false
	}
	deferred := o.CandidatePolicies[index].DeferredFields
	return deferred != nil && deferred(field)
}

func (o *Options) referenceSatisfiedAt(index int, reference Reference) bool {
	if o.candidatePoliciesSet {
		for _, receipt := range o.CandidatePolicies[index].SatisfiedReferences {
			if receipt == reference {
				return true
			}
		}
	}
	return false
}

func (o *Options) includes(record interface{}, field string) bool {
	return o.includesAt(0, record, field)
}

func (o *Options) includesAt(index int, record interface{}, field string) bool {
	if o.candidatePoliciesSet {
		filter := o.CandidatePolicies[index].FieldFilter
		if filter == nil {
			return true
		}
		return filter(field)
	}
	if o.fieldFilter != nil {
		return o.fieldFilter(field)
	}
	if o.SetMarker != nil {
		return o.SetMarker.Marker.IsFieldSet(xunsafe.AsPointer(record), field)
	}
	return true
}
