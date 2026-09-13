package insert

import (
	"github.com/viant/xunsafe"
	"reflect"
)

// explicitIdentity is an opt-in assignment signal, independent of zero value.
// A nil identity pointer still needs database allocation, even if its field was
// present. Marker-free records retain their existing allocation behavior.
func (n *numericSequencer) explicitIdentity(record, value interface{}) bool {
	if n.session == nil || n.session.setMarker == nil || n.session.setMarker.Marker == nil {
		return false
	}
	if !n.session.setMarker.IsSet(xunsafe.AsPointer(record), n.position) {
		return false
	}
	actual := reflect.ValueOf(value)
	for actual.IsValid() && (actual.Kind() == reflect.Pointer || actual.Kind() == reflect.Interface) {
		if actual.IsNil() {
			return false
		}
		actual = actual.Elem()
	}
	return actual.IsValid()
}
