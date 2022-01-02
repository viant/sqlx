package option

import (
	"reflect"
)

//Assign assigns supplied option, if returns un assign options and true if assign at least one
func Assign(options []Option, supplied ...interface{}) bool {
	return assign(options, supplied)
}

//Assign assign supplied option
func assign(options []Option, supported []interface{}) bool {
	if len(options) == 0 {
		return false
	}
	if len(supported) == 0 {
		return false
	}
	var index = make(map[reflect.Type]interface{})
	for i := range supported {
		index[reflect.TypeOf(supported[i]).Elem()] = supported[i]
	}
	assigned := false
	for i := range options {
		option := options[i]
		if option == nil {
			continue
		}
		optionValue := reflect.ValueOf(option)
		target, ok := index[optionValue.Type()]
		if !ok {
			for k, v := range index {
				if optionValue.Type().AssignableTo(k) {
					target = v
					ok = true
					break
				}
			}
		}
		if !ok {
			continue
		}
		assigned = true
		reflect.ValueOf(target).Elem().Set(optionValue)
	}
	return assigned
}
