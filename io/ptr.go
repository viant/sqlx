package io

import (
	"fmt"
	"github.com/viant/xunsafe"
)

// Int64Ptr returns pointer to index-th element of slice as pointer to int84
func Int64Ptr(values []interface{}, index int) (*int64, error) {
	return Int64ValuePtr(values[index])
}

func Int64ValuePtr(value interface{}) (*int64, error) {
	switch actual := value.(type) {
	case *int, *uint, uint64:
		ptr := xunsafe.AsPointer(value)
		return (*int64)(ptr), nil
	case **int64:
		if *actual == nil {
			i := int64(0)
			*actual = &i
		}
		return *actual, nil
	case **int, **uint, **uint64:
		ptr := (**int64)(xunsafe.AsPointer(value))
		if *ptr == nil {
			i := int64(0)
			*ptr = &i
		}
		return *ptr, nil
	case *int64:
		return actual, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", actual)
	}
}
