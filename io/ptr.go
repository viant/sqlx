package io

import (
	"fmt"
	"github.com/viant/xunsafe"
)

// Int64Ptr returns pointer to index-th element of slice as pointer to int84
func Int64Ptr(values []interface{}, index int) (*int64, error) {
	switch actual := values[index].(type) {
	case *int, *uint:
		ptr := xunsafe.AsPointer(values[index])
		return (*int64)(ptr), nil
	case *int64:
		return actual, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", actual)
	}
}
